"""重试队列业务规则。

在原有重试条目清单之外，补齐四块可追溯数据：
- retry_attempt      重试尝试：每个失败URL在一个作业内的每次尝试与所用节点、token、结果
- retry_progress     重试进度数据集：按批次/版本管理，可封存、可合并、可校验
- retry_final_failure 最终失败清单：只在退避耗尽时产生，成功回写后移除
- retry_integrity    完整性台账：数据集快照校验和（见 services/integrity.py）

关键约束：
1. 同一作业内同一失败URL只允许一条未终结尝试；节点并发抢占只放一个槽位进入重试中。
2. 回写必须带尝试 token 与节点编号做身份校验；重复回执按幂等只更新/确认原尝试。
3. 退避未到点不允许派发；最终失败只能由退避耗尽的条目产生。
4. 作业停止后晚到重试只更新清单，绝不把作业状态带回执行中。
5. 查看者只读；节点执行员只能抢占/回写；作业负责人只能登记本作业失败；管理员全量。
"""
from __future__ import annotations

import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.integrity import integrity_service
from app.store import store

MODULE = "retryqueue"
ATTEMPT_TABLE = "retry_attempt"
PROGRESS_TABLE = "retry_progress"
FINAL_FAILURE_TABLE = "retry_final_failure"
JOB_TABLE = "crawljob"
WORKER_TABLE = "worker"

REQUIRED_FIELDS = ["条目编号", "失败URL", "失败原因码"]
STATUS_ORDER = ["草稿", "等待重试", "重试中", "重试成功", "最终失败"]
ACTION_RULES = {"加入队列": "等待重试", "发起重试": "重试中", "标记最终失败": "最终失败"}
NEGATIVE_ACTIONS = []

MAX_BACKOFF_LEVEL = 5
# 退避级别 -> 再次派发前需要等待的秒数；级别1在发现时即刻可派发。
BACKOFF_DELAY_SECONDS = {1: 0, 2: 5, 3: 15, 4: 30, 5: 60}

ROLE_ADMIN = "管理员"
ROLE_WRITER = "作业负责人"
ROLE_WORKER = "节点执行员"
ROLE_VIEWER = "查看者"
ROLES = [ROLE_ADMIN, ROLE_WRITER, ROLE_WORKER, ROLE_VIEWER]

# 角色 -> 可执行的受控动作范围。
ROLE_SCOPE: dict[str, set[str]] = {
    ROLE_ADMIN: {"discover", "claim", "writeback", "seal", "merge", "legacy_action"},
    ROLE_WRITER: {"discover", "legacy_action"},
    ROLE_WORKER: {"claim", "writeback"},
    ROLE_VIEWER: set(),
}

_open_attempt_states = ("等待派发", "重试中")
_terminal_attempt_states = ("重试成功", "重试失败", "退避耗尽")

_lock = threading.RLock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat(timespec="seconds")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class PermissionError(Exception):
    """受控范围校验失败，路由层转成 403。"""


class ConflictError(Exception):
    """并发抢占或状态冲突，路由层转成 409。"""


class RetryqueueService:
    # ---------------------------------------------------------------- 基础清单
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("条目编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str, *, operator: str | None = None, role: str | None = None) -> tuple[dict[str, Any] | None, str]:
        with _lock:
            self._ensure_scope(role, "legacy_action", operator)
            if action == "发起重试":
                return None, "发起重试必须经节点抢占接口生成尝试token与槽位记录，无身份的旧重试入口已停用"
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"重试条目 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于重试队列可执行范围"
            target = ACTION_RULES[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            if action == "标记最终失败" and not self._has_exhausted_attempt(str(entry.get("失败URL", ""))):
                return None, "最终失败只能由退避级别已耗尽的重试条目产生，请等待重试链路给出退避耗尽结论"
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            return entry, f"重试条目已{action}"

    # ---------------------------------------------------------------- 权限控制
    def _ensure_scope(self, role: str | None, action: str, operator: str | None) -> None:
        role = role or ROLE_VIEWER
        if role not in ROLES:
            raise PermissionError(f"角色「{role}」不在受控范围（{ '、'.join(ROLES) }）内")
        if action not in ROLE_SCOPE.get(role, set()):
            raise PermissionError(f"角色「{role}」对重试队列只有只读权限，不能执行{action}")
        if not str(operator or "").strip():
            raise PermissionError("缺少操作人身份，受控数据集不允许匿名改动")

    def _find_job(self, job_no: str) -> dict[str, Any] | None:
        return next((row for row in store.rows(JOB_TABLE) if row.get("作业编号") == job_no), None)

    def _find_worker(self, worker_no: str) -> dict[str, Any] | None:
        return next((row for row in store.rows(WORKER_TABLE) if row.get("节点编号") == worker_no), None)

    def _find_progress(self, job_no: str, url: str) -> dict[str, Any] | None:
        candidates = [
            row for row in store.rows(PROGRESS_TABLE)
            if row.get("作业编号") == job_no and row.get("失败URL") == url
        ]
        if not candidates:
            return None
        # 同一作业同一URL在封存后可能存在多个批次版本，可写操作始终落在开放版本上。
        editable = [row for row in candidates if row.get("批次状态") == "开放"]
        pool = editable or candidates
        return max(pool, key=lambda row: int(row.get("批次版本", 1)))

    def _fork_if_sealed(self, progress: dict[str, Any], stamp: str) -> dict[str, Any]:
        """已封存/已合并批次不可改：继续流转时复制出一个开放的新版本，保留原快照。"""
        if progress.get("批次状态") == "开放":
            return progress
        forked = dict(progress)
        forked["id"] = self._next_id(PROGRESS_TABLE)
        forked["批次号"] = self._next_batch_no()
        forked["批次版本"] = max(
            (int(row.get("批次版本", 1)) for row in store.rows(PROGRESS_TABLE)
             if row.get("作业编号") == progress.get("作业编号") and row.get("失败URL") == progress.get("失败URL")),
            default=1,
        ) + 1
        forked["批次状态"] = "开放"
        forked["合并状态"] = "未合并"
        forked["快照时间"] = None
        forked["更新时间"] = stamp
        store.rows(PROGRESS_TABLE).append(forked)
        return forked

    def _find_open_attempt(self, job_no: str, url: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.rows(ATTEMPT_TABLE)
             if row.get("作业编号") == job_no
             and row.get("失败URL") == url
             and row.get("尝试状态") in _open_attempt_states),
            None,
        )

    def _has_exhausted_attempt(self, url: str) -> bool:
        return any(
            row.get("失败URL") == url and row.get("尝试状态") == "退避耗尽"
            for row in store.rows(ATTEMPT_TABLE)
        )

    def _next_id(self, table: str) -> int:
        return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1

    def _open_batch_id(self) -> str:
        existing = [str(row.get("批次号", "")) for row in store.rows(PROGRESS_TABLE)]
        seq = 1
        while f"B-{seq:04d}" in existing:
            seq += 1
        return f"B-{seq:04d}"

    # ---------------------------------------------------------------- 失败URL登记（发现）
    def discover_failure(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """采集执行侧登记失败URL。重复发现同一URL不生成第二份尝试记录。"""
        with _lock:
            operator = str(values.get("operator") or "").strip()
            role = str(values.get("role") or ROLE_VIEWER).strip()
            self._ensure_scope(role, "discover", operator)
            job_no = str(values.get("作业编号") or "").strip()
            url = str(values.get("失败URL") or "").strip()
            reason = str(values.get("失败原因码") or "").strip()
            missing = [name for name, val in (("作业编号", job_no), ("失败URL", url), ("失败原因码", reason)) if not val]
            if missing:
                return None, f"缺少必填字段：{'、'.join(missing)}"
            job = self._find_job(job_no)
            if job is None:
                return None, f"作业 {job_no} 不存在，失败URL无法归属"
            if job.get("status") == "已停止":
                return None, f"作业 {job_no} 已停止，不再接收新的失败URL登记"
            existing = self._find_progress(job_no, url)
            if existing is not None:
                # 幂等：同一作业同一URL只绑定一次重试链路，重复发现直接回原记录。
                return existing, "该失败URL已存在重试记录，重复发现未生成第二份尝试"
            now = _now_iso()
            progress = {
                "id": self._next_id(PROGRESS_TABLE),
                "作业编号": job_no,
                "失败URL": url,
                "退避级别": 1,
                "已重试次数": 0,
                "下次重试时间": now,
                "尝试编号": None,
                "尝试token": None,
                "节点编号": None,
                "尝试状态": "等待派发",
                "恢复结果": None,
                "失败原因码": reason,
                "来源作业": job_no,
                "处理来源": "retryqueue",
                "批次号": self._open_batch_id(),
                "批次版本": 1,
                "批次状态": "开放",
                "合并状态": "未合并",
                "冲突标记": "无",
                "归属": job_no,
                "创建者": operator,
                "更新时间": now,
                "快照时间": None,
            }
            store.rows(PROGRESS_TABLE).append(progress)
            return progress, "失败URL已登记并进入等待重试，退避级别1可立即派发"

    # ---------------------------------------------------------------- 节点抢占槽位
    def claim_attempt(self, values: dict[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any] | None, str]:
        """节点抢占失败URL：校验节点可用性、退避到点、作业状态与单槽位约束。"""
        with _lock:
            operator = str(values.get("operator") or "").strip()
            role = str(values.get("role") or ROLE_VIEWER).strip()
            self._ensure_scope(role, "claim", operator)
            job_no = str(values.get("作业编号") or "").strip()
            url = str(values.get("失败URL") or "").strip()
            worker_no = str(values.get("节点编号") or "").strip()
            missing = [name for name, val in (("作业编号", job_no), ("失败URL", url), ("节点编号", worker_no)) if not val]
            if missing:
                return None, f"缺少必填字段：{'、'.join(missing)}"
            worker = self._find_worker(worker_no)
            if worker is None or worker.get("status") != "在线":
                return None, f"节点 {worker_no} 不在线或不存在，不能抢占重试槽位"
            progress = self._find_progress(job_no, url)
            if progress is None:
                return None, f"失败URL {url} 尚未在作业 {job_no} 下登记，无法发起重试"
            job = self._find_job(job_no)
            if job is not None and job.get("status") == "已停止":
                return None, f"作业 {job_no} 已停止，退避槽位不再派发"
            current = now or _now()
            due = _parse_iso(str(progress["下次重试时间"]))
            if current < due:
                wait = int((due - current).total_seconds()) + 1
                return None, f"退避时间未到点，还需等待约 {wait} 秒后才能派发"
            holder = self._find_open_attempt(job_no, url)
            if holder is not None:
                raise ConflictError(
                    f"该失败URL已有槽位处于{holder.get('尝试状态')}，"
                    f"被节点 {holder.get('节点编号')} 占用（尝试 {holder.get('尝试编号')}），并发抢占只允许一个槽位"
                )
            progress = self._fork_if_sealed(progress, current.isoformat(timespec="seconds"))
            attempt_no = f"ATT-{self._next_id(ATTEMPT_TABLE):04d}"
            token = uuid.uuid4().hex
            stamp = current.isoformat(timespec="seconds")
            attempt = {
                "id": self._next_id(ATTEMPT_TABLE),
                "尝试编号": attempt_no,
                "作业编号": job_no,
                "失败URL": url,
                "退避级别": int(progress["退避级别"]),
                "尝试token": token,
                "节点编号": worker_no,
                "尝试状态": "重试中",
                "失败原因码": progress.get("失败原因码"),
                "抢占时间": stamp,
                "完成时间": None,
                "恢复结果": None,
                "结果内容": None,
                "回执次数": 0,
                "处理来源": "retryqueue",
                "归属": job_no,
                "执行者": operator,
            }
            store.rows(ATTEMPT_TABLE).append(attempt)
            progress.update({
                "尝试编号": attempt_no,
                "尝试token": token,
                "节点编号": worker_no,
                "尝试状态": "重试中",
                "更新时间": stamp,
            })
            return attempt, f"槽位抢占成功，尝试 {attempt_no} 进入重试中，请凭尝试token回写结果"

    # ---------------------------------------------------------------- 结果回写（幂等）
    def writeback_result(self, values: dict[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any] | None, str]:
        """成功/失败回执：校验尝试身份，重复回执幂等；成功移除最终失败，失败按退避推进。"""
        with _lock:
            operator = str(values.get("operator") or "").strip()
            role = str(values.get("role") or ROLE_VIEWER).strip()
            self._ensure_scope(role, "writeback", operator)
            attempt_no = str(values.get("尝试编号") or "").strip()
            token = str(values.get("尝试token") or "").strip()
            worker_no = str(values.get("节点编号") or "").strip()
            outcome = str(values.get("结果") or "").strip()
            if not attempt_no or not token or not worker_no:
                return None, "缺少尝试身份字段（尝试编号、尝试token、节点编号），回执不予受理"
            attempt = next(
                (row for row in store.rows(ATTEMPT_TABLE) if row.get("尝试编号") == attempt_no),
                None,
            )
            if attempt is None:
                return None, f"尝试 {attempt_no} 不存在，回执无法匹配尝试身份"
            if attempt.get("尝试token") != token or attempt.get("节点编号") != worker_no:
                raise PermissionError("尝试身份校验失败：token或节点编号与抢占时不一致，禁止回写他人尝试")
            if outcome not in ("success", "failure"):
                return None, "结果字段只接受 success 或 failure"
            stamp = (now or _now()).isoformat(timespec="seconds")

            # 幂等：已终结尝试上的重复回执只确认原尝试，不产生第二份记录。
            if attempt.get("尝试状态") in _terminal_attempt_states:
                same = (outcome == "success" and attempt["尝试状态"] == "重试成功") or (
                    outcome == "failure" and attempt["尝试状态"] in ("重试失败", "退避耗尽")
                )
                if same:
                    attempt["回执次数"] = int(attempt.get("回执次数", 0)) + 1
                    return attempt, "回执重复到达，已按幂等处理，原尝试记录未被改写"
                raise ConflictError(f"尝试 {attempt_no} 已终结为{attempt['尝试状态']}，不能再写入相反结果")

            progress = self._find_progress(str(attempt["作业编号"]), str(attempt["失败URL"]))
            if progress is not None:
                progress = self._fork_if_sealed(progress, stamp)
            attempt["回执次数"] = int(attempt.get("回执次数", 0)) + 1
            attempt["完成时间"] = stamp
            attempt["结果内容"] = values.get("结果内容")
            attempt["失败原因码"] = values.get("失败原因码") or attempt.get("失败原因码")

            if outcome == "success":
                attempt["尝试状态"] = "重试成功"
                attempt["恢复结果"] = str(values.get("恢复结果") or "抓取成功并已回写")
                if progress is not None:
                    progress.update({
                        "尝试状态": "重试成功",
                        "恢复结果": attempt["恢复结果"],
                        "更新时间": stamp,
                    })
                    self._mark_cross_job_conflict(progress, stamp)
                removed = self._remove_from_final_failures(str(attempt["作业编号"]), str(attempt["失败URL"]), stamp)
                self.sync_job_failures(str(attempt["作业编号"]), now=now or _now())
                message = "成功回执已写回，重试记录标记为重试成功"
                if removed:
                    message += "，并已从最终失败清单移除"
                return attempt, message

            # 失败路径：退避级别推进；耗尽才进入最终失败清单。
            reason = str(values.get("失败原因码") or attempt.get("失败原因码") or "未知原因")
            next_level = int(attempt["退避级别"]) + 1
            if next_level > MAX_BACKOFF_LEVEL:
                attempt["尝试状态"] = "退避耗尽"
                attempt["恢复结果"] = f"退避级别{MAX_BACKOFF_LEVEL}仍失败：{reason}"
                if progress is not None:
                    progress.update({
                        "尝试状态": "最终失败",
                        "退避级别": MAX_BACKOFF_LEVEL,
                        "恢复结果": attempt["恢复结果"],
                        "失败原因码": reason,
                        "更新时间": stamp,
                    })
                self._append_final_failure(attempt, reason, stamp)
                self.sync_job_failures(str(attempt["作业编号"]), now=now or _now())
                return attempt, f"退避级别已耗尽（{MAX_BACKOFF_LEVEL}级），条目进入最终失败清单"

            delay = BACKOFF_DELAY_SECONDS.get(next_level, 60)
            next_due = (now or _now()) + timedelta(seconds=delay)
            attempt["尝试状态"] = "重试失败"
            attempt["恢复结果"] = f"第{attempt['退避级别']}级退避失败：{reason}"
            if progress is not None:
                progress.update({
                    "尝试状态": "等待派发",
                    "退避级别": next_level,
                    "已重试次数": int(progress.get("已重试次数", 0)) + 1,
                    "下次重试时间": next_due.isoformat(timespec="seconds"),
                    "恢复结果": attempt["恢复结果"],
                    "失败原因码": reason,
                    "尝试编号": None,
                    "尝试token": None,
                    "节点编号": None,
                    "更新时间": stamp,
                })
            self.sync_job_failures(str(attempt["作业编号"]), now=now or _now())
            return attempt, f"失败回执已写回，退避级别升至{next_level}，下次重试时间 {next_due.isoformat(timespec='seconds')}"

    # ---------------------------------------------------------------- 数据集查询
    def list_attempts(self, *, job_no: str | None = None, url: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(ATTEMPT_TABLE)
        if job_no:
            rows = [row for row in rows if row.get("作业编号") == job_no]
        if url:
            rows = [row for row in rows if url in str(row.get("失败URL", ""))]
        return rows

    def list_progress(
        self,
        *,
        job_no: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = store.rows(PROGRESS_TABLE)
        if job_no:
            rows = [row for row in rows if row.get("作业编号") == job_no]
        if batch_id:
            rows = [row for row in rows if row.get("批次号") == batch_id]
        if status:
            rows = [row for row in rows if row.get("尝试状态") == status]
        return rows

    def list_final_failures(self, *, job_no: str | None = None, active_only: bool = False) -> list[dict[str, Any]]:
        rows = store.rows(FINAL_FAILURE_TABLE)
        if job_no:
            rows = [row for row in rows if row.get("作业编号") == job_no]
        if active_only:
            rows = [row for row in rows if row.get("清单状态") == "在册"]
        return rows

    # ---------------------------------------------------------------- 批次封存/合并/校验
    def seal_batch(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        with _lock:
            operator = str(values.get("operator") or "").strip()
            role = str(values.get("role") or ROLE_VIEWER).strip()
            self._ensure_scope(role, "seal", operator)
            batch_id = str(values.get("批次号") or "").strip()
            rows = [row for row in store.rows(PROGRESS_TABLE) if row.get("批次号") == batch_id]
            if not rows:
                return None, f"批次 {batch_id} 下没有重试进度数据，无法封存"
            if any(row.get("批次状态") != "开放" for row in rows):
                return None, f"批次 {batch_id} 已封存或已合并，封存操作不可重复执行"
            stamp = _now_iso()
            for row in rows:
                row["批次状态"] = "已封存"
                row["快照时间"] = stamp
            record = integrity_service.register(
                [dict(row) for row in rows],
                dataset="retry_progress",
                batch_id=batch_id,
                batch_version=int(rows[0].get("批次版本", 1)),
                row_count=len(rows),
                created_at=stamp,
            )
            return record, f"批次 {batch_id} 已封存并登记完整性校验和"

    def merge_batches(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """合并两个批次版本：同键保留高版本，跨作业来源冲突显式标记。"""
        with _lock:
            operator = str(values.get("operator") or "").strip()
            role = str(values.get("role") or ROLE_VIEWER).strip()
            self._ensure_scope(role, "merge", operator)
            batch_a = str(values.get("批次号A") or "").strip()
            batch_b = str(values.get("批次号B") or "").strip()
            if not batch_a or not batch_b or batch_a == batch_b:
                return None, "需要提供两个不同的批次号才能合并"
            rows_a = [row for row in store.rows(PROGRESS_TABLE) if row.get("批次号") == batch_a]
            rows_b = [row for row in store.rows(PROGRESS_TABLE) if row.get("批次号") == batch_b]
            if not rows_a or not rows_b:
                return None, "两个批次都必须存在且包含数据才能合并"
            stamp = _now_iso()
            merged_id = self._next_batch_no()
            merged: dict[tuple[str, str], dict[str, Any]] = {}
            for row in rows_a + rows_b:
                key = (str(row.get("作业编号")), str(row.get("失败URL")))
                old = merged.get(key)
                if old is None or int(row.get("批次版本", 1)) >= int(old.get("批次版本", 1)):
                    merged[key] = dict(row)
            # 同一URL跨作业、处理来源不一致即为批次边界冲突，合并时显式确认。
            url_sources: dict[str, set[str]] = {}
            for row in merged.values():
                url_sources.setdefault(str(row.get("失败URL")), set()).add(str(row.get("处理来源")))
            for row in merged.values():
                conflicted = len(url_sources.get(str(row.get("失败URL")), set())) > 1 or row.get("冲突标记") != "无"
                row.update({
                    "批次号": merged_id,
                    "批次版本": max(int(r.get("批次版本", 1)) for r in rows_a + rows_b) + 1,
                    "批次状态": "已封存",
                    "合并状态": "已合并",
                    "冲突标记": "合并已确认" if conflicted else "无",
                    "快照时间": stamp,
                    "更新时间": stamp,
                })
            for source in (rows_a, rows_b):
                for row in source:
                    row["批次状态"] = "已合并"
                    row["合并状态"] = "已合并"
            merged_rows = list(merged.values())
            store.rows(PROGRESS_TABLE).extend(merged_rows)
            record = integrity_service.register(
                merged_rows,
                dataset="retry_progress",
                batch_id=merged_id,
                batch_version=int(merged_rows[0]["批次版本"]),
                row_count=len(merged_rows),
                created_at=stamp,
            )
            return record, f"批次 {batch_a} 与 {batch_b} 已合并为 {merged_id}，冲突条目已在数据集中标记"

    def _next_batch_no(self) -> str:
        existing = {str(row.get("批次号", "")) for row in store.rows(PROGRESS_TABLE)}
        seq = 1
        while f"B-{seq:04d}" in existing:
            seq += 1
        return f"B-{seq:04d}"

    def verify_dataset(self, values: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(values.get("批次号") or "").strip()
        dataset = str(values.get("数据集类型") or "retry_progress").strip()
        table = FINAL_FAILURE_TABLE if dataset == "retry_final_failure" else PROGRESS_TABLE
        rows = [row for row in store.rows(table) if row.get("批次号") == batch_id or row.get("快照批次") == batch_id]
        return integrity_service.verify(batch_id, current_rows=rows, verified_at=_now_iso())

    def integrity_ledger(self) -> list[dict[str, Any]]:
        return integrity_service.list_ledger()

    # ---------------------------------------------------------------- 内部数据维护
    def _mark_cross_job_conflict(self, progress: dict[str, Any], stamp: str) -> None:
        url = str(progress.get("失败URL"))
        others = [
            row for row in store.rows(PROGRESS_TABLE)
            if row.get("失败URL") == url
            and row.get("作业编号") != progress.get("作业编号")
            and row.get("尝试状态") == "重试成功"
        ]
        if others:
            source = str(others[0].get("作业编号"))
            progress["冲突标记"] = "跨作业来源冲突"
            progress["处理来源"] = f"其他作业:{source}"
            progress["更新时间"] = stamp
            for row in others:
                if row.get("批次状态") == "开放":
                    row["冲突标记"] = "跨作业来源冲突"
                    row["更新时间"] = stamp

    def _remove_from_final_failures(self, job_no: str, url: str, stamp: str) -> bool:
        removed = False
        for row in store.rows(FINAL_FAILURE_TABLE):
            if row.get("作业编号") == job_no and row.get("失败URL") == url and row.get("清单状态") == "在册":
                row["清单状态"] = "已移除"
                row["移除时间"] = stamp
                row["恢复结果"] = "重试成功，成功回执写回后移出最终失败清单"
                removed = True
        return removed

    def _append_final_failure(self, attempt: dict[str, Any], reason: str, stamp: str) -> dict[str, Any]:
        rows = store.rows(FINAL_FAILURE_TABLE)
        # 同一作业同一URL只保留一条在册最终失败，重复入单走幂等。
        existing = next(
            (row for row in rows
             if row.get("作业编号") == attempt["作业编号"]
             and row.get("失败URL") == attempt["失败URL"]
             and row.get("清单状态") == "在册"),
            None,
        )
        if existing is not None:
            return existing
        snapshot_batch = self._next_failure_snapshot_no()
        entry = {
            "id": self._next_id(FINAL_FAILURE_TABLE),
            "作业编号": attempt["作业编号"],
            "失败URL": attempt["失败URL"],
            "失败原因码": reason,
            "退避级别": MAX_BACKOFF_LEVEL,
            "尝试编号": attempt["尝试编号"],
            "节点编号": attempt.get("节点编号"),
            "入单时间": stamp,
            "清单状态": "在册",
            "移除时间": None,
            "恢复结果": attempt.get("恢复结果"),
            "快照批次": snapshot_batch,
            "快照时间": stamp,
        }
        rows.append(entry)
        integrity_service.register(
            [dict(entry)],
            dataset="retry_final_failure",
            batch_id=snapshot_batch,
            batch_version=1,
            row_count=1,
            created_at=stamp,
        )
        return entry

    def _next_failure_snapshot_no(self) -> str:
        existing = {str(row.get("快照批次", "")) for row in store.rows(FINAL_FAILURE_TABLE)}
        seq = 1
        while f"F-{seq:04d}" in existing:
            seq += 1
        return f"F-{seq:04d}"

    # ---------------------------------------------------------------- 回写采集执行侧
    def sync_job_failures(self, job_no: str, *, now: datetime | None = None) -> dict[str, Any]:
        """把重试进度落到采集执行侧作业失败清单；不改变作业状态（停止的作业不会复活）。"""
        from app.services.crawljob import crawljob_service

        return crawljob_service.consume_retry_progress(job_no, now=now or _now())


# 服务单例，路由层与采集执行侧共用。
retryqueue_service = RetryqueueService()
