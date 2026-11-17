"""抓取作业业务规则：状态流转、字段校验、失败清单消费都收在这里。

采集执行侧通过 consume_retry_progress 消费重试队列的进度数据集，把结果落到本作业的
失败清单（job_failure_item），并对每次同步生成可校验的快照。注意：同步只更新清单，
绝不改写作业本身状态——已停止的作业不会被晚到的重试带回执行中。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.integrity import integrity_service
from app.store import store

MODULE = "crawljob"
FAILURE_TABLE = "job_failure_item"
REQUIRED_FIELDS = ["作业编号", "目标站点", "起始URL"]
STATUS_ORDER = ["草稿", "作业审批中", "执行中", "已完成", "已停止"]
ACTION_RULES = {"提交作业审批": "作业审批中", "启动作业": "执行中", "停止作业": "已停止"}
NEGATIVE_ACTIONS = []


class CrawljobService:
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
            rows = [row for row in rows if keyword in str(row.get("作业编号", ""))]
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

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"作业定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于抓取作业可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"作业定义已{action}"

    # ------------------------------------------------------------ 执行侧失败清单
    def list_failure_items(
        self,
        *,
        job_no: str | None = None,
        snapshot: str | None = None,
        active_only: bool = False,
    ) -> list[dict[str, Any]]:
        rows = store.rows(FAILURE_TABLE)
        if job_no:
            rows = [row for row in rows if row.get("作业编号") == job_no]
        if snapshot:
            rows = [row for row in rows if row.get("快照批次") == snapshot]
        if active_only:
            rows = [row for row in rows if row.get("清单状态") == "在册"]
        return rows

    def _next_id(self, table: str) -> int:
        return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1

    def _next_snapshot_no(self) -> str:
        existing = {str(row.get("快照批次", "")) for row in store.rows(FAILURE_TABLE)}
        seq = 1
        while f"J-{seq:04d}" in existing:
            seq += 1
        return f"J-{seq:04d}"

    def consume_retry_progress(self, job_no: str, *, now: datetime | None = None) -> dict[str, Any]:
        """消费重试进度数据集，按作业维度同步失败清单并登记快照校验和。

        口径：
        - 重试成功的URL -> 清单状态 已恢复
        - 退避耗尽且在最终失败清单在册的URL -> 最终失败（最终失败只来自退避耗尽条目）
        - 其余仍在重试链路中的URL -> 重试中
        - 作业已停止时只更新清单，条目标记“已停止冻结”，作业状态保持已停止。
        """
        job = next((row for row in store.rows(MODULE) if row.get("作业编号") == job_no), None)
        if job is None:
            raise ValueError(f"作业 {job_no} 不存在，无法同步失败清单")
        stamp = (now or datetime.now()).isoformat(timespec="seconds")
        progress_rows = [
            row for row in store.rows("retry_progress") if row.get("作业编号") == job_no
        ]
        final_rows = {
            row["失败URL"]: row
            for row in store.rows("retry_final_failure")
            if row.get("作业编号") == job_no and row.get("清单状态") == "在册"
        }
        table = store.rows(FAILURE_TABLE)
        current_index = {
            (row["作业编号"], row["失败URL"]): row
            for row in table
            if row.get("快照批次") is None
        }
        frozen = job.get("status") == "已停止"
        synced: list[dict[str, Any]] = []
        for progress in progress_rows:
            url = str(progress["失败URL"])
            if url in final_rows:
                state = "最终失败"
            elif progress.get("尝试状态") == "重试成功":
                state = "已恢复"
            else:
                state = "重试中"
            item = current_index.get((job_no, url))
            if item is None:
                item = {
                    "id": self._next_id(FAILURE_TABLE),
                    "作业编号": job_no,
                    "失败URL": url,
                    "快照批次": None,
                }
                table.append(item)
                current_index[(job_no, url)] = item
            item.update({
                "退避级别": progress.get("退避级别"),
                "已重试次数": progress.get("已重试次数"),
                "尝试编号": progress.get("尝试编号"),
                "节点编号": progress.get("节点编号"),
                "尝试状态": progress.get("尝试状态"),
                "恢复结果": progress.get("恢复结果"),
                "清单状态": state,
                "失败原因码": progress.get("失败原因码"),
                "处理来源": progress.get("处理来源", "retryqueue"),
                "冲突标记": progress.get("冲突标记", "无"),
                "作业冻结": "已停止冻结" if frozen else "正常",
                "更新时间": stamp,
            })
            synced.append(item)

        snapshot_batch = self._next_snapshot_no()
        snapshot_rows: list[dict[str, Any]] = []
        for item in synced:
            snap = {**item, "id": self._next_id(FAILURE_TABLE), "快照批次": snapshot_batch, "快照时间": stamp}
            table.append(snap)
            snapshot_rows.append(dict(snap))
        ledger = integrity_service.register(
            snapshot_rows,
            dataset="job_failure",
            batch_id=snapshot_batch,
            batch_version=1,
            row_count=len(snapshot_rows),
            created_at=stamp,
        )
        return {
            "作业编号": job_no,
            "作业状态": job.get("status"),
            "同步条目数": len(synced),
            "最终失败数": sum(1 for row in synced if row["清单状态"] == "最终失败"),
            "已恢复数": sum(1 for row in synced if row["清单状态"] == "已恢复"),
            "重试中数": sum(1 for row in synced if row["清单状态"] == "重试中"),
            "作业状态是否被改写": False,
            "快照批次": snapshot_batch,
            "校验和": ledger["校验和"],
            "同步时间": stamp,
        }

    def verify_failure_snapshot(self, snapshot: str) -> dict[str, Any]:
        rows = self.list_failure_items(snapshot=snapshot)
        return integrity_service.verify(
            snapshot,
            current_rows=rows,
            verified_at=datetime.now().isoformat(timespec="seconds"),
        )


crawljob_service = CrawljobService()
