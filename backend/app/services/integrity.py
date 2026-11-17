"""数据集完整性：对重试进度、最终失败清单、采集执行侧失败清单做快照校验。

两层校验口径：
1. 快照完整性：登记批次时把数据集规范化内容（canonical JSON）与 SHA-256 校验和
   一起写入台账；验证时用台账内冻结内容重算校验和，证明台账与快照自洽、未被篡改。
2. 当前一致性：用同一口径重算当前数据集内容，与快照校验和比对——任何后续改动
   （正常生命周期流转或未授权篡改）都会表现为漂移，可被发现。

任何一行被改动、新增或删除都会导致当前一致性校验不通过。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# 参与校验和的稳定字段；id 是内存仓库行号，不纳入业务内容校验。
STABLE_FIELDS = [
    "数据集类型",
    "批次号",
    "批次版本",
    "作业编号",
    "失败URL",
    "退避级别",
    "已重试次数",
    "下次重试时间",
    "尝试编号",
    "尝试token",
    "节点编号",
    "尝试状态",
    "恢复结果",
    "失败原因码",
    "来源作业",
    "处理来源",
    "批次状态",
    "合并状态",
    "冲突标记",
    "快照时间",
    "更新时间",
    "清单状态",
]


def canonical_payload(rows: list[dict[str, Any]]) -> str:
    """把数据集行序列化为稳定字符串：字段固定顺序、行按业务键排序。"""
    normalized: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: (str(item.get("作业编号", "")), str(item.get("失败URL", "")))):
        normalized.append({field: row.get(field) for field in STABLE_FIELDS if field in row})
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def checksum(rows: list[dict[str, Any]]) -> str:
    """计算数据集内容的 SHA-256 校验和。"""
    return hashlib.sha256(canonical_payload(rows).encode("utf-8")).hexdigest()


class IntegrityService:
    """完整性台账的登记与验证，所有重试数据集共用一份台账。"""

    TABLE = "retry_integrity"

    def register(
        self,
        rows: list[dict[str, Any]],
        *,
        dataset: str,
        batch_id: str,
        batch_version: int,
        row_count: int,
        created_at: str,
    ) -> dict[str, Any]:
        from app.store import store

        table = store.rows(self.TABLE)
        next_id = max((int(row.get("id", 0)) for row in table), default=0) + 1
        frozen = canonical_payload(rows)
        record = {
            "id": next_id,
            "台账编号": f"INTG-{next_id:04d}",
            "数据集类型": dataset,
            "批次号": batch_id,
            "批次版本": batch_version,
            "行数": row_count,
            "校验和": hashlib.sha256(frozen.encode("utf-8")).hexdigest(),
            "快照数据": frozen,
            "登记时间": created_at,
            "校验状态": "已登记",
            "最后校验时间": None,
        }
        table.append(record)
        return record

    def verify(self, batch_id: str, *, current_rows: list[dict[str, Any]], verified_at: str) -> dict[str, Any]:
        """按批次号验证快照完整性，并比对当前数据集与快照是否一致。"""
        from app.store import store

        record = next(
            (row for row in store.rows(self.TABLE) if row.get("批次号") == batch_id),
            None,
        )
        if record is None:
            return {
                "批次号": batch_id,
                "通过": False,
                "快照完整": False,
                "当前一致": False,
                "信息": "完整性台账中没有该批次，无法验证",
            }
        frozen_rows = json.loads(str(record.get("快照数据") or "[]"))
        frozen_checksum = hashlib.sha256(str(record.get("快照数据")).encode("utf-8")).hexdigest()
        snapshot_intact = frozen_checksum == record.get("校验和") and checksum(frozen_rows) == record.get("校验和")
        actual = checksum(current_rows)
        current_match = actual == record.get("校验和") and len(current_rows) == int(record.get("行数", -1))
        passed = snapshot_intact and current_match
        record["校验状态"] = "通过" if passed else ("快照不一致" if not snapshot_intact else "当前数据漂移")
        record["最后校验时间"] = verified_at
        if not snapshot_intact:
            message = "台账快照校验和无法重算一致，快照本身已被篡改"
        elif not current_match:
            message = "快照完整，但当前数据集与登记快照不一致：存在后续流转或未授权改动"
        else:
            message = "数据集内容与登记批次一致，完整性验证通过"
        return {
            "批次号": batch_id,
            "数据集类型": record.get("数据集类型"),
            "登记校验和": record.get("校验和"),
            "当前重算校验和": actual,
            "登记行数": record.get("行数"),
            "当前行数": len(current_rows),
            "快照完整": snapshot_intact,
            "当前一致": current_match,
            "通过": passed,
            "校验时间": verified_at,
            "信息": message,
        }

    def list_ledger(self) -> list[dict[str, Any]]:
        from app.store import store

        # 台账列表不向前端回传大块冻结 JSON，只返回校验元数据。
        return [{k: v for k, v in row.items() if k != "快照数据"} for row in store.rows(self.TABLE)]


integrity_service = IntegrityService()
