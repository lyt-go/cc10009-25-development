"""重试队列接口：重试条目维护 + 重试尝试/进度/最终失败数据集。

受控动作（登记失败URL、抢占槽位、结果回写、批次封存合并）需要在请求体里带
operator（操作人）与 role（角色：管理员/作业负责人/节点执行员/查看者）。
查看者只读，越权返回 403；并发抢占冲突返回 409。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.retryqueue import (
    ROLES,
    ConflictError,
    PermissionError as RetryPermissionError,
    retryqueue_service,
)
from app.services.retryqueue import MODULE

router = APIRouter(prefix="/api/retryqueue", tags=["重试队列"])

LIST_FIELDS = ["条目编号", "失败URL", "失败原因码", "首次失败时间", "已重试次数", "下次重试时间", "退避级别", "最终处置"]
STATUSES = ["草稿", "等待重试", "重试中", "重试成功", "最终失败"]


def _identity(values: dict[str, Any]) -> tuple[str, str]:
    return str(values.get("operator") or "").strip(), str(values.get("role") or "查看者").strip()


# ---------------------------------------------------------------- 原有重试条目
@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出重试队列清单：返回当前过滤条件下的全量数据。"""
    items, total = retryqueue_service.list_entries(page=1, size=10000)
    return {"module": MODULE, "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按条目编号检索"),
    status: str | None = Query(default=None, description="草稿、等待重试、重试中、重试成功、最终失败"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按条目编号与状态过滤重试队列列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = retryqueue_service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条重试条目明细；不存在时给出可读的错误说明。"""
    entry = retryqueue_service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"重试条目 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条重试条目，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = retryqueue_service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="重试条目已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条重试条目执行加入队列、发起重试、标记最终失败（最终失败须退避耗尽）。"""
    action = str(payload.values.get("action") or "").strip()
    operator, role = _identity(payload.values)
    try:
        entry, message = retryqueue_service.run_action(entry_id, action, operator=operator, role=role)
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


# ---------------------------------------------------------------- 重试尝试：发现/抢占/回写
@router.post("/discoveries", response_model=ActionResult)
def discover_failure(payload: EntryPayload) -> ActionResult:
    """采集执行侧登记失败URL；同一作业同一URL重复发现只返回原记录，不生成第二份尝试。"""
    operator, role = _identity(payload.values)
    try:
        entry, message = retryqueue_service.discover_failure({**payload.values, "operator": operator, "role": role})
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/attempts/claim", response_model=ActionResult)
def claim_attempt(payload: EntryPayload) -> ActionResult:
    """节点抢占重试槽位；退避未到点/作业已停止/已有槽位占用都会被拦下。"""
    operator, role = _identity(payload.values)
    try:
        entry, message = retryqueue_service.claim_attempt({**payload.values, "operator": operator, "role": role})
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/attempts/writeback", response_model=ActionResult)
def writeback_result(payload: EntryPayload) -> ActionResult:
    """写回抓取结果：校验尝试token与节点身份；重复回执幂等，成功移除最终失败。"""
    operator, role = _identity(payload.values)
    try:
        entry, message = retryqueue_service.writeback_result({**payload.values, "operator": operator, "role": role})
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/attempts/list", response_model=dict)
def list_attempts(
    job_no: str | None = Query(default=None, description="按作业编号过滤尝试记录"),
    url: str | None = Query(default=None, description="按失败URL模糊过滤"),
) -> dict[str, Any]:
    """重试尝试明细：每次尝试所用节点、token、抢占/完成时间与恢复结果都可追溯。"""
    items = retryqueue_service.list_attempts(job_no=job_no, url=url)
    return {"module": "retry_attempt", "total": len(items), "items": items}


# ---------------------------------------------------------------- 重试进度数据集
@router.get("/progress/list", response_model=dict)
def list_progress(
    job_no: str | None = Query(default=None),
    batch_id: str | None = Query(default=None, description="按批次号过滤"),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """重试进度数据集，支持按作业、批次、尝试状态过滤。"""
    items = retryqueue_service.list_progress(job_no=job_no, batch_id=batch_id, status=status)
    return {"module": "retry_progress", "total": len(items), "items": items}


@router.post("/progress/seal", response_model=ActionResult)
def seal_batch(payload: EntryPayload) -> ActionResult:
    """封存一个重试进度批次并登记完整性校验和，封存后内容不可再改。"""
    operator, role = _identity(payload.values)
    try:
        record, message = retryqueue_service.seal_batch({**payload.values, "operator": operator, "role": role})
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.post("/progress/merge", response_model=ActionResult)
def merge_batches(payload: EntryPayload) -> ActionResult:
    """合并两个批次版本；同一失败URL跨作业来源冲突会在合并结果中显式标记。"""
    operator, role = _identity(payload.values)
    try:
        record, message = retryqueue_service.merge_batches({**payload.values, "operator": operator, "role": role})
    except RetryPermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.post("/integrity/verify", response_model=ActionResult)
def verify_dataset(payload: EntryPayload) -> ActionResult:
    """按批次号重算校验和并与台账比对，验证重试进度/最终失败清单完整性。"""
    result = retryqueue_service.verify_dataset(payload.values)
    return ActionResult(ok=bool(result.get("通过")), message=str(result.get("信息")), entry=result)


@router.get("/integrity/ledger", response_model=dict)
def integrity_ledger() -> dict[str, Any]:
    """完整性台账：每个已封存批次/快照的校验和与校验状态。"""
    items = retryqueue_service.integrity_ledger()
    return {"module": "retry_integrity", "total": len(items), "items": items, "roles": ROLES}


# ---------------------------------------------------------------- 最终失败清单
@router.get("/final-failures/list", response_model=dict)
def list_final_failures(
    job_no: str | None = Query(default=None),
    active_only: bool = Query(default=False, description="只看仍在册（未被成功回写移除）的条目"),
) -> dict[str, Any]:
    """最终失败清单：只包含退避耗尽的条目；成功回写后状态变为已移除。"""
    items = retryqueue_service.list_final_failures(job_no=job_no, active_only=active_only)
    return {"module": "retry_final_failure", "total": len(items), "items": items}
