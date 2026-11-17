"""抓取作业接口：作业定义动作 + 采集执行侧失败清单（消费重试进度）。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.crawljob import crawljob_service
from app.services.retryqueue import retryqueue_service

router = APIRouter(prefix="/api/crawljob", tags=["抓取作业"])

LIST_FIELDS = ["作业编号", "目标站点", "起始URL", "爬取深度", "URL去重策略", "页面解析器", "下载超时秒", "最大页面数"]
STATUSES = ["草稿", "作业审批中", "执行中", "已完成", "已停止"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出抓取作业清单：返回当前过滤条件下的全量数据。"""
    items, total = crawljob_service.list_entries(page=1, size=10000)
    return {"module": "crawljob", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按作业编号检索"),
    status: str | None = Query(default=None, description="草稿、作业审批中、执行中、已完成、已停止"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按作业编号与状态过滤抓取作业列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = crawljob_service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条作业定义明细；不存在时给出可读的错误说明。"""
    entry = crawljob_service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"作业定义 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条作业定义，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = crawljob_service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="作业定义已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条作业定义执行提交作业审批、启动作业、停止作业；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = crawljob_service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


# ------------------------------------------------------------ 采集执行侧失败清单
@router.get("/failures/list", response_model=dict)
def list_failure_items(
    job_no: str | None = Query(default=None, description="按作业编号过滤"),
    snapshot: str | None = Query(default=None, description="按快照批次查看历史版本"),
    active_only: bool = Query(default=False, description="只看在测条目，排除快照与已移除"),
) -> dict[str, Any]:
    """作业失败清单：重试进度回写后的采集执行侧记录，含每次同步快照。"""
    items = crawljob_service.list_failure_items(job_no=job_no, snapshot=snapshot, active_only=active_only)
    return {"module": "job_failure_item", "total": len(items), "items": items}


@router.post("/failures/sync", response_model=ActionResult)
def sync_failures(payload: EntryPayload) -> ActionResult:
    """主动拉取重试进度并回写到作业失败清单；作业已停止时只更新清单、不改作业状态。"""
    job_no = str(payload.values.get("作业编号") or "").strip()
    if not job_no:
        return ActionResult(ok=False, message="缺少必填字段：作业编号")
    try:
        summary = retryqueue_service.sync_job_failures(job_no)
    except ValueError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message=f"作业 {job_no} 失败清单已按重试进度同步", entry=summary)


@router.post("/failures/verify", response_model=ActionResult)
def verify_failure_snapshot(payload: EntryPayload) -> ActionResult:
    """校验作业失败清单某个快照批次的完整性。"""
    snapshot = str(payload.values.get("快照批次") or "").strip()
    if not snapshot:
        return ActionResult(ok=False, message="缺少必填字段：快照批次")
    result = crawljob_service.verify_failure_snapshot(snapshot)
    return ActionResult(ok=bool(result.get("通过")), message=str(result.get("信息")), entry=result)
