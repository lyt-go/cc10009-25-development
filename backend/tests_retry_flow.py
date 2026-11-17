"""重试队列全链路验证脚本（独立目录运行：python -I tests_retry_flow.py）。

覆盖需求点：
1. 失败URL登记/重复发现幂等
2. 退避到点才派发、节点在线校验、并发抢占单槽位
3. 尝试身份（token+节点）校验、成功/失败回写、重复回执幂等
4. 退避耗尽才进最终失败清单、成功回写后移除
5. 作业停止后晚到重试不复活作业、停止后不派发/不登记
6. 跨作业来源冲突标记、两个批次版本合并
7. 进度数据集/最终失败清单/执行侧清单快照完整性校验（含篡改检测）
8. 角色受控范围：查看者只读、节点执行员只能抢占回写
9. 每次尝试节点与恢复结果可追溯、执行侧清单同步口径
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from app.services.crawljob import crawljob_service
from app.services.retryqueue import (
    MAX_BACKOFF_LEVEL,
    ConflictError,
    PermissionError as RetryPermissionError,
    retryqueue_service as svc,
)
from app.services.integrity import integrity_service
from app.store import store

ADMIN = {"operator": "管理员甲", "role": "管理员"}
WRITER = {"operator": "负责人乙", "role": "作业负责人"}
WORKER1 = {"operator": "节点员丙", "role": "节点执行员", "节点编号": "WORK-0003"}
WORKER2 = {"operator": "节点员丁", "role": "节点执行员", "节点编号": "WORK-0002"}
VIEWER = {"operator": "旁观者戊", "role": "查看者"}

T0 = datetime(2026, 11, 17, 8, 0, 0, tzinfo=timezone.utc)
passed: list[str] = []
failed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        passed.append(name)
        print(f"  PASS  {name}")
    else:
        failed.append(f"{name} {detail}")
        print(f"  FAIL  {name} {detail}")


def reset_tables() -> None:
    for table in ("retry_attempt", "retry_progress", "retry_final_failure", "retry_integrity", "job_failure_item"):
        store.rows(table).clear()


print("== 1. 失败URL登记与重复发现 ==")
reset_tables()
p, msg = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0003", "失败URL": "https://a.example/p1", "失败原因码": "E500"})
check("登记失败URL生成进度记录", p is not None and p["退避级别"] == 1 and p["尝试状态"] == "等待派发")
p2, msg2 = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0003", "失败URL": "https://a.example/p1", "失败原因码": "E500"})
check("重复发现不生成第二份记录（幂等）", p2["id"] == p["id"] and "重复发现" in msg2)
check("重复发现不产生尝试记录", len(svc.list_attempts(job_no="CRAW-0003")) == 0)
p_bad, _ = svc.discover_failure({**ADMIN, "作业编号": "NO-JOB", "失败URL": "u", "失败原因码": "E"})
check("未知作业拒绝登记", p_bad is None)

print("== 2. 角色受控范围 ==")
for fn, payload, label in (
    (svc.discover_failure, {**VIEWER, "作业编号": "CRAW-0003", "失败URL": "u1", "失败原因码": "E"}, "查看者登记"),
    (svc.claim_attempt, {**VIEWER, "作业编号": "CRAW-0003", "失败URL": "u1"}, "查看者抢占"),
    (svc.claim_attempt, {**WRITER, "作业编号": "CRAW-0003", "失败URL": "u1", "节点编号": "WORK-0003"}, "负责人抢占"),
):
    try:
        fn(payload)
        check(f"{label}被拒绝", False, "未抛权限错误")
    except RetryPermissionError:
        check(f"{label}被拒绝", True)
p3, _ = svc.discover_failure({**WRITER, "作业编号": "CRAW-0003", "失败URL": "u1", "失败原因码": "E403"})
check("作业负责人可登记失败URL", p3 is not None)
try:
    svc.run_action(1, "加入队列", operator="旁观者戊", role="查看者")
    check("查看者不能改原清单", False)
except RetryPermissionError:
    check("查看者不能改原清单", True)
check("查看者可只读列表", svc.list_progress() is not None and svc.integrity_ledger() is not None)

print("== 3. 退避到点才派发 + 节点在线校验 ==")
url1 = "https://a.example/p1"
c, m = svc.claim_attempt({**WORKER2, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0)
check("非在线节点不能抢占", c is None and "不在线" in m)
c, m = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0)
check("退避级别1到点可抢占", c is not None and c["尝试状态"] == "重试中" and len(c["尝试token"]) == 32)
token1, att1 = c["尝试token"], c["尝试编号"]

print("== 4. 并发抢占只允许一个槽位 ==")
try:
    svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0)
    check("第二槽位被拒绝", False)
except ConflictError as e:
    check("第二槽位被拒绝(409冲突)", "槽位" in str(e) or "占用" in str(e))

print("== 5. 尝试身份校验与失败回写/退避推进 ==")
try:
    svc.writeback_result({**WORKER1, "尝试编号": att1, "尝试token": "wrong-token", "结果": "success"})
    check("错误token拒绝回写", False)
except RetryPermissionError:
    check("错误token拒绝回写", True)
try:
    svc.writeback_result({**WORKER1, "尝试编号": att1, "尝试token": token1, "节点编号": "WORK-0001", "结果": "success"})
    check("他人节点拒绝回写", False)
except RetryPermissionError:
    check("他人节点拒绝回写", True)
a, m = svc.writeback_result({**WORKER1, "尝试编号": att1, "尝试token": token1, "结果": "failure", "失败原因码": "E503"}, now=T0)
check("失败回写退避级别升至2", a is not None and svc.list_progress(job_no="CRAW-0003")[0]["退避级别"] == 2)
c, m = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0 + timedelta(seconds=1))
check("退避未到点拒绝派发", c is None and "退避时间未到点" in m)
c, _ = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0 + timedelta(seconds=10))
check("到点后可再次抢占（级别2延迟5秒）", c is not None and c["退避级别"] == 2)

print("== 6. 重复回执幂等 ==")
a, m = svc.writeback_result({**WORKER1, "尝试编号": c["尝试编号"], "尝试token": c["尝试token"], "结果": "failure", "失败原因码": "E503"}, now=T0 + timedelta(seconds=10))
n_attempts = len(svc.list_attempts(job_no="CRAW-0003"))
a2, m2 = svc.writeback_result({**WORKER1, "尝试编号": c["尝试编号"], "尝试token": c["尝试token"], "结果": "failure", "失败原因码": "E503"}, now=T0 + timedelta(seconds=11))
check("重复失败回执幂等不新增尝试", len(svc.list_attempts(job_no="CRAW-0003")) == n_attempts and "幂等" in m2)

print("== 7. 退避耗尽 -> 最终失败清单；成功回写移除 ==")
# 把 url1 一路失败到耗尽：每次按数据集算出的下次重试时间到点后再派发
for _ in range(3, MAX_BACKOFF_LEVEL + 1):
    due = datetime.fromisoformat(svc.list_progress(job_no="CRAW-0003")[0]["下次重试时间"]) + timedelta(seconds=1)
    cc, mm = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=due)
    assert cc is not None, mm
    aa, mm = svc.writeback_result({**WORKER1, "尝试编号": cc["尝试编号"], "尝试token": cc["尝试token"], "结果": "failure", "失败原因码": "E503"}, now=due)
finals = svc.list_final_failures(job_no="CRAW-0003", active_only=True)
check(f"退避耗尽({MAX_BACKOFF_LEVEL}级)才进最终失败清单", len(finals) == 1 and finals[0]["失败URL"] == url1)
prog = svc.list_progress(job_no="CRAW-0003")[0]
check("进度记录指向最终失败", prog["尝试状态"] == "最终失败" and prog["退避级别"] == MAX_BACKOFF_LEVEL)
# 旧动作：未耗尽的条目不允许标记最终失败（找一条草稿条目 id=1 未耗尽）
e, m = svc.run_action(1, "标记最终失败", **ADMIN)
check("未耗尽条目不能手工标记最终失败", e is None and "退避" in m)
# 该URL再成功一次：先重新登记不行（已存在），直接由节点抢占——耗尽后进度为最终失败，无开放尝试
# 模拟恢复：另一个作业抓成功后，本作业负责人对同一URL重新开放一条恢复链路
p_other, _ = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0001", "失败URL": url1, "失败原因码": "E500"})
cc, _ = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0001", "失败URL": url1}, now=T0 + timedelta(hours=1))
aa, m = svc.writeback_result({**WORKER1, "尝试编号": cc["尝试编号"], "尝试token": cc["尝试token"], "结果": "success", "恢复结果": "HTTP200 内容完整"}, now=T0 + timedelta(hours=1))
check("成功回执写回恢复结果", aa["尝试状态"] == "重试成功" and "HTTP200" in aa["恢复结果"])
# 本作业最终失败条目在本作业成功回写后才移除：模拟本作业重新派发成功
store.rows("retry_progress")[0]["尝试状态"] = "等待派发"
store.rows("retry_progress")[0]["下次重试时间"] = (T0 + timedelta(hours=2)).isoformat()
cc2, _ = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": url1}, now=T0 + timedelta(hours=2))
aa2, m = svc.writeback_result({**WORKER1, "尝试编号": cc2["尝试编号"], "尝试token": cc2["尝试token"], "结果": "success", "恢复结果": "HTTP200 恢复"}, now=T0 + timedelta(hours=2))
check("成功回写后从最终失败清单移除", "已从最终失败清单移除" in m and len(svc.list_final_failures(job_no="CRAW-0003", active_only=True)) == 0)
a3, m3 = svc.writeback_result({**WORKER1, "尝试编号": cc2["尝试编号"], "尝试token": cc2["尝试token"], "结果": "success", "恢复结果": "HTTP200 恢复"}, now=T0 + timedelta(hours=2))
check("重复成功回执幂等只确认原尝试", "幂等" in m3 and a3["尝试编号"] == cc2["尝试编号"])

print("== 8. 尝试可追溯：节点与恢复结果 ==")
attempts = svc.list_attempts(job_no="CRAW-0003")
check("每次尝试记录节点/token/时间/结果", all(a["节点编号"] == "WORK-0003" and a["尝试token"] and a["抢占时间"] for a in attempts))
check("尝试链覆盖全部退避级别", sorted({a["退避级别"] for a in attempts}) == list(range(1, MAX_BACKOFF_LEVEL + 1)))

print("== 9. 作业停止：晚到重试不复活作业 ==")
job = next(r for r in store.rows("crawljob") if r["作业编号"] == "CRAW-0003")
job["status"] = "已停止"
c, m = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0003", "失败URL": "u1"}, now=T0 + timedelta(hours=3))
check("停止作业不派发槽位", c is None and "已停止" in m)
p, m = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0003", "失败URL": "u-stopped", "失败原因码": "E"})
check("停止作业拒绝新登记", p is None and "已停止" in m)
# 晚到回执：在另一仍在执行的作业上完成重试，同步时绝不改写任何作业状态
dp, dm = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0001", "失败URL": "u1", "失败原因码": "E500"})
assert dp is not None, dm
cc, mm = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0001", "失败URL": "u1"}, now=T0 + timedelta(hours=3))
assert cc is not None, mm
aa, mm = svc.writeback_result({**WORKER1, "尝试编号": cc["尝试编号"], "尝试token": cc["尝试token"], "结果": "success"}, now=T0 + timedelta(hours=3))
sync = svc.sync_job_failures("CRAW-0001", now=T0 + timedelta(hours=3))
check("晚到重试同步后作业状态不改写", sync["作业状态是否被改写"] is False and sync["作业状态"] == "草稿")
sync3 = svc.sync_job_failures("CRAW-0003", now=T0 + timedelta(hours=3))
check("停止作业清单条目标记冻结且作业仍停止", sync3["作业状态"] == "已停止" and all(r["作业冻结"] == "已停止冻结" for r in crawljob_service.list_failure_items(job_no="CRAW-0003") if r.get("快照批次") is None))

print("== 10. 执行侧失败清单同步口径 ==")
items = [r for r in crawljob_service.list_failure_items(job_no="CRAW-0003") if r.get("快照批次") is None]
check("执行侧清单含重试进度回写字段", items and all("清单状态" in r and "处理来源" in r for r in items))
check("执行侧不存在非退避耗尽的最终失败", all(not (r["清单状态"] == "最终失败") or r["退避级别"] == MAX_BACKOFF_LEVEL for r in items))
check("同步摘要统计自洽", sync3["最终失败数"] + sync3["已恢复数"] + sync3["重试中数"] == sync3["同步条目数"])

print("== 11. 完整性：封存/校验/篡改检测 ==")
rec, m = svc.seal_batch({**ADMIN, "批次号": store.rows("retry_progress")[0]["批次号"]})
bid = rec["批次号"]
v = svc.verify_dataset({"批次号": bid, "数据集类型": "retry_progress"})
check("封存批次校验通过（快照完整且当前一致）", v["通过"] is True and v["快照完整"] is True and v["当前一致"] is True)
target = next(r for r in store.rows("retry_progress") if r["批次号"] == bid)
target["恢复结果"] = "被人篡改"
v2 = svc.verify_dataset({"批次号": bid, "数据集类型": "retry_progress"})
check("篡改后当前一致性校验不通过", v2["通过"] is False and v2["当前一致"] is False and v2["快照完整"] is True)
target["恢复结果"] = None  # 还原
# 最终失败清单快照：成功回写移除是正常生命周期流转，快照仍完整，当前表现为漂移
ff_rows = svc.list_final_failures()
f_batch = next(r["快照批次"] for r in ff_rows if r.get("快照批次", "").startswith("F-"))
vf = svc.verify_dataset({"批次号": f_batch, "数据集类型": "retry_final_failure"})
check("最终失败清单快照完整可验证", vf["快照完整"] is True and vf["当前一致"] is False)
# 对在册最终失败行做未授权改动，同样能被当前一致性发现
live_ff = next((r for r in store.rows("retry_final_failure") if r.get("清单状态") == "在册"), None)
if live_ff is not None:
    original = live_ff["失败原因码"]
    live_ff["失败原因码"] = "E999-篡改"
    # 重新登记一个当前在册批次用于比对
    svc2_rec = integrity_service.register(
        [dict(live_ff)], dataset="retry_final_failure", batch_id="F-TAMPER",
        batch_version=1, row_count=1, created_at=T0.isoformat(),
    )
    vt = svc.verify_dataset({"批次号": "F-TAMPER", "数据集类型": "retry_final_failure"})
    check("在册最终失败行篡改可检出", vt["当前一致"] is False)
    live_ff["失败原因码"] = original
# 执行侧快照
j_batch = sync3["快照批次"]
vj = crawljob_service.verify_failure_snapshot(j_batch)
check("执行侧清单快照校验通过", vj["通过"] is True)
snap_row = next(r for r in store.rows("job_failure_item") if r["快照批次"] == j_batch)
snap_row["清单状态"] = "最终失败"
vj2 = crawljob_service.verify_failure_snapshot(j_batch)
check("执行侧快照篡改可检出", vj2["通过"] is False and vj2["当前一致"] is False)

print("== 12. 跨作业冲突与批次合并 ==")
# url1 已在 CRAW-0001 成功；CRAW-0003 同名URL记录存在 -> 成功回写时已打冲突标记
conflicted = [r for r in svc.list_progress() if r.get("冲突标记") == "跨作业来源冲突"]
check("跨作业同URL成功抓取被标记冲突", len(conflicted) >= 2)
b_a = store.rows("retry_progress")[0]["批次号"]
# 造第二个批次版本
p_x, _ = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0001", "失败URL": "u-merge", "失败原因码": "E"})
b_b = p_x["批次号"]
merged, m = svc.merge_batches({**ADMIN, "批次号A": b_a, "批次号B": b_b})
check("两个批次版本合并生成新批次", merged is not None and "已合并" in m)
merged_rows = svc.list_progress(batch_id=merged["批次号"])
check("合并结果含合并状态与冲突确认", merged_rows and all(r["合并状态"] == "已合并" for r in merged_rows))
vm = svc.verify_dataset({"批次号": merged["批次号"], "数据集类型": "retry_progress"})
check("合并批次完整性可验证", vm["通过"] is True)
ledger = svc.integrity_ledger()
check("完整性台账登记全部快照", len(ledger) >= 4 and all(r["校验和"] for r in ledger))

print("== 13. 封存不可变：继续流转派生新版本，旧快照保持一致 ==")
p9, _ = svc.discover_failure({**ADMIN, "作业编号": "CRAW-0002", "失败URL": "u-seal", "失败原因码": "E"})
seal_rec, _ = svc.seal_batch({**ADMIN, "批次号": p9["批次号"]})
seal_id = seal_rec["批次号"]
vs0 = svc.verify_dataset({"批次号": seal_id, "数据集类型": "retry_progress"})
check("封存批次初始校验一致", vs0["通过"] is True)
# 封存后节点再抢占：派生开放新版本，原封存批次内容不变
cc, mm = svc.claim_attempt({**WORKER1, "作业编号": "CRAW-0002", "失败URL": "u-seal"})
assert cc is not None, mm
aa, mm = svc.writeback_result({**WORKER1, "尝试编号": cc["尝试编号"], "尝试token": cc["尝试token"], "结果": "success"}, now=T0 + timedelta(hours=4))
vs1 = svc.verify_dataset({"批次号": seal_id, "数据集类型": "retry_progress"})
check("封存批次在后续流转后仍保持快照一致（不可变）", vs1["快照完整"] is True and vs1["当前一致"] is True)
new_prog = svc.list_progress(job_no="CRAW-0002", status="重试成功")
check("流转写入派生的新批次版本", any(r["批次状态"] == "开放" and r["批次号"] != seal_id for r in new_prog))
# 重复封存被拒绝
again, m_again = svc.seal_batch({**ADMIN, "批次号": seal_id})
check("重复封存被拒绝", again is None and "不可重复" in m_again)
# 无身份旧重试入口停用
e, m = svc.run_action(1, "发起重试", **ADMIN)
check("旧版无token发起重试已停用", e is None and "抢占接口" in m)

print()
print(f"结果：通过 {len(passed)} 项，失败 {len(failed)} 项")
if failed:
    for item in failed:
        print("  -", item)
    sys.exit(1)
print("全部验证通过")
