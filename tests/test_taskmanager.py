#!/usr/bin/env python3
"""TaskManager 调度与停机收尾测试(无需真实 GDAL / FastAPI)。

在临时目录生成 gdalwarp / gdal_translate / gdalinfo 桩程序并前置到 PATH。
本仓库的运行环境可能禁止向进程发信号(沙箱),因此测试将 os.killpg 替换为
"记录调用 + 写哨兵文件让桩退出"的假实现,以验证编排逻辑;真实的进程组
SIGKILL 在 Docker 容器中生效(见 README 的手动验证步骤)。

停机场景通过环境变量让桩程序打点/卡住,构造停机与进程创建、阶段交接、
完成发布交错的窗口:
  TM_HOOK_DIR   桩进程经过关键点时写 <stage>-start/-end-<pid> 文件
  TM_START_GATE 文件存在时桩停留在"已创建、未交接"窗口
  TM_END_GATE   文件存在时桩停留在"已产出、未退出"的交接窗口
  TM_EXIT_DELAY 被终止后延迟退出,用于拉长收尾窗口
  COG_SLOW      gdal_translate 停留在 COG 阶段

运行: python3 -I tests/test_taskmanager.py
"""
import asyncio
import os
import signal
import stat
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import taskmanager as tm  # noqa: E402

STUB_PREAMBLE = """#!/usr/bin/env python3
import os, sys, time

def hook(name):
    marker = os.environ.get("TM_HOOK_DIR")
    if marker:
        open(os.path.join(marker, name + "-" + str(os.getpid())), "w").close()
    gate = os.environ.get("TM_START_GATE")
    if gate and os.path.exists(gate):
        deadline = time.time() + 60
        while time.time() < deadline:
            if not os.path.exists(gate):
                break
            time.sleep(0.02)

def end_gate():
    # 上一阶段已写完结果、尚未退出的交接窗口
    gate = os.environ.get("TM_END_GATE")
    if gate and os.path.exists(gate):
        deadline = time.time() + 60
        while time.time() < deadline:
            if not os.path.exists(gate):
                break
            time.sleep(0.02)

def die_instead():
    time.sleep(float(os.environ.get("TM_EXIT_DELAY", "0")))
    sys.exit(0)
"""

GDALWARP = STUB_PREAMBLE + """
args = sys.argv[1:]
hook("gdalwarp-start")
if any("slow" in a for a in args):
    # 等待只针对本进程(组)的哨兵: cwd 是任务目录
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(sentinel):
            die_instead()
        time.sleep(0.05)
else:
    open(args[-1], "wb").write(b"mosaic")
hook("gdalwarp-end")
end_gate()
"""

GDAL_TRANSLATE = STUB_PREAMBLE + """
import shutil
args = sys.argv[1:]
hook("gdal_translate-start")
if os.environ.get("COG_SLOW") == "1":
    sentinel = f"KILL-{os.getpid()}"
    deadline = time.time() + 60
    while time.time() < deadline:
        if os.path.exists(sentinel):
            die_instead()
        time.sleep(0.05)
shutil.copyfile(args[-2], args[-1])
hook("gdal_translate-end")
"""

GDALINFO = STUB_PREAMBLE + """
hook("gdalinfo-start")
print('{"size":[120,80],"geoTransform":[100.0,2.0,0.0,200.0,0.0,-2.0],'
      '"bands":[{}],"coordinateSystem":{"wkt":"PROJCS[stub]"}}')
"""


def make_stubs(root: Path) -> None:
    for name, body in [
        ("gdalwarp", GDALWARP),
        ("gdal_translate", GDAL_TRANSLATE),
        ("gdalinfo", GDALINFO),
    ]:
        p = root / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


async def wait_for(cond, timeout=10.0, what="condition"):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"超时等待: {what}")


class Harness:
    """承载 killpg 假实现;按场景切换任务根目录以便写哨兵。"""

    def __init__(self):
        self.killpg_calls = []
        self.tasks_root = None

    def install(self):
        def fake_killpg(pgid, sig):
            self.killpg_calls.append((pgid, sig))
            root = self.tasks_root
            if root is None or not root.is_dir():
                return
            for d in root.iterdir():
                if d.is_dir():
                    (d / f"KILL-{pgid}").write_text("1")

        self._real_killpg = os.killpg
        os.killpg = fake_killpg

    def uninstall(self):
        os.killpg = self._real_killpg


def new_mgr(harness, prefix, **kwargs) -> tm.TaskManager:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    harness.tasks_root = root
    return tm.TaskManager(root, **kwargs)


def mk(mgr, input_name, res=2.0):
    t = mgr.create([input_name], res)
    (t.inputs_dir / input_name).write_bytes(b"x")
    mgr.enqueue(t)
    return t


async def scenario_basic(harness):
    """原有调度行为:并行、排队、取消、清理,以及收尾后无残留。"""
    mgr = new_mgr(harness, "tm-basic-", max_parallel=2)

    t1 = mk(mgr, "00_slow.tif")
    t2 = mk(mgr, "00.tif")
    t3 = mk(mgr, "00.tif")
    await wait_for(
        lambda: t1.status == tm.STATUS_RUNNING and t2.status == tm.STATUS_RUNNING,
        what="t1/t2 进入 RUNNING",
    )
    assert t3.status == tm.STATUS_QUEUED, f"t3 应排队,实际 {t3.status}"

    mgr.cancel(t1.id)
    await wait_for(lambda: t1.status == tm.STATUS_CANCELLED, what="t1 变为 CANCELLED")
    assert any(sig == signal.SIGKILL for _, sig in harness.killpg_calls)
    assert not t1.workdir.exists(), "取消后暂存目录应被清理"
    assert t1.result is None, "取消任务不得发布结果"

    await wait_for(lambda: t2.status == tm.STATUS_DONE, what="t2 完成")
    await wait_for(lambda: t3.status == tm.STATUS_DONE, what="t3 补位完成")
    assert t2.result["width"] == 120 and t2.result["height"] == 80
    assert t2.result["extent"] == [100.0, 40.0, 340.0, 200.0], t2.result["extent"]
    assert t2.result["crs"] == "EPSG:3857"
    assert (t2.workdir / "result.tif").exists(), "成功任务应保留结果文件"

    os.environ["COG_SLOW"] = "1"
    try:
        s1 = mk(mgr, "00.tif")
        s2 = mk(mgr, "00.tif")
        q = mk(mgr, "00.tif")
        await wait_for(
            lambda: s1.status == tm.STATUS_RUNNING and s2.status == tm.STATUS_RUNNING,
            what="s1/s2 进入 RUNNING",
        )
        assert q.status == tm.STATUS_QUEUED
        mgr.cancel(q.id)
        assert q.status == tm.STATUS_CANCELLED
        assert not q.workdir.exists()

        mgr.cancel(s1.id)
        await wait_for(lambda: s1.status == tm.STATUS_CANCELLED, what="s1 CANCELLED")
        assert s1.result is None and not s1.workdir.exists()
        mgr.cancel(s2.id)
        await wait_for(lambda: s2.status == tm.STATUS_CANCELLED, what="s2 CANCELLED")
    finally:
        os.environ.pop("COG_SLOW", None)

    f1 = mk(mgr, "00.tif")
    await wait_for(lambda: f1.status == tm.STATUS_DONE, what="f1 完成")
    await mgr.shutdown()
    assert not mgr._runners, "关闭后不得残留后台协程"
    print("  basic: ok")


async def scenario_shutdown(harness):
    """停机:拒绝新任务、排队不启动、运行中终止、先回收再清理、DONE 保留。"""
    mgr = new_mgr(harness, "tm-shutdown-", max_parallel=2, shutdown_timeout=10)

    done = mk(mgr, "00.tif")
    await wait_for(lambda: done.status == tm.STATUS_DONE, what="done 完成")

    os.environ["COG_SLOW"] = "1"
    try:
        a = mk(mgr, "00.tif")
        b = mk(mgr, "00.tif")
        await wait_for(
            lambda: a.status == tm.STATUS_RUNNING and b.status == tm.STATUS_RUNNING,
            what="a/b 进入 RUNNING",
        )
        queued = [mk(mgr, "00.tif"), mk(mgr, "00.tif")]
        assert all(t.status == tm.STATUS_QUEUED for t in queued)

        await mgr.shutdown()
    finally:
        os.environ.pop("COG_SLOW", None)

    for t in (a, b, *queued):
        assert t.status == tm.STATUS_CANCELLED, (t.id, t.status, t.error)
        assert t.result is None, "停机取消的任务不得发布结果"
        assert t.stage is None, "终态任务不应保留阶段"
        assert not t.workdir.exists(), f"未完成任务目录未清理: {t.workdir}"
    assert done.workdir.exists() and (done.workdir / "result.tif").is_file(), \
        "已完成结果在本次关闭中应保留"
    assert not mgr._runners and not mgr._run_tasks
    assert mgr.closing

    # 关闭后拒绝新任务/新入队
    try:
        mgr.create(["late.tif"], 2.0)
    except tm.ShuttingDown:
        pass
    else:
        raise AssertionError("关闭后 create 应被拒绝")
    orphan = tm.Task(
        id="orphan", resolution=2.0, filenames=["late.tif"],
        workdir=mgr.tasks_root / "orphan",
    )
    orphan.workdir.mkdir(parents=True)
    assert mgr.enqueue(orphan) is False, "关闭后 enqueue 应被拒绝"
    assert not orphan.workdir.exists(), "被拒绝的入队任务目录应清理"
    print("  shutdown: ok")


async def scenario_late_process(harness):
    """停机与进程创建交错:迟到进程被回收,不启动后续阶段。"""
    mgr = new_mgr(harness, "tm-late-", max_parallel=1, shutdown_timeout=10)
    hooks = Path(tempfile.mkdtemp(prefix="tm-hooks-late-"))
    gate = Path(tempfile.mkdtemp(prefix="tm-gate-")) / "hold"
    gate.write_text("1")
    os.environ["TM_HOOK_DIR"] = str(hooks)
    os.environ["TM_START_GATE"] = str(gate)
    try:
        t = mk(mgr, "00.tif")
        await wait_for(lambda: t.status == tm.STATUS_RUNNING, what="进入 RUNNING")
        await wait_for(
            lambda: any(p.name.startswith("gdalwarp-start-") for p in hooks.iterdir()),
            what="gdalwarp 进程已创建",
        )
        closing = asyncio.create_task(mgr.shutdown())
        await asyncio.sleep(0.1)
        gate.unlink()  # 关闭已开始后才放行迟到进程
        await closing
    finally:
        for k in ("TM_HOOK_DIR", "TM_START_GATE"):
            os.environ.pop(k, None)

    assert t.status == tm.STATUS_CANCELLED, (t.status, t.error)
    assert t.result is None
    assert not t.workdir.exists(), "迟到进程结束后才清理目录"
    assert not any(p.name.startswith("gdal_translate-start-") for p in hooks.iterdir()), \
        "停机后不得启动后续阶段"
    assert not any(p.name.startswith("gdalinfo-start-") for p in hooks.iterdir())
    print("  late-process: ok")


async def scenario_handoff(harness):
    """停机与阶段交接交错:上一阶段刚产出、尚未退出的窗口内关闭,不启动下一阶段。"""
    mgr = new_mgr(harness, "tm-handoff-", max_parallel=1, shutdown_timeout=10)
    hooks = Path(tempfile.mkdtemp(prefix="tm-hooks-handoff-"))
    end_gate = Path(tempfile.mkdtemp(prefix="tm-endgate-")) / "hold"
    end_gate.write_text("1")
    os.environ["TM_HOOK_DIR"] = str(hooks)
    os.environ["TM_END_GATE"] = str(end_gate)
    try:
        t = mk(mgr, "00.tif")
        await wait_for(
            lambda: any(p.name.startswith("gdalwarp-end-") for p in hooks.iterdir()),
            what="gdalwarp 已产出 mosaic.tif",
        )
        # gdalwarp 仍存活,交接窗口内开始关闭
        closing = asyncio.create_task(mgr.shutdown())
        await asyncio.sleep(0.1)
        end_gate.unlink()
        await closing
    finally:
        for k in ("TM_HOOK_DIR", "TM_END_GATE"):
            os.environ.pop(k, None)

    assert t.status == tm.STATUS_CANCELLED, (t.status, t.error)
    assert t.result is None
    assert not any(p.name.startswith("gdal_translate-start-") for p in hooks.iterdir()), \
        "关闭后不得启动 COG 阶段"
    assert not any(p.name.startswith("gdalinfo-start-") for p in hooks.iterdir())
    print("  handoff: ok")


async def scenario_idempotent_and_cancelled_wait(harness):
    """重复关闭幂等;关闭等待被取消时收尾仍完成后再传播取消。"""
    mgr = new_mgr(harness, "tm-idem-", max_parallel=1, shutdown_timeout=15)
    os.environ["TM_EXIT_DELAY"] = "0.8"  # 拉长收尾窗口
    try:
        t = mk(mgr, "00_slow.tif")
        await wait_for(lambda: t.status == tm.STATUS_RUNNING, what="进入 RUNNING")

        waiter = asyncio.create_task(mgr.shutdown())
        wait2 = asyncio.create_task(mgr.shutdown())
        await asyncio.sleep(0.05)
        first = mgr._closing_task
        assert first is not None and not first.done()

        waiter.cancel()
        try:
            await waiter
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("被取消的 shutdown 应传播 CancelledError")
        # 取消不打断收尾:抛出时收尾已经完成
        assert first.done(), "关闭等待被取消后收尾仍未完成"
        assert not t.workdir.exists(), "收尾应先完成清理再传播取消"

        await wait2
        assert mgr._closing_task is first, "重复关闭必须是同一次收尾"
        assert mgr.begin_shutdown() is first
        assert t.status == tm.STATUS_CANCELLED
    finally:
        os.environ.pop("TM_EXIT_DELAY", None)
    print("  idempotent/cancelled-wait: ok")


async def main():
    tmp = Path(tempfile.mkdtemp(prefix="tm-test-"))
    (tmp / "bin").mkdir()
    make_stubs(tmp / "bin")
    os.environ["PATH"] = str(tmp / "bin") + os.pathsep + os.environ["PATH"]

    harness = Harness()
    harness.install()
    try:
        print("scenario basic")
        await scenario_basic(harness)
        print("scenario shutdown")
        await scenario_shutdown(harness)
        print("scenario late process")
        await scenario_late_process(harness)
        print("scenario handoff")
        await scenario_handoff(harness)
        print("scenario idempotent shutdown")
        await scenario_idempotent_and_cancelled_wait(harness)
        print("ALL TASKMANAGER TESTS PASSED")
    finally:
        harness.uninstall()


if __name__ == "__main__":
    asyncio.run(main())
