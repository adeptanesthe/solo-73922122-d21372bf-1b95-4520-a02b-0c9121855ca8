"""后台拼接任务调度。

流水线:MOSAIC(gdalwarp 重投影+镶嵌)-> COG(gdal_translate 生成带概览的 COG)
-> METADATA(gdalinfo 读取范围/尺寸)。全部成功才发布结果。

设计要点:
- 全局信号量限制并行任务数(默认 2),其余任务排队。
- 每个 GDAL 阶段都是独立子进程,放在自己的进程组里(start_new_session),
  取消时按进程组 SIGKILL 并 await 回收,不影响其他任务的进程。
- 阶段启动前、子进程退出后、发布结果前都检查取消/停机标志:
  取消或停机后绝不启动下一阶段,也绝不发布迟到结果。
- 失败/取消后清理该任务的暂存目录;完成的任务保留结果供下载。
- 停机收尾是有序的:先拒绝新任务,再取消排队/运行中的任务并终止进程组,
  等全部后台协程结束、全部 GDAL 子进程退出并回收后,才清理未完成任务目录;
  已完成任务的结果保留。重复关闭复用同一次收尾,关闭等待被取消时也先收尾完成。
- 像素数据全程由 GDAL 流式处理,不把整幅栅格读入内存。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_CANCELLED = "CANCELLED"
TERMINAL_STATUSES = {STATUS_DONE, STATUS_FAILED, STATUS_CANCELLED}

STAGE_MOSAIC = "MOSAIC"
STAGE_COG = "COG"
STAGE_METADATA = "METADATA"

TARGET_CRS = "EPSG:3857"
DST_NODATA = "-9999"

MAX_TASKS_KEPT = 200  # 内存中保留的最大任务记录数,超出时回收最旧的终态任务
DEFAULT_SHUTDOWN_TIMEOUT = 15.0  # 停机时等待后台协程自行结束的时限(秒)


class StageError(Exception):
    """某个 GDAL 阶段执行失败。"""


class TaskCancelled(Exception):
    """任务被取消(在阶段边界或子进程退出后检测到)。"""


class ShuttingDown(Exception):
    """服务已开始关闭,不再接受新的入队任务。"""


@dataclass
class Task:
    id: str
    resolution: float
    filenames: list  # 原始文件名,按覆盖优先级从低到高排列
    workdir: Path
    created_at: float = field(default_factory=time.time)
    status: str = STATUS_QUEUED
    stage: Optional[str] = None
    error: Optional[str] = None
    log_tail: str = ""
    result: Optional[dict] = None
    cancel_requested: bool = False
    proc: Optional[asyncio.subprocess.Process] = None
    proc_pgid: Optional[int] = None

    @property
    def inputs_dir(self) -> Path:
        return self.workdir / "inputs"

    def public(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "stage": self.stage,
            "error": self.error,
            "resolution": self.resolution,
            "filenames": list(self.filenames),
            "created_at": self.created_at,
            "log_tail": self.log_tail[-400:],
            "result": self.result,
            "download_url": f"/api/tasks/{self.id}/download"
            if self.status == STATUS_DONE
            else None,
        }


class TaskManager:
    def __init__(
        self,
        tasks_root: Path,
        max_parallel: int = 2,
        shutdown_timeout: float = DEFAULT_SHUTDOWN_TIMEOUT,
    ):
        self.tasks_root = Path(tasks_root)
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        self.sem = asyncio.Semaphore(max_parallel)
        self.tasks: dict = {}
        self._runners: set = set()
        self.shutdown_timeout = shutdown_timeout
        self._closing = False
        self._closing_task: Optional[asyncio.Task] = None
        self._run_tasks: dict = {}  # 任务 id -> 后台协程,供停机时逐一追踪

    # ---------- 任务生命周期 ----------

    @property
    def closing(self) -> bool:
        """已开始关闭:不再接受新的入队任务。"""
        return self._closing

    def create(self, filenames, resolution: float) -> Task:
        """建立任务记录与暂存目录(尚未调度)。上传校验失败可调用 discard。"""
        if self._closing:
            raise ShuttingDown("服务正在关闭,不再接受新任务")
        self._gc()
        tid = uuid.uuid4().hex[:12]
        workdir = self.tasks_root / tid
        (workdir / "inputs").mkdir(parents=True)
        t = Task(id=tid, resolution=resolution, filenames=list(filenames), workdir=workdir)
        self.tasks[tid] = t
        return t

    def enqueue(self, t: Task) -> bool:
        """入队。已开始关闭时拒绝调度并清理(入队前后都可能进入关闭窗口)。"""
        if self._closing:
            self._finalize(t, STATUS_CANCELLED)
            shutil.rmtree(t.workdir, ignore_errors=True)
            return False
        runner = asyncio.create_task(self._run(t), name=f"task-{t.id}")
        self._runners.add(runner)
        self._run_tasks[t.id] = runner
        runner.add_done_callback(lambda r: self._forget_runner(t.id, r))
        return True

    def _forget_runner(self, task_id: str, runner: asyncio.Task) -> None:
        self._runners.discard(runner)
        if self._run_tasks.get(task_id) is runner:
            self._run_tasks.pop(task_id, None)

    def discard(self, t: Task) -> None:
        """任务尚未入队时放弃(如上传校验失败)。"""
        self.tasks.pop(t.id, None)
        shutil.rmtree(t.workdir, ignore_errors=True)

    def cancel(self, task_id: str) -> Optional[Task]:
        """请求取消:置标志、杀当前 GDAL 进程组;排队任务直接终态并清理。"""
        t = self.tasks.get(task_id)
        if t is None or t.status in TERMINAL_STATUSES:
            return t
        t.cancel_requested = True
        self._kill_proc(t)
        if t.status == STATUS_QUEUED:
            self._finalize(t, STATUS_CANCELLED)
        return t

    def cleanup_stale(self) -> None:
        """启动时清空上次运行遗留的暂存目录(内存注册表已重置)。"""
        for child in self.tasks_root.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # ---------- 停机收尾 ----------

    def begin_shutdown(self, timeout: Optional[float] = None) -> "asyncio.Task":
        """幂等地开始停机收尾,返回同一个收尾协程。

        - 置关闭标志:此后 create/enqueue 一律拒绝,排队任务不再启动。
        - 对所有未完成任务置取消标志并终止其 GDAL 进程组。
        - 等待全部后台协程结束(它们负责 wait() 回收子进程),
          再清理未完成任务目录;已完成(DONE)结果保留。
        """
        if self._closing_task is None:
            self._closing = True
            self._closing_task = asyncio.create_task(
                self._close(self.shutdown_timeout if timeout is None else timeout),
                name="taskmanager-shutdown",
            )
        return self._closing_task

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        """等待收尾完成。重复调用等同一次;本等待被取消时也先收尾再传播取消。"""
        closing = self.begin_shutdown(timeout)
        cancelled = False
        while not closing.done():
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError:
                # 关闭等待被取消:收尾协程继续运行,收尾完成后才向上传播取消
                cancelled = True
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()
            except Exception:  # noqa: BLE001 - 收尾异常在下面统一处理
                break
        if closing.cancelled():
            if cancelled:
                raise asyncio.CancelledError()
            raise RuntimeError("停机收尾协程被意外取消")
        exc = closing.exception()
        if exc is not None:
            logger.error("停机收尾失败", exc_info=exc)
            if cancelled:
                raise asyncio.CancelledError()
            raise exc
        if cancelled:
            raise asyncio.CancelledError()

    async def _close(self, timeout: float) -> None:
        logger.info("停机: 拒绝新任务, 停止排队与运行中的任务")
        for t in list(self.tasks.values()):
            if t.status not in TERMINAL_STATUSES:
                t.cancel_requested = True
        self._kill_all_procs()

        # 等待全部后台协程结束。它们负责 wait() 回收各自的 GDAL 子进程。
        # 超时后继续按秒重发进程组终止并等待,但绝不 cancel 协程:
        # 若恰好在子进程创建窗口取消协程,可能丢掉刚创建进程的句柄。
        runners = [r for r in self._runners if not r.done()]
        if runners:
            logger.info(
                "停机: 等待 %d 个后台协程结束并回收 GDAL 进程(最多 %.1fs)",
                len(runners),
                timeout,
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max(0.0, timeout)
            warned = False
            while runners:
                remaining = deadline - loop.time()
                if remaining > 0:
                    await asyncio.wait(runners, timeout=remaining)
                else:
                    await asyncio.sleep(0)  # 让恰好在此刻结束的协程完成回调
                if not warned and any(not r.done() for r in runners):
                    logger.warning(
                        "停机: 后台协程未在 %.1fs 内结束,持续终止进程组直至全部退出",
                        timeout,
                    )
                    warned = True
                if warned:
                    self._kill_all_procs()
                    await asyncio.sleep(0.2)
                runners = [r for r in self._runners if not r.done()]

        # 后台协程已结束。防御性核查:若仍有存活子进程,先终止并回收。
        while True:
            pending_reap = [
                asyncio.ensure_future(t.proc.wait())
                for t in list(self.tasks.values())
                if t.proc is not None and t.proc.returncode is None
            ]
            if not pending_reap:
                break
            self._kill_all_procs()
            await asyncio.wait(pending_reap)
        # 到此所有已登记子进程都已退出并回收
        for t in list(self.tasks.values()):
            t.proc = None
            t.proc_pgid = None
        logger.info("停机: 全部后台协程结束, GDAL 进程已退出并回收")

        # 只有到此才清理未完成任务目录;已完成结果本次保留。
        removed = self._cleanup_unfinished()
        logger.info(
            "停机: 已清理 %d 个未完成任务目录, 已完成结果保留;收尾完成", removed
        )

    def _cleanup_unfinished(self) -> int:
        removed = 0
        for t in list(self.tasks.values()):
            if t.status == STATUS_DONE:
                continue  # 已完成结果在本次关闭中保留
            if t.workdir.exists():
                shutil.rmtree(t.workdir, ignore_errors=True)
                removed += 1
        return removed

    # ---------- 内部实现 ----------

    def _gc(self) -> None:
        if len(self.tasks) <= MAX_TASKS_KEPT:
            return
        terminal = sorted(
            (t for t in self.tasks.values() if t.status in TERMINAL_STATUSES),
            key=lambda t: t.created_at,
        )
        for t in terminal[: len(self.tasks) - MAX_TASKS_KEPT]:
            self.tasks.pop(t.id, None)
            if t.status != STATUS_DONE:
                shutil.rmtree(t.workdir, ignore_errors=True)

    def _kill_all_procs(self) -> None:
        for t in list(self.tasks.values()):
            self._kill_proc(t)

    def _kill_proc(self, t: Task) -> None:
        proc = t.proc
        if proc is None or proc.returncode is not None:
            return
        try:
            # 子进程在独立会话/进程组中,整组 SIGKILL,回收本任务全部 GDAL 进程
            os.killpg(t.proc_pgid or proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                pass

    async def _reap(self, proc: asyncio.subprocess.Process) -> None:
        """等待子进程被回收;即使当前协程被取消也继续,绝不留下存活进程。"""
        while proc.returncode is None:
            try:
                await asyncio.shield(proc.wait())
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()
            except Exception:  # noqa: BLE001 - 进程已消失等情况
                return

    def _finalize(self, t: Task, status: str, error: Optional[str] = None) -> None:
        if t.status in TERMINAL_STATUSES:
            return  # 已被取消端点等置为终态,不覆盖
        t.status = status
        t.stage = None
        t.error = error
        # 停机期间不在这里删目录:统一等所有进程回收、协程结束后由 _close 清理,
        # 避免删掉迟到 GDAL 进程仍在使用的目录。
        if status in (STATUS_FAILED, STATUS_CANCELLED) and not self._closing:
            shutil.rmtree(t.workdir, ignore_errors=True)

    async def _run(self, t: Task) -> None:
        async with self.sem:
            if (
                self._closing
                or t.cancel_requested
                or t.status in TERMINAL_STATUSES
            ):
                # 排队期间被取消或被停机叫停:绝不启动后续阶段
                self._finalize(t, STATUS_CANCELLED)
                return
            t.status = STATUS_RUNNING
            try:
                await self._mosaic(t)
                await self._cog(t)
                await self._metadata(t)
            except TaskCancelled:
                self._finalize(t, STATUS_CANCELLED)
            except asyncio.CancelledError:
                # 协程被外部取消:先置终态与清理,再传播取消
                self._finalize(t, STATUS_CANCELLED)
                raise
            except StageError as e:
                self._finalize(t, STATUS_FAILED, str(e))
            except Exception as e:  # noqa: BLE001 - 兜底,保证任务进入终态
                self._finalize(t, STATUS_FAILED, f"内部错误: {e!r}")
            else:
                # 发布前再检查:取消/停机后绝不发布迟到结果
                if (
                    self._closing
                    or t.cancel_requested
                    or t.status in TERMINAL_STATUSES
                ):
                    self._finalize(t, STATUS_CANCELLED)
                else:
                    self._finalize(t, STATUS_DONE)

    async def _exec(self, t: Task, stage: str, cmd: list) -> str:
        """执行一个 GDAL 阶段。启动前与退出后都检查取消/停机标志。"""
        if self._closing or t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        t.stage = stage
        t.log_tail = ""
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(t.workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,  # 独立进程组,便于整组终止
            )
        except FileNotFoundError:
            raise StageError(f"找不到 GDAL 工具: {cmd[0]}")
        t.proc = proc
        t.proc_pgid = proc.pid
        # 停机/取消恰好发生在进程创建窗口(迟到的子进程):立即终止。
        # 本次阶段的读取与 wait() 会回收它;阶段结束后的检查保证协同取消时
        # 不进入下一阶段、不发布结果。
        if self._closing or t.cancel_requested:
            self._kill_proc(t)
        chunks: list = []
        rc: Optional[int] = None
        finished = False
        try:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                t.log_tail = (t.log_tail + chunk.decode("utf-8", "replace"))[-800:]
            rc = await proc.wait()  # 回收子进程,避免僵尸
            finished = True
        finally:
            if not finished:
                # 异常路径(如协程被取消):终止并回收,绝不留下存活进程
                self._kill_proc(t)
                await self._reap(proc)
            t.proc = None
            t.proc_pgid = None
        if self._closing or t.cancel_requested or t.status in TERMINAL_STATUSES:
            raise TaskCancelled()
        if rc != 0:
            raise StageError(f"{stage} 阶段失败 (exit {rc}): {t.log_tail[-300:]}")
        return b"".join(chunks).decode("utf-8", "replace")

    async def _mosaic(self, t: Task) -> None:
        inputs = sorted(p for p in t.inputs_dir.iterdir() if p.is_file())
        if not inputs:
            raise StageError("没有输入文件")
        res = f"{t.resolution:g}"
        # 输入按优先级从低到高排列:gdalwarp 依次写入,后文件的有效像元覆盖先文件;
        # 各输入自带的 NoData 会被 gdalwarp 识别为掩膜,不会盖掉已有高程。
        cmd = [
            "gdalwarp",
            "-t_srs", TARGET_CRS,
            "-tr", res, res,
            "-r", "bilinear",
            "-dstnodata", DST_NODATA,
            "-ot", "Float32",
            "-of", "GTiff",
            "-co", "TILED=YES",
            "-co", "COMPRESS=DEFLATE",
            "-wm", "256",
            "--config", "GDAL_CACHEMAX", "256",
            "-multi",
            "-wo", "NUM_THREADS=2",
        ]
        cmd += [str(p) for p in inputs]
        cmd.append(str(t.workdir / "mosaic.tif"))
        await self._exec(t, STAGE_MOSAIC, cmd)

    async def _cog(self, t: Task) -> None:
        cmd = [
            "gdal_translate",
            "-of", "COG",
            "-co", "COMPRESS=DEFLATE",
            "-co", "RESAMPLING=AVERAGE",  # 概览重采样
            "-co", "NUM_THREADS=2",
            "mosaic.tif",
            "result.tif",
        ]
        await self._exec(t, STAGE_COG, cmd)

    async def _metadata(self, t: Task) -> None:
        out = await self._exec(t, STAGE_METADATA, ["gdalinfo", "-json", "result.tif"])
        try:
            info = json.loads(out)
            w, h = info["size"]
            gt = info["geoTransform"]
        except (ValueError, KeyError) as e:
            raise StageError(f"解析 gdalinfo 输出失败: {e}")
        xs = [gt[0], gt[0] + w * gt[1]]
        ys = [gt[3], gt[3] + h * gt[5]]
        t.result = {
            "width": w,
            "height": h,
            "extent": [min(xs), min(ys), max(xs), max(ys)],  # EPSG:3857 米
            "crs": TARGET_CRS,
            "file_size_bytes": (t.workdir / "result.tif").stat().st_size,
        }
