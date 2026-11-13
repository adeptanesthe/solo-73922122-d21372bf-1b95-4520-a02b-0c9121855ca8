"""DEM 拼接 Web 服务:上传/样例 -> 排队 -> gdalwarp 镶嵌 -> COG -> 下载。"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from taskmanager import STATUS_DONE, ShuttingDown, Task, TaskManager

logger = logging.getLogger("uvicorn.error")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR.parent / "data"))
SAMPLES_DIR = Path(os.environ.get("SAMPLES_DIR", DATA_DIR / "samples"))
TASKS_ROOT = DATA_DIR / "tasks"
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "2"))
SHUTDOWN_TIMEOUT = float(os.environ.get("SHUTDOWN_TIMEOUT", "15"))
MAX_INPUTS = 4
SAMPLE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+\.tiff?$", re.IGNORECASE)

manager: TaskManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    TASKS_ROOT.mkdir(parents=True, exist_ok=True)
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    manager = TaskManager(
        TASKS_ROOT, max_parallel=MAX_PARALLEL, shutdown_timeout=SHUTDOWN_TIMEOUT
    )
    # 启动清理沿用原规则:清空上次运行遗留的暂存目录
    manager.cleanup_stale()
    try:
        yield
    finally:
        # 停机收尾:拒绝新任务 -> 停止排队/运行中任务 -> 回收全部 GDAL 进程
        # -> 等待后台协程结束 -> 清理未完成任务目录(已完成结果本次保留)。
        await manager.shutdown()
        logger.info("服务关闭收尾完成")


app = FastAPI(title="DEM 拼接服务", lifespan=lifespan)


# ---------- 页面 ----------

@app.get("/", response_class=HTMLResponse)
async def index():
    return (BASE_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/readyz")
async def readyz():
    """关闭开始后返回 503,便于停机期间拒绝新流量。"""
    if manager.closing:
        raise HTTPException(503, "服务正在关闭")
    return {"ok": True}


# ---------- 样例 ----------

def _load_manifest() -> dict:
    p = SAMPLES_DIR / "manifest.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


@app.get("/api/samples")
async def list_samples():
    manifest = _load_manifest()
    out = []
    if SAMPLES_DIR.is_dir():
        for p in sorted(SAMPLES_DIR.glob("*.tif")):
            out.append(
                {
                    "name": p.name,
                    "size": p.stat().st_size,
                    "description": manifest.get(p.name, ""),
                }
            )
    return out


# ---------- 任务 ----------

@app.get("/api/tasks")
async def list_tasks():
    return [
        t.public()
        for t in sorted(manager.tasks.values(), key=lambda x: x.created_at, reverse=True)
    ]


@app.post("/api/tasks")
async def create_task(
    resolution: float = Form(...),
    plan: str = Form(...),
    files: Optional[List[UploadFile]] = File(default=None),
):
    """plan 为 JSON 数组,按覆盖优先级从低到高排列:
    [{"type":"upload","index":0}, {"type":"sample","name":"dem_A_utm32n.tif"}]
    """
    if manager.closing:
        raise HTTPException(503, "服务正在关闭,不再接受新任务")
    try:
        task = await _prepare_task(resolution, plan, files)
    except ShuttingDown:
        # 校验期间进入关闭窗口,create 拒绝建立任务
        raise HTTPException(503, "服务正在关闭,不再接受新任务")
    try:
        queued = manager.enqueue(task)
    except ShuttingDown:
        queued = False
    if not queued:
        # 关闭窗口内被拒绝:入队未发生,清掉刚建好的暂存目录
        manager.discard(task)
        raise HTTPException(503, "服务正在关闭,不再接受新任务")
    return task.public()


async def _prepare_task(resolution: float, plan: str, files) -> Task:
    """校验 plan,写入并校验输入(不调度);失败时自行清理。"""
    if not (0 < resolution <= 10000):
        raise HTTPException(400, "目标像元边长必须为大于 0 的数(米)")
    try:
        plan_items = json.loads(plan)
    except json.JSONDecodeError:
        raise HTTPException(400, "plan 不是合法 JSON")
    if not isinstance(plan_items, list) or not (1 <= len(plan_items) <= MAX_INPUTS):
        raise HTTPException(400, f"文件数量须为 1~{MAX_INPUTS}")

    uploads = list(files or [])
    resolved = []  # (kind, ref, display_name)
    for item in plan_items:
        if not isinstance(item, dict):
            raise HTTPException(400, "plan 元素格式错误")
        kind = item.get("type")
        if kind == "upload":
            idx = item.get("index")
            if not isinstance(idx, int) or not (0 <= idx < len(uploads)):
                raise HTTPException(400, "plan 中的 upload 索引越界")
            up = uploads[idx]
            resolved.append(("upload", up, up.filename or "upload.tif"))
        elif kind == "sample":
            path = _sample_path(item.get("name") or "")
            resolved.append(("sample", path, path.name))
        else:
            raise HTTPException(400, f"未知 plan 类型: {kind}")

    task = manager.create([d for _, _, d in resolved], resolution)
    try:
        for pos, (kind, ref, display) in enumerate(resolved):
            dst = task.inputs_dir / f"{pos:02d}.tif"
            if kind == "upload":
                await _save_upload(ref, dst)
            else:
                shutil.copyfile(ref, dst)
            await _validate_geotiff(dst, display)
    except HTTPException:
        manager.discard(task)
        raise
    except Exception as e:
        manager.discard(task)
        if manager.closing:
            # 收尾进行中目录被清理,或输入准备被停机打断:按关闭处理
            raise HTTPException(503, "服务正在关闭,不再接受新任务")
        raise HTTPException(400, f"保存输入失败: {e}")
    return task


@app.post("/api/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    t = manager.cancel(task_id)
    if t is None:
        raise HTTPException(404, "任务不存在")
    return t.public()


@app.get("/api/tasks/{task_id}/download")
async def download(task_id: str):
    t = manager.tasks.get(task_id)
    if t is None:
        raise HTTPException(404, "任务不存在")
    if t.status != STATUS_DONE:
        raise HTTPException(409, "任务未成功完成,不能下载")
    path = t.workdir / "result.tif"
    if not path.is_file():
        raise HTTPException(410, "结果文件已不存在")
    return FileResponse(path, media_type="image/tiff", filename=f"mosaic_{t.id}.tif")


# ---------- 工具 ----------

async def _save_upload(up: UploadFile, dst: Path) -> None:
    """分块写盘,不把整个文件读入内存。"""
    with open(dst, "wb") as f:
        while True:
            chunk = await up.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)


def _sample_path(name: str) -> Path:
    if not SAMPLE_NAME_RE.match(name):
        raise HTTPException(400, f"非法样例名: {name}")
    p = SAMPLES_DIR / name
    if not p.is_file():
        raise HTTPException(404, f"样例不存在: {name}")
    return p


async def _validate_geotiff(path: Path, display: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "gdalinfo", "-json", str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        err_msg = err.decode("utf-8", "replace")[-200:]
        raise HTTPException(400, f"文件 {display}: gdalinfo 无法读取 ({err_msg})")
    try:
        info = json.loads(out)
    except ValueError:
        raise HTTPException(400, f"文件 {display}: gdalinfo 输出无法解析")
    bands = info.get("bands") or []
    if len(bands) != 1:
        raise HTTPException(400, f"文件 {display}: 需要单波段 GeoTIFF,实际 {len(bands)} 波段")
    if not ((info.get("coordinateSystem") or {}).get("wkt")):
        raise HTTPException(400, f"文件 {display}: 缺少坐标系(CRS)定义")
