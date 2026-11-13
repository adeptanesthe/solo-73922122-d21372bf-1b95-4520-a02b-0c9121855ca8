#!/usr/bin/env python3
"""GDAL CLI 慢速包装器:让停机/取消/交接演示可控、可复现。

镜像构建时 gdalwarp / gdal_translate / gdalinfo 会被重命名为 *.real,
原路径放一个调用本脚本的 shim:

    python3 /opt/demo/slow_gdal.py <tool> <real 可执行文件> <原始参数...>

两段可选延迟,均由环境变量控制(秒,可带小数):

启动延迟(进程已创建、尚未真正读写数据,用于制造"迟到进程"窗口):
    SLOW_GDAL_FACTOR                 默认值,按工具名略有差异(确定性)
    SLOW_GDAL_DELAY_<TOOL>           仅该工具,如 SLOW_GDAL_DELAY_GDALWARP

结束延迟(真实工具已产出结果、包装进程尚未退出,用于制造"阶段交接"窗口):
    SLOW_GDAL_AFTER_FACTOR           默认值
    SLOW_GDAL_AFTER_DELAY_<TOOL>     仅该工具,如 SLOW_GDAL_AFTER_DELAY_GDALWARP

未设置或为 0 时直接 exec 真实程序,对正常使用没有影响。延迟期间包装进程
与真实的 GDAL 子进程处于同一进程组,停机/取消时的进程组 SIGKILL 会一起回收。
"""
import hashlib
import os
import subprocess
import sys
import time

KNOWN_TOOLS = ("gdalwarp", "gdal_translate", "gdalinfo")


def _parse(raw) -> float:
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


def _seconds(tool: str, specific_prefix: str, factor_name: str, jitter: bool) -> float:
    specific = os.environ.get(specific_prefix + tool.upper())
    if specific is not None:
        return _parse(specific)
    base = _parse(os.environ.get(factor_name, "0"))
    if base <= 0 or not jitter:
        return base
    # 因子模式下各工具时延略有差异(确定性),便于观察阶段顺序
    digest = hashlib.sha256(tool.encode()).digest()
    return base * (1.0 + digest[0] / 255.0)


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] not in KNOWN_TOOLS:
        print("用法: slow_gdal.py <tool> <real> <args...>", file=sys.stderr)
        return 2
    tool, real, args = sys.argv[1], sys.argv[2], sys.argv[3:]

    start_delay = _seconds(tool, "SLOW_GDAL_DELAY_", "SLOW_GDAL_FACTOR", jitter=True)
    after_delay = _seconds(
        tool, "SLOW_GDAL_AFTER_DELAY_", "SLOW_GDAL_AFTER_FACTOR", jitter=False
    )

    if start_delay > 0:
        print(
            f"[slow-gdal] {tool} pid={os.getpid()} 启动延迟 {start_delay:.2f}s",
            file=sys.stderr,
            flush=True,
        )
        time.sleep(start_delay)

    if after_delay <= 0:
        os.execv(real, [tool] + args)  # 正常运行:透明转发,不额外占用进程
        return 0

    rc = subprocess.run([real] + args).returncode
    print(
        f"[slow-gdal] {tool} pid={os.getpid()} 已产出结果, 结束延迟 {after_delay:.2f}s",
        file=sys.stderr,
        flush=True,
    )
    time.sleep(after_delay)
    return rc


if __name__ == "__main__":
    sys.exit(main())
