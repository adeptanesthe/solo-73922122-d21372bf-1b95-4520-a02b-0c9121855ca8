# GDAL 官方镜像(含 gdalwarp / gdal_translate / gdalinfo CLI 与 python3)
# 若无法访问 ghcr.io,可改为 Docker Hub 的 osgeo/gdal:ubuntu-small-3.9.2
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.2

ENV PYTHONUNBUFFERED=1 \
    PIP_BREAK_SYSTEM_PACKAGES=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
      "fastapi>=0.110" \
      "uvicorn[standard]>=0.29" \
      "python-multipart>=0.0.9"

# 慢 GDAL 演示包装器:按环境变量 SLOW_GDAL_FACTOR / SLOW_GDAL_DELAY_<TOOL>
# 在真实工具前加入可控延迟;未设置时透明转发,不影响正常运行。
COPY tools/ /opt/demo/
RUN set -eu; \
    for t in gdalwarp gdal_translate gdalinfo; do \
      real="$(command -v "$t")"; \
      mv "$real" "${real}.real"; \
      printf '#!/bin/sh\nexec python3 /opt/demo/slow_gdal.py %s %s "$@"\n' \
        "$t" "${real}.real" > "$real"; \
      chmod 0755 "$real"; \
    done

WORKDIR /app
COPY app/ /app/
COPY samples/ /samples/

RUN chmod 0755 /opt/demo/demo_shutdown.sh

ENV DATA_DIR=/data \
    SAMPLES_DIR=/data/samples \
    MAX_PARALLEL=2 \
    SHUTDOWN_TIMEOUT=15

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
