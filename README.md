# DEM 拼接服务(FastAPI + GDAL)

把相邻架次的高程栅格拼成统一底图:网页上传最多 **4 份带坐标系的单波段 GeoTIFF**,
调整覆盖顺序、设置目标像元边长(米,>0),后台统一重投影到 **EPSG:3857** 后镶嵌,
再转换为**带概览的 COG**。全部成功才开放下载,并显示范围与尺寸。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`samplegen` 服务会先生成演示样例(写入 `./data/samples`),完成后 `web` 自动启动。
浏览器中可直接勾选样例、调整顺序、提交拼接、取消任务、下载结果,无需准备数据。

> 若无法拉取 `ghcr.io/osgeo/gdal`,把 Dockerfile 第一行改为
> `osgeo/gdal:ubuntu-small-3.9.2`(Docker Hub)。

## 演示样例(不同坐标系 / 重叠 / NoData)

| 文件 | CRS | 说明 |
|---|---|---|
| `dem_A_utm32n.tif` | EPSG:32632 | 2m,中部一块 NoData 空洞 |
| `dem_B_utm32n.tif` | EPSG:32632 | 与 A 重叠约 200m×500m,西侧 NoData 条带 |
| `dem_C_wgs84.tif` | EPSG:4326 | 经纬度(度),覆盖 A 西南角,含 NoData 空洞 |
| `dem_D_webmerc.tif` | EPSG:3857 | 与 B 东北角重叠,四周一圈 NoData 边缘 |

建议体验顺序:勾选 A、B、C、D 加入列表 → 像元边长 `2` → 开始拼接。
把 B 调到 A 之下/之上各跑一次,可看到**列表靠后的文件有效像元优先**,
而 NoData 不会盖掉已有高程。重新生成样例:`FORCE=1 docker compose run --rm samplegen`。

## 处理流水线

每个任务三个 GDAL 阶段,全部流式处理,**不把整幅栅格读入内存**:

1. **MOSAIC** — `gdalwarp -t_srs EPSG:3857 -tr <res> <res> -r bilinear -dstnodata -9999`
   多输入一次镶嵌:自动计算并集范围;输入按列表顺序依次写入,**后文件有效像元覆盖先文件**;
   各输入自带的 NoData 被识别为掩膜,不参与写入,因此不会盖掉已有高程。
2. **COG** — `gdal_translate -of COG -co COMPRESS=DEFLATE -co RESAMPLING=AVERAGE`,
   COG 驱动自动生成概览。
3. **METADATA** — `gdalinfo -json` 读取尺寸与地理变换,计算 EPSG:3857 范围(米)。

只有三个阶段全部成功,任务才进入 `DONE` 并开放下载。

## 并发、取消与资源回收

- 全局信号量限制 **2 个任务并行**,其余排队(`QUEUED`);取消/失败会释放槽位。
- 每个 GDAL 阶段是独立子进程,置于**独立进程组**(`start_new_session`)。
  取消时对该进程组发 `SIGKILL` 并 `await` 回收,只影响本任务,不影响另一并行任务。
- **阶段交接安全**:阶段启动前、子进程退出后、发布结果前都检查取消标志——
  取消后绝不启动下一阶段,也绝不发布迟到结果。
- 失败或取消后,先终止并回收本任务全部 GDAL 进程,再删除该任务暂存目录;
  成功任务保留 `result.tif` 供下载。服务重启时清空遗留暂存。

### 停机收尾(服务停止时)

服务收到 SIGTERM(如 `docker compose stop/down`)后按以下顺序收尾,收尾完成才退出:

1. **拒绝新任务**:关闭标志立即置位,`POST /api/tasks` 返回 503,`/readyz` 同样返回 503;
   已通过校验但恰在关闭窗口入队的任务被取消并清理。
2. **停止排队与运行中任务**:所有未完成任务置取消标志;排队任务绝不启动,
   运行中任务的 GDAL 进程组 `SIGKILL`。
3. **等待后台协程并回收进程**:等所有任务协程结束(它们负责 `wait()` 回收子进程);
   超时后继续按周期重发进程组终止,直到全部协程退出、全部子进程被回收。
4. **清理未完成任务目录**:确认没有存活 GDAL 进程后,才删除未完成任务暂存目录;
   **本次关闭中已完成任务的结果保留**。

交错窗口的处理:

- 停机与**进程创建**交错:子进程创建后若发现已进入关闭,立即终止,仍在本阶段内
  `wait()` 回收;不会启动后续阶段。
- 停机与**阶段交接**交错:阶段退出后的检查包含关闭标志,绝不启动下一阶段。
- 停机与**完成发布**交错:发布结果前再检查关闭标志,迟到的成功结果不会发布。
- **重复关闭**幂等:所有调用共享同一次收尾协程。
- **关闭等待被取消**时收尾继续进行,收尾完成后才向上传播取消。
- 清理规则不变:本次关闭保留已完成结果;下次启动时启动清理仍清空全部遗留暂存目录。
- 未改动的部分:上传后的输入准备/校验阶段逻辑不变(仍写任务目录并校验 GeoTIFF)。

`SHUTDOWN_TIMEOUT` 控制等待后台协程自行结束的时限(默认 15s);超时后仍会持续
强制终止进程组直至全部回收。Compose 的 `stop_grace_period` 更大(默认 120s),
确保应用能自行收尾而不是被 docker 强杀。

## 慢 GDAL 停机/交接演示(Docker)

镜像内 GDAL CLI 前面加了一层可控延迟包装(`tools/slow_gdal.py`),
`SLOW_GDAL_FACTOR=<秒>` 时每个阶段在真正读写前先延迟数秒(拉宽"进程已创建、
阶段未完成"的窗口);`SLOW_GDAL_AFTER_FACTOR=<秒>` 让阶段产出结果后延迟退出
(拉宽"阶段交接"窗口)。两者都未设置时透明 `exec` 真实工具,不影响正常使用。
也可用 `SLOW_GDAL_DELAY_GDALWARP` / `SLOW_GDAL_AFTER_DELAY_GDAL_TRANSLATE`
等按工具单独指定。

```bash
./tools/demo_shutdown.sh           # 默认 SLOW_GDAL_FACTOR=8
SLOW_GDAL_FACTOR=15 ./tools/demo_shutdown.sh
```

演示脚本会:提交 2 个并行 + 若干排队任务并让一个快速任务先完成;在运行中
任务卡在 GDAL 阶段时 `docker compose stop`,随后检查容器内**无残留 GDAL 进程**、
**未完成任务目录已清理**、**已完成结果保留**;再在阶段交接窗口内停一次;
最后验证关闭期间新提交返回 503。也可手工按下面步骤观察。

```bash
SLOW_GDAL_FACTOR=8 docker compose -f docker-compose.yml -f docker-compose.demo.yml up --build -d
# 网页提交若干任务,等任务处于 RUNNING/MOSAIC 或 COG 时:
docker compose -f docker-compose.yml -f docker-compose.demo.yml stop web
docker compose -f docker-compose.yml -f docker-compose.demo.yml exec web \
  ps -eo pid,args | grep -E '[g]dal(warp|_translate|info)'  # 应无输出
find data/tasks -mindepth 1 -maxdepth 1 -type d ! -exec test -f '{}/result.tif' ';' -print  # 未完成目录应为空
docker compose -f docker-compose.yml -f docker-compose.demo.yml logs --no-log-prefix web \
  | tail -n 20   # 可见"停机"收尾日志序列
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 网页 UI |
| GET | `/api/samples` | 样例列表 |
| POST | `/api/tasks` | 创建任务(multipart:`resolution`、`plan` JSON、`files`) |
| GET | `/api/tasks` | 任务列表(状态/阶段/结果/日志尾) |
| POST | `/api/tasks/{id}/cancel` | 取消任务 |
| GET | `/api/tasks/{id}/download` | 下载 COG(仅 `DONE`,否则 409) |

`plan` 示例(按覆盖优先级从低到高):
`[{"type":"upload","index":0},{"type":"sample","name":"dem_B_utm32n.tif"}]`

## 目录结构

```
app/main.py          FastAPI 路由(上传校验、任务 API、下载、停机收尾)
app/taskmanager.py   任务调度:并发限制、三阶段流水线、取消与停机收尾(不依赖 FastAPI)
app/static/index.html 单页 UI
samples/generate_samples.py 样例生成(标准库 + gdal_translate)
tools/slow_gdal.py   GDAL CLI 慢速包装器(停机/交接演示用)
tools/demo_shutdown.sh 容器内慢 GDAL 停机演示脚本
tests/test_taskmanager.py  调度与停机收尾测试(GDAL 桩程序,无需真实 GDAL)
data/                运行期数据(样例 + 每任务暂存目录,Compose 自动创建)
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | 数据根目录 |
| `SAMPLES_DIR` | `$DATA_DIR/samples` | 样例目录 |
| `MAX_PARALLEL` | `2` | 并行任务数 |
| `SHUTDOWN_TIMEOUT` | `15` | 停机时等待后台协程自行结束的秒数(之后持续强制回收) |
| `SLOW_GDAL_FACTOR` | `0` | 演示用:每个 GDAL 阶段启动后的基准延迟秒数 |
| `SLOW_GDAL_AFTER_FACTOR` | `0` | 演示用:阶段产出结果后、包装进程退出前的基准延迟秒数 |
| `SLOW_GDAL_DELAY_<TOOL>` | 未设置 | 演示用:单独指定某工具启动延迟(如 `SLOW_GDAL_DELAY_GDALWARP=10`) |
| `SLOW_GDAL_AFTER_DELAY_<TOOL>` | 未设置 | 演示用:单独指定某工具结束延迟 |

注意:任务注册表在进程内,uvicorn 必须单 worker(镜像默认如此)。

## 验证取消时的进程回收(容器内)

```bash
docker compose up --build -d
# 网页提交一个大任务后立即取消,然后:
docker compose exec web ps aux   # 不应残留 gdalwarp/gdal_translate 进程
ls data/tasks/                   # 被取消任务的目录已清理
```

## 本地运行调度测试(无需 GDAL)

```bash
python3 -I tests/test_taskmanager.py
```

覆盖:两任务并行与排队、取消运行中/排队任务(killpg 参数与清理)、COG 阶段取消
不发布结果、槽位释放;停机时拒绝新任务与排队任务、终止运行中任务、进程回收后
才清理未完成目录、已完成结果保留;停机与进程创建/阶段交接交错的迟到进程回收与
后续阶段抑制;重复关闭幂等;关闭等待被取消时先完成收尾再传播取消。
沙箱环境禁止真实信号时,测试用哨兵文件模拟进程组终止;真实 `SIGKILL` 在 Docker
容器中生效。
