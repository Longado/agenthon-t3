<h1 align="center">agenthon-t3</h1>
<p align="center"><strong>用 Rust 重写的 ABIDES 市场模拟器，逐个事件写出和原版相同的记录。</strong></p>
<p align="center">为 Agenthon 2026 第 3 赛道（模拟提速）用四天做成。核验后的决赛排名为 55 队第 10。</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue" alt="MIT"></a>
  <img src="https://img.shields.io/badge/tested%20on-macOS%20%7C%20Linux%20x86--64-black" alt="tested on macOS and Linux x86-64">
  <img src="https://img.shields.io/badge/Rust-1.98-orange" alt="Rust 1.98">
  <img src="https://img.shields.io/badge/status-Alpha-orange" alt="Alpha">
</p>

<p align="center">
  <a href="README.md">English</a> ·
  <a href="#快速开始">快速开始</a> ·
  <a href="#一起做">一起做</a>
</p>

同一个公开场景、同一台 Mac，本仓库里的 Python 版要 1.6 秒，Rust 引擎约 15 毫秒，写出的每一行都相同。

**和 Python 版 ABIDES 逐行相同 · 不联网 · Python 只做兜底**

## 是什么

[ABIDES](https://github.com/abides-sim/abides) 是一个基于智能体的限价订单簿市场模拟器。Agenthon 2026 第 3 赛道要求做更快的模拟器，输出必须和官方 ABIDES 分支完全一致。事件数取决于每一次随机抽样，所以更快的引擎必须按 Python 代码的顺序，重放 numpy `RandomState` 的每一次抽样。

本仓库包括：引擎本身（`native/`，一个 `t3sim` 程序，有 `simulate` 和 `simulate-batch` 两个命令）；它复刻的 ABIDES（`abides/`，固定在 `f9cbe51`，加官方第 3 赛道的四个补丁）；Python 适配层（`abides_fork/`）；以及核对和计时用的工具（`bench/`）。

> **这是一份参赛作品，不是通用库。** 引擎覆盖公开场景里的四种智能体和下单路径，其他情况交给 Python 适配层，设了 `T3SIM_NO_FALLBACK=1` 时直接拒绝。已核对：65 个公开单场景的 `trace.parquet` 和 `message_trace.parquet` 内容与 Python 版相同（本次发布前重跑过），开发期间另用 `bench/fuzz.py` 对比过随机变体。未核对：公开参数范围以外的场景。见[已知问题](#已知问题)。

## 成绩

| | 分数 | 名次 |
|---|---|---|
| 决赛，核验后（隐藏单元） | 2,581,199 | 55 队第 10 |
| 开发榜，10 月 12 日 | 210,604 | 56 队第 10 |

分数是各单元在 Docker 容器运行窗口内每秒事件数的平均值，每个单元最多计 1,000 万。决赛第 1 名是 3,349,638。排行榜见 [agenthon.net](https://www.agenthon.net/#leaderboard)。

`t3sim` 跑一次的输出（Apple M5）：

```text
$ t3sim simulate --config native/pgo/t3-as06-throughput-fast/scenario.json --out out/as06/trace.parquet
{"events_per_sec":4703916.3,"n_events":74502,"n_messages":83337,"seed":1040981657,
 "trace_sha256":"247b65e7fe46a98dbeb2d05c298ad1ce9c855a1d5b1592e377374c35b2381dfd","wall_clock_sec":0.0158, ...}
```

## 怎么工作

| 你 | 它 |
|---|---|
| 运行 `t3sim simulate --config <scenario.json> --out <目录>/trace.parquet` | 在 `<目录>` 写出 `trace.parquet`、`message_trace.parquet` 和 `events.json`（事件数、每个文件的 SHA-256、耗时） |
| 在场景文件旁放一个 `card.toml`，写 `requires_message_ledger = false` | 不写 `message_trace.parquet` |
| 运行 `t3sim simulate-batch --batch-dir <目录> --out-dir <目录>` | 每个子场景各用一个线程，写出 `batch_events.json` |
| 给它不支持的输入 | 把同一条命令交给 `python -m abides_fork.simulate` 重跑；设了 `T3SIM_NO_FALLBACK=1` 时以退出码 3 结束 |
| 同一个场景跑两次 | 得到逐字节相同的文件 |

开发期间在 x86 云机上量到的提速来源：

| 改动 | 省下的时间 |
|---|---|
| 消息延迟在辅助线程上提前抽好（延迟用独立的 `RandomState`，每条消息恰好抽一次） | 大单元 21–24% |
| 4 叉事件队列选子节点改用 `select_unpredictable`，不走分支跳转 | 大单元 11–14%（Intel） |
| 订单簿每一侧在前面留空位，最优价处增删不再整体挪动 | 约 1,000 个智能体的场景 35–37% |
| 忙碌智能体的重排消息排在它最前一条后面 | 一个拥堵的随机场景从 295 秒降到 0.05 秒 |
| 边跑边写输出文件、边算哈希 | 放大 3 倍和 10 倍的单元 7–10% |
| 镜像里用剖析引导编译（PGO） | 2–3% |

试过没用的：基数堆（慢 16–22%）、`x86-64-v3`（约 1%）、价位二分查找（撤单集中在最优价附近时慢 2–5%）。

## 可以试试

| 试试 | 看到什么 |
|---|---|
| 跑 `t3-as06-throughput-fast`（命令见上） | Apple M5 上 74,502 个事件用 15–40 毫秒，每次的 `trace_sha256` 都一样 |
| 跑最大的公开单元 `native/pgo/t3-gb-mega-throughput/scenario.json` | 1,168,360 个事件约 0.14 秒；不写 `message_trace.parquet`，因为它的 card 写了不需要 |
| 用 Python 跑全部公开场景再对比（步骤见下） | 每个场景一行 `OK`，最后是 `65/65 identical` |
| `T3SIM_NO_FALLBACK=1 t3sim simulate --config /dev/null` | 打印 `t3sim: unsupported (missing --config/--out)`，退出码 3 |

## 快速开始

### 方式 A：让你的 AI 帮你装

把下面这段贴进 Claude Code 或 Codex：

```text
先读 https://github.com/Longado/agenthon-t3 的 README.md。
检查是否装了 Rust 1.98 或更新版本（rustc --version）。
克隆仓库，用 cargo build --release --manifest-path native/Cargo.toml 编译。
按 README 跑 t3-as06-throughput-fast 场景，给我看 events.json。
如果我要和 Python 版对比：把 Agenthon-2026/track3-simulation-public 克隆到我的主目录，
建一个 Python 3.11 虚拟环境，装 numpy==1.26.4 pandas==1.5.3 pyarrow==15.0.2 scipy coloredlogs，
再按 README 的对比步骤做。不要全局安装任何东西。
```

### 方式 B：自己装

```sh
git clone https://github.com/Longado/agenthon-t3 && cd agenthon-t3
cargo build --release --manifest-path native/Cargo.toml
native/target/release/t3sim simulate \
  --config native/pgo/t3-as06-throughput-fast/scenario.json --out out/as06/trace.parquet
cat out/as06/events.json
```

在全部公开场景上和 Python 版对比：

```sh
git clone https://github.com/Agenthon-2026/track3-simulation-public ~/track3-simulation-public
python3.11 -m venv .venv && .venv/bin/pip install numpy==1.26.4 pandas==1.5.3 pyarrow==15.0.2 scipy coloredlogs
export PYTHONPATH=$PWD/abides/abides-core:$PWD/abides/abides-markets:$PWD
for f in ~/track3-simulation-public/regression_suite/scenarios/*.json; do
  u=$(basename "$f" .json); [ "$u" = index ] && continue
  .venv/bin/python -m abides_fork.simulate simulate --config "$f" --out "ref/$u/trace.parquet"
done
.venv/bin/python bench/compare.py --bin native/target/release/t3sim --ref ref
```

参赛镜像（`Dockerfile`，只支持 linux/amd64）用剖析引导编译引擎，最后只把静态程序放进空镜像。

## 已知问题

- `bench/*.py` 固定到 `~/track3-simulation-public` 找官方仓库，没有参数可以改路径。
- 引擎写出的 parquet 字节和 pandas 写的不同。内容相同，同一个引擎重复跑字节也相同；比对请用 `bench/compare.py` 比内容，不要比文件哈希。
- 兜底要求 `PATH` 上的 `python` 能导入 ABIDES；否则不支持的场景以退出码 2 失败。
- 批量运行时，子场景数不少于核数时每个只有一个线程，比单独跑同一场景慢约 17%。
- 决赛前在和评分机同代的 Intel 机器上，公开单元按评分口径量到每秒约 517 万事件，决赛分数只有一半。原因不明，决赛日志不公开。

## 一起做

**基于智能体的市场模拟器，能不能既完全精确、又快到能做大规模研究？**

| 你喜欢…… | 可以贡献 |
|---|---|
| Rust 性能 | 事件队列仍占主线程约 30%。mimalloc、把更多与状态无关的随机抽样移出主线程，都还没试。 |
| 市场微观结构 | 移植公开四种智能体以外的智能体和下单路径；现在它们会交给 Python。 |
| 测试 | 把 `bench/fuzz.py` 扩到新的场景形状，继续用 Python 版当标准答案。 |
| 工具 | 给 `bench/` 加一个官方仓库路径参数。 |
| 批量运行 | 一个批次里有几个大子场景时，更好地分配空闲的核。 |

欢迎提 issue 或 pull request。

## 开发

```sh
cargo test --release --manifest-path native/Cargo.toml          # 单元测试
python bench/compare.py --bin native/target/release/t3sim --ref <Python 输出>   # 比内容
python bench/fuzz.py --bin native/target/release/t3sim --n 200 --out <目录>     # 随机变体（要 Docker 和基线镜像）
python bench/bench.py --image <镜像> --out <目录>                               # 按决赛方式计容器时间（要 Docker）
```

`bench/scale.py` 和 `bench/bscale.py` 跑放大后的压力测试。

## 致谢与许可证

本仓库新写的代码用 MIT 许可，© 2026 Yutai Lin（[LICENSE](LICENSE)）。ABIDES 是 BSD-3-Clause，© Georgia Tech Research Corporation（[abides/LICENSE-ABIDES.txt](abides/LICENSE-ABIDES.txt)）。`abides_fork/` 适配层、第 3 赛道补丁，以及 `native/pgo/` 下的场景和任务卡来自 [Agenthon-2026/track3-simulation-public](https://github.com/Agenthon-2026/track3-simulation-public)（MIT）。详见 [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md)。Agenthon 2026 由 Society of Quantitative Analysts 和石溪大学 CEWIT 主办。
