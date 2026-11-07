# R7 动态候选持久登记、取消与崩溃回收

本轮基于 [Draft #97](https://github.com/AstroVela/ducklake/pull/97) 的 `01ad28e868c9411689bfe50f4a1a7ec7d04f2744`，在[受控 LIRE](native-vortex-spfresh-lire.md)之后补齐本地动态候选的生命周期组件。统一入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

本轮在实际 SPFresh／RocksDB 文件上实现持久候选登记、源 checkpoint 引用、继承式进程锁、封存／发布／取消与分批 GC。业务仍由 DuckLake 原生受管事务提交数据和回执，显式 flush 后固定实际 Vortex 文件、ReadView snapshot 与 RowRef。生产 SQL／optimizer 仍使用静态 provider；本地组件 registry 尚未接入 DuckLake 元数据事务。

## 单一提交根与候选绑定

[Registry／Session 实现](../test/vane/vortex_spfresh_candidates.py)采用全新的私有 POSIX 目录，`state.json` 是唯一的原子提交根。它同时保存 head、候选状态与源引用；一次发布在同一份 JSON 提交里把旧 generation 退休、把新候选发布并释放它对构建源的引用，不存在两个独立指针的提交间隙。

每个候选绑定独立 candidate／owner UUID、表 UUID、schema／向量与业务字段身份、维度、源 generation／seal／snapshot／capture hash、目标 snapshot／capture hash，以及 expected publication revision。相同业务 ID 不会被推断为同一个 native VID。正常调用核对确切绑定，旧 revision、错误 owner、取消后的 worker 或已经回收的 handle 均明确拒绝。

| 状态 | 允许的推进与所有权 |
| --- | --- |
| `building` | 先持久登记，再创建候选数据；可执行有预算的原生任务，完成关闭／封存后转 `sealed` |
| `sealed` | 文件和 capture 与登记一致、原生句柄已关闭；只能以原 expected revision 显式发布，或取消 |
| `published` | 当前不可变 head；发布新版本时原子转 `retired`，GC 不能删除当前 head |
| `retired` | 没有构建源引用且没有活跃 owner 时，可显式进入 GC |
| `cancelled` | 不再允许原生推进或发布；仍保留构建源引用，直到候选完整回收 |
| `deleting` | 仅按持久删除清单续收；文件／目录／lease 清理后删除登记，释放源引用与 slot |

registry 最多 **16 个登记**，包括当前 head、退休、构建和清理中的版本；完整 registry 最多 **1 MiB**，达到上限直接拒绝。该限制属于新的本地实验组件，没有扩大生产请求／generation 预算。格式仅接受本轮严格布局；没有旧 `head.json` 的迁移或兼容解码。

写入先创建并 fsync `state.pending`，再原子 rename 到提交根并 fsync 目录。崩溃留下 pending 时，读者仍只读已提交根；后续写入拒绝。必须显式调用 `recover_metadata` 丢弃未提交 pending，不采用其中的状态或自动重试。

## 原生子进程与旧视图

候选在注册后持有 sidecar 上的独占 `flock`，持久记录其 inode。启动实际 SPFresh 进程时通过 `pass_fds` 继承同一个 open file description。父进程关闭或被杀不会释放仍被原生子进程持有的锁；恢复和 GC 使用新的文件描述符尝试非阻塞加锁，以内核锁作为活跃性依据，不用 PID、TTL、超时或进程名推断所有权。

登记、lease 建立、数据复制和原生启动依次完成。候选数据不会先于持久 intent 出现，原生进程也不会先于持久 lease 绑定启动。取消与正常操作经 registry 锁串行化；即使候选已被取消，只要原生子进程仍持锁，GC 仍返回 `live-owner`。清理中的候选继续 pin 构建源，避免取消后过早删除仍可能被复制或使用的源版本。

ReadView 在提交根锁下校验并复制完整不可变 checkpoint，再打开自己的只读原生副本。旧 canonical generation 与候选回收后，已打开视图继续查询其确切 snapshot。这里仍有 O(checkpoint 大小) 的复制 I/O，未证明生产低写放大或后台并发 serving。

## 显式恢复与有预算 GC

恢复不会接管原生任务。`building`／`sealed` 的 owner 锁仍活跃时暂缓；锁已释放则显式标记 `cancelled`，即使磁盘已经存在完整 seal 也不自动发布。发布提交后响应丢失时，重新读提交根确认 `published`，不会重建或回滚已提交版本。取消确认丢失也保留持久 `cancelled` 状态。

GC 每次最多 **16 项／64 MiB**。本组件采用更细的计费：每个文件、目录、lease 和最后的登记删除各计一项，文件按声明字节计费，包含 seal；目录、lease 和登记按零字节计费。这比上一轮一个完整 canonical checkpoint 计一个逻辑对象更保守，生产 maintenance／GC 的单位和预算没有改变。

1. 核对非当前 head、无构建源引用、无活跃 owner；拒绝 symlink、非受管文件和不匹配的 inode／内容。实验清单最多 4,096 个文件／目录节点，并受 1 MiB 元数据限制。
2. 记录文件／目录身份、大小和文件 SHA，先持久提交本批 `pending` 删除 intent，再逐项 unlink／rmdir 并 fsync 父目录。
3. 提交本批进度；未完成时保留剩余清单和源引用。崩溃重开后，只有已记录在 pending 的缺失项可以继续。
4. 对重入时已经缺失的计划项仍保守计原对象／字节；更小的本次预算不能被旧 intent 扩大。预算不足则明确暂缓，不隐式提高预算或递归删除未知数据。

计费约束删除项及声明文件字节，**不涵盖清单检查读取、JSON／fsync I/O、RocksDB 写放大或总磁盘／RSS**。本轮没有验证任意损坏文件系统的自动修复。既有 100,000 物理行、source／vector／artifact 各 64 MiB、manifest 1 MiB、mutation 1,000 操作／4 MiB、LIRE 每次 16 次嵌套操作／64 MiB posting 写入等限制保持。

## 确定性验证

[协调器](../test/vane/test_vane_vortex_spfresh_candidates.py)使用实际 DuckLake 事务、请求回执、flush、Vortex 与捕获的 ReadView；[独立事件模型](../test/vane/vortex_spfresh_candidate_oracle.py)不导入 registry，实现对提交 head、候选状态、目标绑定、源引用和物理删除的独立核对。原生查询沿用历史 SQL、实际 Vortex、IEEE／typed 行、距离／排序／recall 检查，并独立解码最终原生 posting／version／head。

正式专项通过 **315 次实际 ANN 查询比较**（含 135 次分区边界查询），最低 recall **1.0**；**13 个实际 SIGKILL 边界、83 次 GC 调用、20 项明确拒绝、4 项故意错误 registry 结果**均符合预期。

| 场景 | 结果 |
| --- | --- |
| 两个候选竞争同一 revision | 一个发布，另一个不能继续原生推进；取消后仍 pin 源，清理完成才允许源 GC |
| 16 个登记满额 | 新登记拒绝；显式恢复／回收空 intent 后释放 slot，旧 handle 仍拒绝，新 UUID 不复用 |
| 仅父进程被杀 | 先在真实 split 中暂停原生子进程，再杀父进程；恢复报告 busy，取消后 GC 仍暂缓；明确终止子进程后才能回收 |
| 登记／复制／增量／split 中崩溃 | durable intent 可发现，原 head 不变；未封存版本不能被发布，显式取消后受预算清理 |
| seal 文件／登记／发布边界 | seal 存在仍不自动采用；发布前 pending 丢弃后保留旧 head，发布提交后的未知响应确认新 head |
| GC unlink／进度提交前崩溃 | 两次都以 1 项预算续收；物理删除为零的重入调用仍按原计划文件大小记费 |
| 旧视图 | 初始及 LIRE 前视图在 canonical GC、取消与崩溃后继续查询原 snapshot；最终版本重开并在 GC 后查询 |
| 最终原生状态 | 66 个 head 槽、65 个活跃 head、1,856 个 live VID，750 个 live VID 有 version 增量；全量最近分区检查通过 |

13 个 SIGKILL 点为：登记提交、lease 提交、复制后、原生增量后、取消提交、真实 split 后、seal 文件完成、seal 登记提交、发布 rename 前、发布提交后、仅父进程死亡、GC unlink 后、GC 进度 rename 前。测试用 subreaper 仅回收自己的进程组，并在结束后恢复设置；这些 PID 只用于确定性测试控制，不是协议的所有权依据。

20 项拒绝覆盖 UUID／字段／snapshot／revision、布尔 revision、超大 capture、未封存发布、过期 worker、错误 owner、取消 worker、旧格式、路径越界、当前引用、四种 GC 预算、已回收 handle、满 registry 及 pending 元数据。四项 oracle 控制分别篡改 head、水位、状态与源所有权，均被独立事件模型拒绝。

GC 共记 **305 项／5,414,718 字节**，单次实测最大 **16 项／686,013 字节**；这些仅是本 fixture 的观测，不是大型容量或性能资格。本地完整 fixture `/dev/shm/dvcandidate-final-20261107`，持久摘要 `/tmp/ducklake-r7-candidates-proof-20261107/report.json`，SHA `76a2bbcc1c035592c76c6dcec3112910ee9ff492a65fd02b4dd165308f1812d6`。

本轮只改测试 Python 与 CI／文档，复用 #97 从干净固定源码构建的探针，SHA `7dc36b75722e88e7903c2780f538d9042b300d98072b30f3f1f81f118bd3ec02`。专项检查二进制、原构建输入及当前 checkout 对应 C++／patch 字节；生产 `src/`、C++／Rust／SDK pins、已安装 Vane／DuckLake／Vortex 构件保持，不需要无关 native 重建。CPU 6–11、13 项继承线程环境、原生单线程与 DuckDB threads=2 保持。

同一新 checkout 重跑 #97 通过 **252 次 ANN 查询、388 次 LIRE drain、10 项拒绝、3 个实际 SIGKILL**，merge 同输入控制仍失败而修正版通过；#96 通过 **156 次查询、23 项拒绝、6 个实际 SIGKILL**，两项 fixture 最低 recall 均为 1.0。

Vane 基础 launcher 分非 Ray／Ray 进程通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**，日志 `/tmp/ducklake-r7-candidates-base-20261107.log`。`checkout-audit.json` 核对新专项 24 份已加载源码、两份旧专项源码、每份 SDK 的全部 342 个文件、原生构建／运行库及三个已安装构件。format-check、Python AST、workflow YAML／shell、文档链接、copyleft 和 diff 检查通过；elapsed time 只作观测，没有延迟／吞吐通过条件。

## 复现与后续

先按[上一轮构建命令](native-vortex-spfresh-lire.md#复现)得到固定的干净原生探针及 `build.json`，然后执行：

```bash
VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_spfresh_candidates.py \
  --probe /verified/lire-build/build/ducklake_spfresh_update_probe \
  --build-report /verified/lire-build/build.json \
  --cpus 6,7,8,9,10,11 --output-dir /new/private/candidate-proof
```

CI 复用已构建的原生探针，增加此专项并保存完整候选证据，不增加原生线程或性能门槛。CI 使用 runner 自身 CPU 集合，不能视为重测了本地 CPU 6–11。

下一项推荐 R7 最小原生动态 provider 与 DuckLake 元数据原子覆盖水位接线：以固定 ReadView 和显式提交增量游标驱动，复用本轮所有权与候选状态契约，继续验证失败、取消、旧视图和 GC。当前 Python registry 不是生产元数据接口，不能据此标记生产在线更新已完成。

R6 继续暂缓；后台并发、维度／过滤／质量矩阵、断电恢复、长期容量与收益仍待独立验证，依赖稳定负载的 A/A、性能 A/B 和长期稳态继续暂缓。
