# R7 受控 LIRE split／merge／reassign 的确定性验证

本轮基于 [Draft #96](https://github.com/AstroVela/ducklake/pull/96) 的 `c8cf424834f650080cd1f8ee232b4c3c5445a747`，继续验证本地原生 SPFresh。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)，上一轮[同步原生增删](native-vortex-spfresh-updates.md)的历史结果保留。

本轮运行实际 `Split`、`MergePostings`、`Reassign` 和 RocksDB，检查重平衡后的持久文件、重开与旧 ReadView。生产 SQL／optimizer 继续使用静态 provider；动态元数据发布、后台并发与 R6 不在本轮范围内。

## 业务链路与实验所有权

业务数据继续由现有 DuckLake 受管事务提交数据和回执，再 flush 到实际 Vortex 文件；动态候选只消费真实 native ReadView 捕获的 UUID、字段身份、snapshot 与物理 RowRef。幂等重放返回原回执。原生 VID 单调追加；删除和内部重分配不会把业务 ID 当成物理地址，也不会改变 DuckLake MVCC。

候选从已关闭、封存的 checkpoint 完整复制，在私有目录内增删及重平衡。查询视图持有自己的完整只读副本和固定的 typed 行映射；原 canonical generation 被 GC 后仍查询原 snapshot。整份复制有 O(checkpoint 大小) I/O，不构成低写放大或生产性能收益的证据。

发布仍使用上一轮 fixture 的单一 `head.json`、expected-revision 校验、原子 rename 与目录 fsync。只有队列排空、WAL 同步、head／version labels／posting sizes 保存、句柄关闭并封存后才可发布；这尚未接入 DuckLake 元数据事务。失败或被杀的候选保持未封存，恢复不会自动接管。

## 原生任务与预算

[测试 SDK patch](../test/vane/spfresh_update/native-lire.patch) 在固定 Vortex `static-only.patch` 和上一轮 `native-update.patch` 后应用，不进入生产 provider wheel。SPFresh 固定 `5893eb61ee3b18610b6b00f1939be7dae1af8904`，Vortex 固定 `122b63e0408a187a9d0ecdd129f1cbe12b6b46b2`，链接实际 RocksDB 8.9.1。

新实验模式显式使用 `InPlace=false／Update=false`，由[单线程控制队列](../test/vane/spfresh_update/lire_control.h)调用实际 upstream job 的 `exec`。没有启动原生后台线程池；这证明串行受控重平衡契约，不证明共享可变句柄上的并发查询或 upstream 后台调度正确性。上一轮同步增删模式保留作独立回归，不是失败后的替代路径。

| 项目 | 固定边界 |
| --- | --- |
| 数据与查询 | 8D Float32、平方 L2；64 初始 head／64 search heads、12 posting pages、4 replicas、MaxCheck=4096 |
| 重平衡输入 | 256 个初始键，再分两次各追加 800 行，使用原 12 pages 对应的 1,328-record 分裂阈值；不降低阈值 |
| 线程与 CPU | 本地 CPU 6–11；13 项线程环境保持继承；native build/search 各 1，DuckDB threads=2；RocksDB 后台任务／subcompaction 各 1 |
| 单次显式 drain | 最多 16 次嵌套原生操作、64 MiB 序列化 posting 写入；计数覆盖同步嵌套 Split |
| 嵌套 admission | Reassign 出队前预留 `1＋replicas` 次操作；预算不足则整项留在队列，不先执行到超限 |
| 既有 admission | 100,000 物理行；source／vector／artifact 各 64 MiB；manifest 1 MiB；mutation 1,000 操作／4 MiB |
| canonical GC | 每次最多 16 个逻辑对象／64 MiB，包含 seal；旧视图继续保留自己的副本 |

posting 写入预算在实际 RocksDB Put／Merge／Delete 前计费，**不包含检查读取、RocksDB 自身写放大或 checkpoint I/O**，不能视为总 I/O／磁盘／RSS 上限。读取和完整 artifact 沿用各自原有 admission。超过 posting 字节预算会毒化该私有候选，不能封存或继续；没有自动重试。有限测试控制器显式多次 drain，未扩大每次预算。

## 独立验收与缺陷对照

[物理 oracle](../test/vane/vortex_spfresh_lire_oracle.py) 通过只读 RocksDB iterator 取得原始 key／posting 字节，独立解码 version labels、head 向量、head 删除标志和 posting-size 文件。它核对：

1. VID 的向量 Float32 位与固定映射一致，删除标签与 ReadView 可见性一致。
2. 每个活跃 posting 的持久计数等于真实记录数，同一个 posting 内没有重复的当前版本 VID；退休 head 的计数为零。
3. 所有可见 VID 都有当前 version 的覆盖，并且在穷举的最近活跃 head 中至少存在一份当前版本记录。

合法的旧版本记录可以保留并单独计数，不把“已不可见”等同于“字节已回收”。实际 ANN 结果另外复用上一轮的历史 SQL、真实 Vortex、typed 值／IEEE 与排序／距离／recall 验收；穷举仅用于验收，没有查询 fallback。

本轮发现并修正两处 merge 问题：

- 过滤过期记录后，upstream 用 live 数量遍历原始 posting 的前缀，可能漏掉需要重分配的 live 向量。改为遍历对应的过滤后记录。
- merge 把仍存活的 keeper head 作为 `HeadPrev`，使 RNGSelection 在它仍是一个副本时提前跳过修复。改为传入实际退休的 head，使新的最近分区和 version 更新得以写入。

[原生组件反例](../test/vane/vortex_spfresh_lire_merge_case.py) 使用固定的 256＋1,600 行输入，显式标为 synthetic component VID，独立于真实 DuckLake RowRef 的端到端验证。它在同一份关闭后的 checkpoint 上分别运行修复版与撤回上述两处修正的[控制构建](../test/vane/spfresh_update/native-merge-control.patch)，逐文件 SHA 确认输入相同。构建报告要求两份 SDK 只在目标 header 不同；控制构建仅用于验收，没有运行时兼容模式。

## 确定性结果

正式 LIRE 专项通过 **252 次实际 ANN 查询比较、388 次显式 drain、10 项拒绝检查与 3 个真实 SIGKILL 边界**，fixture 最低 recall **1.0**。其中 108 次查询位于追加数据的两个簇与分区边界，分别检查 k=1／3／10；其余查询延续上一轮固定输入。源 snapshot、历史 SQL、Vortex IEEE 行与业务 NULL／NUL 值均独立核对。

| 验收 | 实测结果 |
| --- | --- |
| 真实 DuckLake 输入的 split | 1 次实际 split、1,605 次 reassign、750 次 version 增量；重开后 66 个 head 槽、65 个活跃 head、1,856 个 live VID |
| 真实 DuckLake 输入的 merge | 为一个稀疏 posting 分两笔删除 1,000＋121 行；1 次 merge、5 次 reassign／version 增量；重开后 64 个活跃 head、735 个 live VID |
| 每次工作预算 | 实测最大 13 次嵌套操作、59,237 字节 posting 写入；预算 1 能执行单次 split，但下一项 Reassign 整项暂缓，队列与写入保持不变 |
| 原生反例 | 固定 720 个删除后，控制版遗漏 VID 257 的最近 head 65，只在 head 66 留有当前版本；修正版完成 5 次 version 更新并通过全部 live VID 最近分区检查 |
| 实际 SIGKILL | split 修改 head／posting 后、Reassign version 递增但尚未追加时、merge 修改持久状态后；全部保留原发布 head，候选无 seal，旧视图继续查询 |
| 明确拒绝 | pending checkpoint、1 字节 posting 预算、4 种无效工作／字节预算，以及错误版本标签／向量映射／posting size／head 退休标志 |
| 旧版本回收 | 4 个 canonical checkpoint 分别按 1 对象计费，包含 seal；初始／分裂前／分裂后私有视图继续查询固定 snapshot |

本地完整 fixture 为 `/dev/shm/dvlire-final-20261107`；持久摘要目录 `/tmp/ducklake-r7-lire-proof-20261107` 的 `report.json` SHA 为 `46193b54eeb802b1a2da33eddffed54a3acc26ae830faa84c9d13385881896cd`。干净构建的修正版探针 SHA 为 `7dc36b75722e88e7903c2780f538d9042b300d98072b30f3f1f81f118bd3ec02`，控制版为 `c479ab96b7bf50e75b2a8df5169274b4587cde47f919ac0c3de119bf59f5896a`；两份构建报告随摘要保存。13 项线程环境均保持未设置。

用同一个新探针重跑上一轮原生增删专项，通过 **156 次 ANN 查询、23 项拒绝与 6 个真实 SIGKILL 边界**，最低 recall 1.0。Vane 基础 launcher 分非 Ray／Ray 进程通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**；日志 `/tmp/ducklake-r7-lire-base-20261107.log`，原生增删回归日志 `/tmp/ducklake-r7-lire-updates-regression-20261107.log`。

生产 `src/`、Cargo／SDK pins 和三个已安装 Vane／DuckLake／Vortex 构件保持 #96 字节；新增 C++ 单独编译，无需重建未改变的生产包。同摘要目录的 `checkout-audit.json` 核对执行源码、每份 SDK 的全部 342 个文件、两个构建 profile、运行库及已安装构件；仓库 format-check、workflow YAML／shell、文档链接、copyleft 和 diff 检查通过。基础套件和构建 elapsed time 只作观测，不设延迟／吞吐通过条件。

## 复现

```bash
python -I test/vane/build_vortex_spfresh_updates.py \
  --spfresh-source /clean/pinned/SPFresh --vortex-source /clean/pinned/vortex \
  --dependency-prefix /usr --build-dir /new/private/lire-build \
  --cpus 6,7,8,9,10,11

python -I test/vane/build_vortex_spfresh_updates.py \
  --spfresh-source /clean/pinned/SPFresh --vortex-source /clean/pinned/vortex \
  --dependency-prefix /usr --build-dir /new/private/lire-control-build \
  --cpus 6,7,8,9,10,11 --merge-control

VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_spfresh_lire.py \
  --probe /new/private/lire-build/build/ducklake_spfresh_update_probe \
  --build-report /new/private/lire-build/build.json \
  --merge-control-probe /new/private/lire-control-build/build/ducklake_spfresh_update_probe \
  --merge-control-build-report /new/private/lire-control-build/build.json \
  --cpus 6,7,8,9,10,11 --output-dir /new/private/lire-proof
```

构建器要求全新目录和确切、干净的两份 upstream checkout；报告记录全部 SDK 文件、patch／C++／构建输入、依赖及二进制 SHA。测试核对两个构建 profile 和源码差异，固定 CPU，并在结束时核对执行源码和继承线程环境未变。CI 在既有 native workflow 中同时重跑上一轮增删专项与本轮 LIRE／反例，保存构建报告及完整专项证据；CI 使用 runner 的 CPU 集合。

## 后续

下一项推荐 R7 动态候选的持久登记、取消和有预算的崩溃回收契约，先明确 UUID／源 snapshot／覆盖水位及状态转换。随后接入动态 provider、DuckLake 元数据原子发布、提交增量游标和并发快照；当前实验指针不能替代这些协议。

后续已增加[持久候选／取消／崩溃回收的本地组件验证](native-vortex-spfresh-candidates.md)，包括继承式原生进程锁、单一提交根、源引用与分批 GC；本页保留 #97 历史范围和结果，最新顺序见总 roadmap。

R6 继续暂缓。后台并发、任意 filter、维度／质量矩阵、断电持久性、持续容量与收益仍需独立验证；依赖稳定负载的 R4 A/A、性能 A/B、长期稳态及相关 R5 项目继续暂缓。
