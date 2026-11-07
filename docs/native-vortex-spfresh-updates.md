# R7 本地 SPFresh 原生增删、checkpoint 与旧 ReadView 验证

本轮基于 [Draft #95](https://github.com/AstroVela/ducklake/pull/95) 的 `56e8c9036ef068f487512caa2a146ed5c5f52e24`，新增独立的本地 R7 组件实验。R6 的实际远程 codec 接线继续暂缓，未提交的 R6 工作区保持独立。总入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。

**已经实际调用 SPFresh `AddIndexSPFresh`／`DeleteIndex`，并验证关闭重开、候选崩溃与旧视图。生产 SQL／optimizer 仍使用 `spfresh.static`。** 本轮没有把动态 provider 接到生产 SQL，也没有绕过 DuckLake 的事务、MVCC、flush、回执或普通事务保护。

## 运行范围与固定输入

| 项目 | 本轮边界 |
| --- | --- |
| 业务数据 | 256 个初始键、8D Float32、平方 L2；替换、删除、新增，含 NULL、Unicode、NUL 和 signed zero |
| 业务写入 | 既有受管 mutation API，同一 DuckLake 原生事务提交数据和回执；提交后先由正常 B/F/T 查询覆盖 |
| 动态输入 | 显式 flush 后的实际 native ReadView；绑定 UUID、schema、字段 ID、snapshot、文件版本及物理 RowRef |
| SPFresh | 固定 `5893eb61ee3b18610b6b00f1939be7dae1af8904`，实际 RocksDB 8.9.1；UseKV=true／UseSPDK=false |
| 查询／构建 | 64 个 head、64 search heads、12 pages、4 replicas、MaxCheck=4096；native build/search 各使用原来的单线程设置，DuckDB threads=2 |
| 资源 | CPU 6–11；13 项线程环境全部继承且保持；原生输入 100,000 物理行、source/vector/artifact 各 64 MiB、manifest 1 MiB；mutation 1,000 操作／4 MiB；GC 每次最多 16 逻辑对象／64 MiB |
| 新存储依赖 | 只链接独立测试探针；RocksDB 后台任务／subcompaction 各 1，block cache 64 MiB；不改生产 provider 或现有线程环境 |

`InPlace=true／Update=false` 在此固定 upstream 中选择同步 posting 增删，并关闭后台 LIRE split/merge/reassign。本轮验证原生增删接口与版本所有权；**没有验证 LIRE 重平衡、生产动态查询或持续负载收益**。Cache 和回收预算也不构成总 RSS／磁盘上界。

## 数据提交与索引推进

1. 业务 upsert/delete 通过既有受管入口提交。旧普通 DuckLake 事务继续看到原版本；相同请求返回原回执，改变内容复用请求被拒绝。
2. 新向量在 T 中立即参加既有命名 B＋T 查询。未 flush 的数据不能成为这个文件型动态实验的输入。
3. 显式 flush 后调用 native ReadView，固定这次调用实际捕获的 snapshot、文件清单、可见物理 RowRef 和业务列。
4. 从已验证且关闭的 checkpoint 复制一个新的私有候选；按物理 RowRef 差集调用原生 Delete/Add，不按业务 ID 推测 native VID。新增 VID 单调追加，删除版本保留映射。
5. 同步 RocksDB WAL，保存 posting-size record、head、version labels，关闭原生句柄。逐文件 fsync、清单封存和目录 fsync 后，才允许 expected-revision 发布。

这里的 `head.json` 是[实验 fixture](../test/vane/vortex_spfresh_update_fixture.py) 的单一本地发布指针，采用锁、原子 rename 和目录 fsync。它**尚未在 DuckLake 元数据事务中注册或原子发布动态索引**。业务事务与索引水位分开推进；实验查询要求确切水位匹配，失配直接拒绝。生产命名查询继续通过原有 B/F/T 路径执行。

每次更新复制完整 checkpoint，是本轮保护旧版本的明确实验方案。已有 head 文件在 Add/Delete 后字节不变，没有重新调用全量 BuildIndex；复制本身仍有 O(checkpoint 大小) 的 I/O，本轮不据此宣称低写放大或生产性能收益。

## 旧视图与恢复契约

ReadView 在发布／回收锁保护下校验并完整复制封存 checkpoint，然后以 RocksDB `OpenForReadOnly` 打开自己的副本，同时持有固定的 typed 行和 RowRef→VID 关系。查询不修改 checkpoint；原 canonical 版本删除后，已打开的旧视图继续运行实际 ANN，并返回原 snapshot 的 payload 和向量。

原生增删成功本身不是发布或业务 ACK。候选在 mutation 后、checkpoint 后被 SIGKILL，都没有可接受的 seal；完成 seal 但尚未发布的候选也不会被恢复过程自动采用。发布前崩溃保留原 head；发布后响应丢失则核对持久的原 revision／generation，不自动重建或改写旧请求。

另一个场景在 DuckLake mutation 已提交、数据仍在 T、ANN 尚未推进时，实际 SIGKILL 业务进程。重开后业务数据和回执从原生 WAL 恢复，既有 B/F/T 查询立即返回新版本；动态实验仍明确报告水位落后。只有显式 flush、Add/Delete、checkpoint 和发布完成，动态查询才推进到新 snapshot。

本轮 GC 只验证实验中**已封存且不再被 head 引用**的 canonical checkpoint，按原对象／字节上限计费。未封存候选保留作证据，没有自动扫描接管或删除。生产 managed generation/source/scratch 的 GC 协议未改变；动态候选登记、取消与部分回收的完整生产协议仍待后续实现。

## 独立验收与 SDK 修正

[协调器](../test/vane/test_vane_vortex_spfresh_updates.py) 获取真实 native ReadView；[独立验收基准](../test/vane/vortex_spfresh_update_oracle.py) 另外查询该确切 snapshot 的历史 SQL、物理行地址和实际 Vortex 文件，按 Float32 IEEE 位比较向量和 typed 值，并穷举平方 L2 检查 native 命中、距离、排序和 recall。native Float32 距离使用相对／绝对 1e-5 容差；现有生产 SQL 的双精度重排与全局平局规则保持。穷举仅用于验收，不参与查询返回，也没有增加 exact-scan fallback。

独立探针所需的 [SDK patch](../test/vane/spfresh_update/native-update.patch) 在固定 Vortex `static-only.patch` 之后应用，仅进入测试构建：

- 不编译 SPDK 分支，选择错误 backend 直接失败；使用实际 RocksDB，不提供模拟 KV 或替代 decoder。
- 修复已复现的 CPU feature 全局初始化期间访问尚未构造的 logger 的崩溃。
- 动态查询使用合法的本地 SearchStats，检查 RocksDB 读取错误；沿用既有静态查询的显式 head/page/check 路由预算，修复 head 命中被置为无效后错误跳过 posting 的空结果。
- 增加明确的只读打开和 checkpoint 操作，保存 posting-size record 并同步 WAL；支持 RocksDB merge 的空 existing value。

空结果控制使用同一份冻结 checkpoint：旧探针返回空，修正后 1／3／10 个命中均可取得。旧探针及日志保留在 `/tmp/ducklake-r7-probe-before-head-fix-20261107` 和 `/tmp/ducklake-r7-native-rank-{pilot,fixed}-20261107.log`；初始化崩溃的旧二进制也独立保留。该对照不是性能 A/B。

## 确定性结果与复现

最终专项通过 **156 次实际 ANN 查询比较、23 项明确拒绝、6 个真实 SIGKILL 边界**，fixture 最低 recall **1.0**。四项故意错误结果（重复、旧 payload、错误距离、不可见地址）均被独立验收拒绝。两个已退休 checkpoint 分别按 **1 对象／131,206 字节**、**1 对象／143,959 字节**回收，旧视图均继续查询成功。

```bash
python -I test/vane/build_vortex_spfresh_updates.py \
  --spfresh-source /clean/pinned/SPFresh --vortex-source /clean/pinned/vortex \
  --dependency-prefix /usr --build-dir /new/private/r7-build \
  --cpus 6,7,8,9,10,11

VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_spfresh_updates.py \
  --probe /new/private/r7-build/build/ducklake_spfresh_update_probe \
  --build-report /new/private/r7-build/build.json \
  --cpus 6,7,8,9,10,11 --output-dir /new/private/r7-proof
```

构建器要求两个上游输入处于确切 revision 且干净，复制源码后应用固定 patch，从头编译独立探针并记录所有源码、依赖、构建输入和二进制 SHA。最终探针 SHA 为 `7542815d6ba38261de70f46118b1fa63770b754d7446d210370cc73c9544c2bb`；专项核对实际二进制与构建报告。Ubuntu 24.04 CI 固定安装 RocksDB／TBB 版本，在既有 native provider workflow 中新增此组件专项，并保存构建／查询／故障证据；CI 使用 runner 自身 CPU，不能外推为在 CI 重测了本地 CPU 6–11。

本地报告 `/tmp/ducklake-r7-proof-v3-20261107/report.json`，SHA `f7b6e7f4e5921b012aa021ef446ffc0c747338b4c0e9e86aef4afa831fa7ffad`；干净构建报告为同目录 `native-build.json`。服务和 fixture 使用显式私有 `/dev/shm` 目录，报告另存 `/tmp`，没有自动切换路径。原生生产源码、Cargo／SDK pins 和三个已安装 Vane／DuckLake／Vortex 构件保持 #95 字节；新增 C++ 是单独编译的测试程序，不进入 provider wheel。

受管写入回归通过 **13 个原生／恢复用例和 28 次查询比较**，最低 recall 0.9；epoch 回归通过 **8 个功能检查和 14 个原生／恢复用例**。Vane 基础 launcher 在独立非 Ray／Ray 进程通过 **1,222＋32＝1,254 项**，可选依赖缺失跳过 **11＋4＝15 项**。日志为 `/tmp/ducklake-r7-{keyed-regression,epochs-regression,base}-20261107.log`，回归报告保存在 `/tmp/ducklake-r7-regressions-20261107/`。

专项同目录 `checkout-audit.json` 核对 13 个执行源码摘要、342 个干净 SDK 源文件、native 构建／依赖／二进制及与 #95 相同的三个已安装构件。重开还核对原生 live-row 总数等于 ReadView 的可见行数。仓库 format-check、workflow YAML／shell、文档链接、copyleft 和 diff 检查通过；C++ 探针单独编译，生产包无需无关重建。

## 剩余路线

下一项推荐 R7 有预算的 LIRE split／merge／reassign 确定性专项，先验证重平衡时 native VID、posting、version labels 和 checkpoint 是否一致，以及旧视图是否保持。随后再决定动态 provider 的生产接口、DuckLake 元数据原子绑定、增量游标、候选生命周期和并发协议。

后续已补充[受控串行 LIRE 的独立确定性验证](native-vortex-spfresh-lire.md)，包括原始 posting oracle、merge 缺陷的同输入控制构建、重平衡中实际 SIGKILL 和旧视图；本页保留 #96 当轮范围与历史结果，最新顺序以总 roadmap 为准。

固定输入上的复制／重开证明不等于高吞吐在线 serving。任意 filter、现有全局 top-k／稳定平局顺序、维度矩阵、长期质量、掉电持久性、原生 SQL 自动接线与长期磁盘／RSS 资格仍需单独验证。R6 继续暂缓；R4 A/A、性能 A/B、长期稳态和依赖稳定负载的 R5 项目也继续暂缓。
