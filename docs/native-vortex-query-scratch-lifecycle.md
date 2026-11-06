# Native Vortex 查询 scratch 所有权与显式回收

这是 R5 的本地 POSIX 生命周期验证。承接 [容量 admission](native-vortex-capacity-admission.md) 和 [reference／WAL 契约](native-vortex-reference-wal-lifecycle.md)，为原生 SQL 查询建立私有副本登记、活跃持有者保护和崩溃后的显式回收。统一设计见 [DuckLake＋Vortex roadmap](ducklake-vortex-design-roadmap.md)。

本轮使用同一套固定输入、CPU 6–11、DuckDB `threads=2`、既有 SPFresh／环境线程设置及事务保护。性能 A/A、A/B 和长期稳态继续暂缓；测试耗时仅作观察，不作为门槛。

## 所有权范围

查询以 canonical `TMPDIR` 为父目录，为 canonical DuckLake `DATA_PATH` 建立一个命名空间：

```text
TMPDIR/
  .ducklake-vortex-query-v1-<euid>-<sha256(DATA_PATH)>.lock
  .ducklake-vortex-query-v1-<euid>-<sha256(DATA_PATH)>/
    root.json
    registry/<query-uuid>
    work/<query-uuid>/
      owner.lock
      payload/<SDK 私有 generation 副本>
```

根、registry、work、每个查询工作目录及 payload 为 `0700`，控制文件为 `0600`。SDK 子文件继承原有 umask；它们由私有根和 payload 隔离，拒绝其他用户所有、world-writable、符号链接、硬链接和非普通文件。无需改变进程全局 umask 或 SDK 线程配置。

`root.json` 绑定 DATA_PATH、euid、根／父目录／父级锁／registry／work 的 device 和 inode。查询记录进一步绑定工作目录、payload 和 owner.lock。仅名称相同、复制登记文件或替换 inode 均不能取得所有权。

根初始化在同一父目录中先完整写入控制信息，fsync 后以不覆盖目标的 rename 发布。发布前不会写入查询副本。这个阶段崩溃可能留下未登记的少量初始化元数据；显式回收不会扫描或接管它。旧版临时目录和任意其他 TMPDIR 同级目录也不被接管。

payload 是登记时独占委托给 SDK 的工作树。在首次回收计划发布前，其普通文件属于这一查询；无法也不声称能区分同一用户故意放入该树的其他普通文件。协议目录中的未知控制文件明确失败；不在登记中的 work 同级目录保持原样；计划发布后新增、替换或无删除意图消失的 payload 项明确失败。

## 查询与活跃保护

查询先持命名空间锁，durably 写入 `allocating` 记录，再创建目录及 owner.lock，持有文件锁后发布 `live`。正常结束、取消和可返回错误都先释放 ReadView／SDK lease，再关闭登记。关闭要求 payload 已由 SDK 释放；异常残留会明确报错并保留登记，交给显式回收。

文件锁随进程退出由内核释放，不根据 PID、时间戳或目录年龄猜测查询是否结束。回收对活跃 owner 的锁使用 try-lock，返回 `blocked_views`，不读取其 payload。已打开的旧 ReadView 在规范源／generation GC 及 metadata checkpoint 后仍持有私有副本和文件锁。

查询诊断 JSON 增加 `scratch_root`。正常关闭后，根、父级锁、空 registry／work 及轮转游标会保留；“已释放”要求没有任何登记或 payload，不能要求整个 TMPDIR 为空。

## 显式 SQL 与预算

调用方从查询诊断保存确切根路径，在 autocommit 下显式执行：

```sql
SELECT * FROM ducklake_collect_vortex_query_scratch(
    'lake', '/canonical/tmp/.ducklake-vortex-query-v1-<uid>-<hash>',
    16, 67108864
);
```

参数为 catalog、已登记根、对象预算、字节预算。对象数限制为 1–16、字节为 1–64 MiB，不增加默认兼容路径或扫描重试。返回一行：

| 字段 | 含义 |
| --- | --- |
| root | 此次认证的命名空间 |
| registered_views／remaining_views | 调用前后登记数 |
| examined_views／blocked_views | 本次检查及因活跃锁暂缓的 owner 数 |
| collected_objects | 已实际删除的 payload 文件／目录，加完成清理的 owner 数 |
| collected_files／collected_bytes | 已实际删除的 payload 普通文件数和文件字节 |
| removed_views | 已完成清理的登记数 |

同一次调用共享预算，不能按文件或 owner 放大。最多检查对象预算数量的 owner，持久游标轮转，避免前 16 个活跃 owner 长期阻塞后面的死 owner。命名空间锁被分配或另一回收持有时，显式调用返回 busy，由调用方决定下一次调用；没有内部重试。

每个文件仍受既有 64 MiB artifact 限制，删除清单受既有 1 MiB manifest 限制；路径长度及深度也有限制。协议元数据、清单遍历、校验及 fsync 开销不计作 payload 字节；这些预算不构成总 I/O、RSS、延迟或磁盘空间的上界。

本 API 不改受管 generation/source 的 GC、13 张 Vortex 元数据表、调用方 reference JSON、业务事务、原生 WAL 或 checkpoint 协议。显式用户事务内调用拒绝执行；重开不会自动清理或启动 optimizer。

## 部分清理与恢复

死 owner 的 payload 按后序清单冻结，记录相对路径、类型、device／inode、大小和 `present/pending/removed` 状态。删除始终使用已打开的目录 FD 与 `openat(O_NOFOLLOW)`／`unlinkat`。

1. 发布 `collecting` 清单并 fsync。
2. 每项检查身份及剩余预算，写唯一 `pending` 意图并 fsync，再 unlink，fsync 父目录后确认 `removed`。
3. 中断后，仅 pending 项允许已经消失；此前无意图的缺失、未知新增或身份变化均失败。
4. 未完成 pending 优先恢复。若它尚在且超过本次字节预算，保持一个 pending，不另外创建意图；若已 unlink，则只确认状态，不重复收费。
5. 所有 payload 项删除后，先发布 `finishing` 意图，再删除空 payload、owner.lock、工作目录，最后删除登记。崩溃后的工作目录缺失只有这一持久意图可以授权。

协议预留的 `.record.tmp` 是根锁串行化的有界元数据暂存槽；下次认证打开可丢弃它。它不存放查询副本，也不授权接管其他未知文件。

## 确定性证据

入口为 [Python coordinator](../test/vane/test_vane_vortex_query_scratch.py)、[独立文件系统模型](../test/vane/vortex_query_scratch_model.py)、[真实 native fixture](../test/vane/native_vortex_scratch_fixture.hpp) 和 [Rust 子进程探针](../test/vane/vortex_index_probe/src/scratch_probe.rs)。CI 在同一 provider 闭包上执行并保留报告与恢复证据。

```bash
env -u VORTEX_SPFRESH_NATIVE VANE_RUNNER=local-fast python -I test/vane/test_vane_vortex_query_scratch.py \
  --driver /path/to/ducklake_vortex_write_driver \
  --probe /path/to/ducklake-vortex-index-probe \
  --cpus 6,7,8,9,10,11 --output-dir /fresh/query-scratch-proof
```

功能验证逐项比较 13 张原生元数据表、业务 typed rows、snapshots，以及全部 data／indexes 文件的 SHA-256。普通 SDK 文件边界使用独立 filesystem fixture，真实查询副本单独经 native SQL 和安装态 provider 验证，不把合成文件测试计为 ANN 质量或容量结论。

截至 2026-11-07，本轮通过：

- **25 组独立文件系统协议场景**：正常关闭、活跃锁、未完成分配、对象／字节边界、真实 64 MiB 单文件及全局字节计费、无效参数、错误／复制根、未知控制文件、符号／硬链接、目录／锁／文件身份替换、计划后新增和无意图缺失、部分回收重试，以及 18 个 owner 的轮转公平性。
- **5 个真实查询／安装态重开场景**：正常、查询取消、分配取消、打开索引后失败，以及旧视图跨规范 GC／checkpoint 查询。
- **5 个查询 SIGKILL／重开边界**：`scratch_allocated`、`scratch_registered`、`source_pinned`、`after_open`、`before_fetch`。
- **7 个真实清理故障／重开场景**：plan 发布、unlink 前后、工作目录删除前后五处实际 SIGKILL，以及 unlink 后的取消和注入错误。

以上 42 个场景合计执行 **45 次实际 SIGKILL**（包括生成崩溃 fixture 的 owner），不把同一次恢复重复计为额外故障点。每次故障均由新的安装态进程显式重开／回收；正常查询与重开不隐式删除其他 owner。

本地主报告 `/tmp/ducklake-vortex-scratch-proof-v5-20261107/report.json` 的 SHA-256 为 `7375cb617a4d69f351ee8f16758952627c36e7142ff00534ddba72327277d716`。报告锁定执行前后的源码、probe、driver、已安装 provider artifact 和 `_native` 身份，并验证子进程 affinity／线程环境及父进程 affinity 恢复。

| 构件 | SHA-256 |
| --- | --- |
| `_native` | `8ca20bd4c8a4203f239ff6d785c45575241d3b2c8d1f8e9fc184d90de1d54876` |
| native write driver | `7e88e2b4076aa04cd1ef79de121371d1c00527a7910f0f05e4ead28b8630bfef` |
| Rust probe | `eb24ad5ca1b5df57dae942f2d2212eef55fc85dbf7fd33e81c064a05cfc1c4d0` |
| 已安装 DuckLake artifact | `9a8b761d8ab6b61647bd912c7d2a614e0080b5c315c7002fdf3178d8c9f23878` |
| 已安装 Vortex artifact | `16cc10c2bb607f539618a0887a5551f41eae51c2bd981fdb063d3c2536a85495` |
| DuckLake 测试 provider wheel | `3ff4ac58a8e0fa0175e80b6a49ca7ddb37fef1c9b20e62312c28efc314632e1b` |
| Vortex 测试 provider wheel | `d7cd646e0db78bbc5e2ac03c84030794165109150c8684e1445e59d388a30b8e` |

匹配测试 wheel 与非 editable 隔离环境分别保留于 `/tmp/ducklake-vortex-scratch-wheels-20261106`、`/tmp/ducklake-vane-scratch-env-20261106`；它们仅供验证，使用公开 CI 测试签名，不是发布资格。既有 Vane、wrapper、vortex-index／SPFresh pin 与 #87 相同；Cargo.lock 只增加两个已锁定依赖的直接引用，没有变更依赖版本。

相关五组链路回归通过 **77 项原生／恢复用例、76 次已记录 fixture 比较**。容量 admission 再次通过 11 组真实数据、31 项 envelope 解析、7 项通用存储，以及 4 个原生事务／重开和 3 个 SIGKILL／恢复场景。四维度矩阵通过 **36 个阶段、864 次普通／诊断查询和 56 项拒绝检查**；Vane base launcher 分进程通过 **1,222 个非 Ray、32 个 Ray 测试，15 项可选依赖跳过**。格式、Rustfmt、copyleft、工作流 YAML 和文档链接检查通过。

经典 native fixtures 的 22 处空 TMPDIR 断言也迁移到同一严格的 released-scratch 模型，18 个模块的加载、空根／认证空协议根及未知路径拒绝适配检查通过。当前 Vane 构建为 `BUILD_UNITTESTS=OFF`，没有经典 catalog driver target；本轮没有把这些旧 driver 的端到端运行计为已通过。真实查询的执行与重开证据来自上述 17 个新 native 场景及安装态回归。

R5 仍有表 UUID 状态记录及总磁盘生命周期工作；远程／分布式 R6 与 SPFresh 原生在线更新 R7 未由本轮实现。
