# Native Vortex 表 UUID 状态生命周期

本轮承接 [查询 scratch 生命周期](native-vortex-query-scratch-lifecycle.md)，实现 R5 的 UUID 防重放及显式表退休。统一入口为 [DuckLake＋Vortex 总设计与 roadmap](ducklake-vortex-design-roadmap.md)。本地验证保持 CPU 6–11、原有 DuckDB／SPFresh 线程设置、事务保护和既有预算；稳定负载 A/A、性能 A/B 与长期稳态继续暂缓。

## UUID 是请求身份的一部分

表名可以重用，新表的 epoch 可以重新从 1 开始。因此，所有可重试批次和 epoch 推进都必须提交请求创建时保存的 UUID，不能在重试时重新读取当前表的 UUID。

```sql
SELECT * FROM ducklake_vortex_request_status('lake','main','items','knn');
-- 为新请求保存返回的 table_uuid 和 request_epoch。

SELECT * FROM ducklake_mutate_vortex_batch(
    'lake','main','items','knn','request-1',
    [{'id':1::BIGINT,'embedding':[1,0]::FLOAT[],
      'category':'A'::VARCHAR,'payload':'value'::VARCHAR}],
    []::BIGINT[], 1, '<saved-table-uuid>'
);

SELECT * FROM ducklake_advance_vortex_request_epoch(
    'lake','main','items','knn',1,'<saved-table-uuid>'
);
```

`ducklake_write_vortex_batch` 同样在原 epoch 参数后增加必需的 UUID。C++ Append／Mutate／AdvanceRequestEpoch 也要求该参数。UUID 必须是 canonical 字符串，并与实际目标匹配；同名重建后，已配置目标的旧 UUID 返回 `STALE_TABLE`，业务行和回执保持不变。表不存在时也不接受写入。旧参数个数没有兼容重载或自动升级路径。

请求幂等键仍是 `(table_uuid, request_epoch, request_id)`，已有 payload 编码、哈希、窗口推进与旧 epoch 拒绝规则不变。一个 UUID 的状态退休后，新表可以使用相同请求 ID，但必须提交新 UUID；旧请求不能转换为新表请求。

## 显式退休入口

```sql
SELECT * FROM ducklake_retire_vortex_table(
    'lake','main','items','<saved-table-uuid>'
);
-- table_uuid, retired
```

此入口要求 autocommit、原生 DuckDB 元数据、已初始化的索引／写入／optimizer catalog 和当前四业务列 Vortex schema。它校验确切 UUID，在一个原生事务中完成逻辑 DROP 和 UUID 状态释放：

| 对象 | 退休行为 |
| --- | --- |
| 请求回执、epoch、managed 注册、写预算、optimizer policy | 只删除目标 UUID 的记录 |
| 所有命名索引及 generation refs | 同一提交中解除该 UUID 的全部命名引用 |
| generation | 最后一个引用解除后标记 retired，后续由现有 GC 回收 |
| build／rewrite 所有权 | 仍有记录则拒绝；先显式恢复和回收，再提交退休 |
| catalog 级 maintenance／source retention／源删除意图 | 保持其现有作用域和生命周期 |
| 调用方 reference JSON、未知文件、查询 scratch | 由各自所有权协议处理 |
| DuckLake 表历史、inline 历史、源文件和 WAL | 保留原生历史及物理回收契约 |

受管表仍拒绝普通逻辑 DROP；退休入口只为目标 UUID 开放这一操作。未管理表的普通 DuckLake SQL 语义保持，未通过退休入口完成的删除不能视为已经释放 Vortex UUID 状态。重开不会扫描接管旧孤儿状态；本轮没有为这些状态增加迁移或 fallback。

成功后，在原生表历史仍可认证该 UUID 且同名表尚未重建时，重复退休返回同一 UUID／`retired=true`，不再推进快照。历史已过期的未知 UUID 明确失败；同名新表的 UUID 不符也失败。入口不凭名称、缺失记录或目录内容推定其他对象已经退休。

## 提交与后台任务保护

退休沿用写批次串行化锁。提交仍先取得写 admission，再取得 publication exclusive；随后持有 optimizer 元数据锁，直到原生 DROP 与 UUID 记录删除提交完成。内部提交重试为零，冲突由调用方显式重试。

enabled policy 或仍在执行的任务阻止退休。执行标记覆盖 worker 取出任务到完成错误处理的整个范围，包括加载元数据前和等待写者的阶段。取消标记不能代替任务结束；已复制的待执行任务也须在运行前重新确认调度归属。

元数据提交成功后才释放内存中的 policy、取消及错误记录，避免旧 UUID 消耗 16-policy 容量或再次保存已退休状态。取消、注入错误和真实元数据冲突必须同时回滚 DROP、引用变化与 UUID 状态删除。

## 物理回收与旧读者

退休本身不 unlink 数据或索引文件，不删除调用方 JSON，不清理 WAL。generation／source collector 继续使用原有对象数及字节预算、事务 gate、引用与文件身份检查。保留中的普通 SQL 事务继续读取其旧快照，并继续阻止源 GC；已打开的私有 ReadView 可在规范 generation GC／checkpoint 后完成查询。

释放 UUID 元数据和逻辑配额不等于原生历史、元数据文件、WAL 或 RSS 已收缩。本轮不建立总磁盘或长期容量上界，也不接入 SPFresh 原生在线更新、远程存储或分布式查询。

## 确定性验证

入口为 [安装态 coordinator](../test/vane/test_vane_vortex_table_uuid.py) 和 [native fixture](../test/vane/native_vortex_uuid_fixture.hpp)。固定 typed row 模型、13 张 Vortex 元数据表、原生表历史、snapshot 身份及全部 data／indexes 文件摘要用于恢复比较。

```bash
env -u VORTEX_SPFRESH_NATIVE VANE_RUNNER=local-fast python -I \
  test/vane/test_vane_vortex_table_uuid.py \
  --driver /path/to/ducklake_vortex_write_driver \
  --cpus 6,7,8,9,10,11 --output-dir /fresh/table-uuid-proof
```

覆盖另一 UUID 的回执、待清理 build、引用和实际 B 查询保持不变，同名重建后的旧写入／推进／退休拒绝、100,000 回执配额释放、17 次 policy 槽位复用、普通读者保护、共享 B 引用解除、活跃及取消中的任务、未结束 build、八个并发相同退休、真实冲突、取消和 SIGKILL。恢复必须由新的安装态进程显式执行，不能把重开视为维护已自动恢复。

本轮专项验收通过 **6 组公开契约、25 个 native／安装态重开场景**，其中 **5 次实际 SIGKILL** 位于 DROP 提交前、索引引用暂存后、回执暂存后、全部 UUID 状态暂存后及提交后。取消和注入错误分别覆盖取得写锁后到提交前的七个阶段。

真实冲突通过另一个连接提交独立的空原生 snapshot／changes 记录，竞争本次 DROP 的 snapshot 主键。测试确认实际冲突错误，逐项比较回滚后的 13 张元数据表、原生历史与业务行，再显式重试成功；不会把未形成冲突的并发更新计为冲突验收。

本地主报告为 `/tmp/ducklake-vortex-uuid-proof-v5-20261106/report.json`，SHA-256 为 `940716383d3d7625e9700b3d0b9caf593c630694a04e70b5aae53fbeabc9967f`。报告锁定执行前后的源码、driver、已安装 provider 闭包及 `_native` 身份，并核验 CPU affinity 继承／恢复和原线程环境。所有环境线程变量保持原值；测试耗时仅作观察。

| 构件 | SHA-256 |
| --- | --- |
| `_native` | `8ca20bd4c8a4203f239ff6d785c45575241d3b2c8d1f8e9fc184d90de1d54876` |
| native write driver | `3345502759f87925a36f4d31e001b2508ecc37a31a098181d8925c47f4530849` |
| 已安装 DuckLake artifact | `da5826322a42a6e5bbe8e8897d6b99d6f11d4f26be2b0839cfdf2d253d5b321d` |
| 已安装 Vortex artifact | `16cc10c2bb607f539618a0887a5551f41eae51c2bd981fdb063d3c2536a85495` |
| DuckLake 测试 wheel | `7993d833a8c2aa7f8d8f97cf529dabe0c01648668f953290d456afa32873bc7f` |
| Vortex 测试 wheel | `d7cd646e0db78bbc5e2ac03c84030794165109150c8684e1445e59d388a30b8e` |

隔离环境与匹配 wheel 保留于 `/tmp/ducklake-vane-uuid-env-20261106`、`/tmp/ducklake-vortex-uuid-wheels-v3-20261106`。构建使用非 editable 增量 Release 重装和公开 CI 测试签名；不是发布资格。Vane、wrapper、vortex-index／SPFresh 与 #88 的 source pin 相同；没有修改依赖版本或增加兼容路径。

现有五组链路回归通过 **77 项原生／恢复用例、76 次 fixture 比较**；容量 admission 的 11 组真实边界、31 项 envelope、7 项通用存储及 7 项原生／恢复场景再次通过。查询 scratch 的 42 个场景和 45 次实际 SIGKILL、四维度矩阵的 36 个阶段／864 次普通及诊断查询／56 项拒绝检查也通过。Vane base launcher 分进程通过 **1,222 个非 Ray、32 个 Ray 测试，15 项可选依赖跳过**。相关报告保留于 `/tmp/ducklake-vortex-uuid-regressions-20261106`。格式、copyleft、工作流 YAML 与文档链接检查通过。
