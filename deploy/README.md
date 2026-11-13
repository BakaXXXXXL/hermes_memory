# Hermes 记忆系统健康巡检（待部署）

本目录只包含待部署文件，未修改生产 systemd、生产脚本或生产配置。

`../reference/` 是 `note_indexer.py` 与 `shared_store_backup.py` 的只读副本，仅用于确认 state 格式和备份布局；巡检脚本不导入它们。

## 组成

- `scripts/health_watchdog.py`：只读巡检 + 告警。健康时完全静默；异常时按签名去重，30 分钟内不重复；恢复时发一次恢复通知。timer 与 OnFailure 共用实例锁，state/log 原子写入并 fsync。OnFailure 先写待处理队列再抢锁，锁竞争时不会丢告警。
- `health-watchdog.json`：路径、阈值、通知通道配置（不含密钥）。
- `systemd/hermes-health-watchdog.service`：oneshot 巡检单元。
- `systemd/hermes-health-watchdog.timer`：每 10 分钟触发的 watch timer。
- `systemd/hermes-health-watchdog@.service`：`OnFailure` 模板，接收失败单元名。
- `systemd/dropins/`：note-indexer、wake 的 `OnFailure` drop-in。gateway 不装 `OnFailure`，原因见下。
- `install.sh` / `rollback.sh`：默认 dry-run，只有加 `--apply` 才改生产。

## 巡检项

| 检查 | 数据来源 | 默认阈值 |
| --- | --- | --- |
| note-indexer 真实退出 | `systemctl show note-indexer.service` 的 `Result` / `ExecMainStatus` / `ActiveState` | 非 success / 非 0 即报 |
| 索引 state 新鲜度 | `/root/.hermes/state/note-index.json` mtime 与 JSON 结构 | 15 分钟 |
| note-indexer timer | `note-indexer.timer` 的 `ActiveState` / `LastTriggerUSec`（systemd `--timestamp=unix`） | 20 分钟未触发 |
| wake | `scope-recall-wake.service` 上次退出与 timer | 15 分钟 |
| gateway heartbeat | `/root/.hermes/state/gateway.heartbeat` 的 `updated_at`、`pid` 与 systemd `MainPID` | 180 秒 |
| 备份年龄 | `backups/auto/daily/memory-*.sqlite3`、`weekly/` | 每日 30 小时；每周文件 8 天；每日/每周文件均检查最小大小；周一 03:30 后 2 小时仍无本周（ISO 周）文件即报，即使旧 weekly 还在 |
| 备份一致性 | 同代 `memory-*`、`meta-*.tar.gz`、`manifest-*.json`；DB `user_version` 对 manifest `source_user_version`、meta 内 `.backup-generation.json` | manifest/size/代次任一不符即报；legacy（无 manifest 且 meta 无代次）发 notice 提醒 |
| 备份 cron | `cron/jobs.json` 中 `4d313aab436e` | 停用/上次失败/超过 36 小时未运行/超计划 2 小时即报；缺少或无法解析 `last_run_at`/`next_run_at` 也告警 |
| 周检缺失 | `reports/memory-health/*.md`、`cron/jobs.json` 中 `549fe0406878` | 报告 8 天；cron 超计划 2 小时；jobs 不可读即告警 |
| entry 停写 | `memory.sqlite3` 最近 `entry_scan_rows` 行（按 rowid 有界扫描） | 24 小时；超出窗口无对话条目时发 notice |
| 捕获拒绝 | `errors.log*` 增量读取（有界 bytes/lines），按事件键哈希去重 | 近 7 天；新事件才报 |

捕获拒绝属于安全策略，不是故障；告警只列事件键哈希与时间，不输出原始日志行。首次运行只把现有拒绝事件记为基线（`rejection_bootstrap`），不补发历史；首投失败时不写入基线，下一次仍按 bootstrap 处理。日志游标保存在 state 的 `log_cursors` 中；有界扫描未完成时保留 `rejection_bootstrap_pending`，下一轮继续按基线处理。若 state 中的游标结构损坏（不是对象、缺字段或类型错误），看门狗会丢弃损坏项、从日志起点重新扫描并发出 notice，而不是每轮在读取游标时失败。

待处理队列中的非法 UTF-8 行不会让看门狗崩溃：原文件会完整复制到同目录 `*.corrupt-*` 隔离文件，可读记录保留在原队列并继续告警。合法 UTF-8 但 JSON 无法解析（或缺少 `unit`/`at`）的行同样会隔离并告警，不会被静默跳过；追加记录前会补齐缺失的换行，避免把上一条记录拼坏。超过 `pending_failure_max_age`（默认 24 小时）的记录会先以 notice 列出单元和年龄，成功投递后再清理，不再静默丢弃。

### gateway 不使用 OnFailure

生产 `hermes-gateway.service` 使用 `Restart=always`。单元退出后 systemd 会直接重启，通常不会进入 `failed` 状态，`OnFailure=` 因此不会触发；给 gateway 装 OnFailure drop-in 只会制造“已兜底”的错觉。本目录不再安装 gateway 的 OnFailure drop-in，gateway 由每 10 分钟的 `ActiveState` + heartbeat `updated_at` + `MainPID` 探针覆盖（见上表）。如果确实需要更短故障发现时间，应缩短 timer 周期或增加独立探针，而不是依赖 `OnFailure`。如果曾应用过旧版本，新的 `install.sh` 不会自动删除已存在的 `hermes-gateway.service.d/30-onfailure.conf`，需在 `--apply` 前手工移除并 `daemon-reload`。

### 备份一致性

修复后的备份脚本把 `memory-YYYYMMDD.sqlite3`、`meta-YYYYMMDD.tar.gz` 和 `manifest-YYYYMMDD.json` 作为同一代写入。代次记录在 manifest 和 meta 包内的 `.backup-generation.json`；DB 的 `PRAGMA user_version` 保留源库 schema 版本，manifest 的 `source_user_version` 与之比对。巡检只做轻量校验：manifest（schema 3）可解析、`kind`/`snapshot` 匹配、文件项大小与磁盘一致、DB `user_version` 与 `source_user_version` 一致、meta 代次与 manifest generation 一致；不会对每个备份重算 sha256 或跑 `quick_check`，避免每 10 分钟读整份快照。完整校验仍用备份脚本的 `--verify`。

没有 manifest、且 meta 包内也没有 `.backup-generation.json` 的旧备份被明确视为 legacy：只做原有的 mtime/大小检查，并发送 notice 提醒升级备份脚本。分类按当前快照自身判断，与 `backup --verify` 一致：旧目录里存在其他 manifest 不会把真正的 legacy 快照误报成 error；只有 meta 已带代次但当前快照缺 manifest 时，才按未完成备份报 error。

### 锁竞争与 OnFailure

timer 和 OnFailure 共用实例锁。`--trigger failure` 会先把失败单元追加到 `paths.pending`（默认 `/root/.hermes/scripts/.health_watchdog.pending.jsonl`），再以更长的 `failure_lock_timeout`（默认 120 秒，仍受 service `TimeoutStartSec=180` 约束）等待锁；如果仍然超时，JSONL 日志会记录 `action=lock_busy`，失败单元保留在队列中，下一轮 timer 会补发。成功投递或确认静默后队列才清理。这样 OnFailure 不再因为 timer 正在运行而静默丢失。

## 双通道

`notify.channels` 默认 `["hermes", "smtp"]`：

1. `hermes send --to telegram --file -`：走 Hermes 已配置的 bot 凭据，不依赖运行中的 gateway。
2. `SMTP`：读 `/root/.hermes/qq_smtp.env`（`SENDER` / `TO` / `SMTP_HOST` / `SMTP_PORT` / `AUTH_CODE`），显式 `ssl.create_default_context()`（`CERT_REQUIRED`）用于 `SMTP_SSL` 或 `STARTTLS`；授权码只在内存中使用，不写日志。

任一通道成功即视为已送达；另一通道失败会写 JSONL 日志并以 stderr 提示。两个都失败时退出码 1，状态不推进，下一轮重试。测试全部使用 mock 通道，不发送真实消息。

如果改由 Hermes cron 的 no-agent 脚本模式调用，可加 `--channel stdout`：stdout 由 cron 投递，不再走 `hermes send`/SMTP。

## 部署（需另行批准，本次未执行）

```bash
cd /root/workspace/memory-fix-20261112/health-watchdog
./deploy/install.sh                 # 打印计划
sudo ./deploy/install.sh --apply    # 安装文件、daemon-reload、启用 timer
# 首次启用后手动跑一次只读检查（不发送）：
sudo /usr/bin/python3 /root/.hermes/scripts/health_watchdog.py --trigger manual --dry-run
```

`install.sh` 在首次启用 `Persistent=true` 的 timer 前写入 `/var/lib/systemd/timers/stamp-hermes-health-watchdog.timer`，避免 systemd 立即补跑；首轮落在下一个 10 分钟边界。它不会立即启动巡检 service；需要立刻跑一次时加 `--run-now`。安装会先把每个源文件复制到目标目录的临时文件，校验 sha256 后再原子替换，最后再校验已安装文件；源文件缺失或校验失败时不会覆盖现有文件。测试或打包可用 `DESTDIR`，或分别用 `HEALTH_WATCHDOG_SYSTEMD_DIR`、`HEALTH_WATCHDOG_ROOT`、`HEALTH_WATCHDOG_SYSTEMCTL`、`HEALTH_WATCHDOG_TIMER_STAMP` 重定向路径。设置 `DESTDIR` 且未显式设置 `HEALTH_WATCHDOG_SYSTEMCTL` 时，安装/回滚脚本只写目标树，不调用宿主 `systemctl`；需要操作指定实例时再显式设置该变量。

## 回滚

```bash
sudo ./deploy/rollback.sh --apply
```

回滚会停用并停止 timer/service，恢复（没有备份则删除）timer/service/模板与两个 drop-in，恢复脚本/配置。备份按文件名中的 `YYYYmmddTHHMMSS[纳秒]` 时间戳标识排序选择最新版本，不依赖 `mtime`（`cp -a` 会保留旧文件的 mtime，按 mtime 会选错）。恢复前会校验所有选中备份可读，并按类型校验 Python 语法、JSON 结构和 unit 语法；全部校验通过后才把备份复制到目标目录临时文件并校验 sha256，确认无误后才停止服务，再原子替换并复核。任一校验或复制失败都不会恢复损坏版本，也不会先停掉正在运行的服务；如果停止服务后提交失败，脚本会尝试重新启用并启动 timer。`daemon-reload` 后对主单元和模板实例执行 `reset-failed`。状态文件、待处理队列和 JSONL 日志保留，便于审计；如不需要可手工删除：

- `/root/.hermes/scripts/.health_watchdog.state.json`
- `/root/.hermes/scripts/.health_watchdog.pending.jsonl`
- `/root/.hermes/logs/health-watchdog.log`

## 依赖

- `/usr/bin/python3`（3.11+，仅标准库）
- systemd（`systemctl show --timestamp=unix`、`OnFailure`、template unit）
- 实例锁文件：`/root/.hermes/scripts/.health_watchdog.lock`
- `hermes` CLI（默认 `/usr/local/lib/hermes-agent/.hermes/bin/hermes`）
- `/root/.hermes/qq_smtp.env`（SMTP 通道；缺失时该通道失败，另一通道仍会发送）

## 测试

```bash
cd /root/workspace/memory-fix-20261112/health-watchdog
python3 -m unittest discover -s tests -v
```

测试使用临时目录、假 systemd、假通道和 mock SMTP，不触碰生产状态。安装/回滚测试还会在临时根目录中注入缺失源文件、校验和失败和 mtime 倒序的备份，验证失败不会覆盖现有文件。
