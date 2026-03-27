# 本机常驻 A 股盯盘调度

## 为什么不用 GitHub Actions 定时

GitHub Actions 的 `schedule` 不是硬实时调度，可能出现分钟级延迟，极端情况下会晚很多。
对于 `09:30` 竞价报告和 `15:00` 收盘报告，这种延迟会直接破坏时效性。

## 适用场景

当你希望：

- `09:30` 报告在 `09:30` 前后就送达
- 盘中其他时段尽量在 `10` 分钟内送达
- `15:00` 收盘报告在 `15:10` 前送达

建议改用本机或自有服务器常驻运行。

## 启动命令

```bash
cd /Users/null/Documents/Playground/daily_stock_analysis_repo
./scripts/run-intraday-market-daemon-macos.sh
```

可选参数：

```bash
./scripts/run-intraday-market-daemon-macos.sh \
  --intraday-grace-minutes 10 \
  --intraday-poll-seconds 20
```

## 当前内置触发策略

本机常驻调度器按本地时间触发，但报告标签仍保持业务时点：

- `09:24` 触发，生成 `09:30` 报告
- `10:30` 触发，生成 `10:30` 报告
- `11:30` 触发，生成 `11:30` 报告
- `13:30` 触发，生成 `13:30` 报告
- `14:30` 触发，生成 `14:30` 报告
- `15:03` 触发，生成 `15:00` 收盘报告

## 本地环境要求

至少需要这些环境变量：

- `GEMINI_API_KEY`
- `EMAIL_SENDER`
- `EMAIL_PASSWORD`
- `EMAIL_RECEIVERS`
- `EMAIL_ATTACHMENT_FORMAT=pdf`

如果你本机没有 `.env`，可以先从 `.env.example` 复制一份：

```bash
cp .env.example .env
```

然后只填写本机常驻盯盘所需的最小字段。

## 说明

- 守护器只负责“什么时候执行”，不会改变现有 PDF 报告内容。
- 守护器支持 `10` 分钟内的迟到补救窗口。
- 如果进程在某个时段启动较晚，但仍在补救窗口内，会自动补发对应时段报告。

## macOS 常驻方式

如果你希望这台 Mac 登录后自动常驻，可以配合 `launchd`：

1. 将仓库准备好 Python 运行环境，例如 `.venv311`
2. 确保 `.env` 已填写邮件和 Gemini 配置
3. 创建 `~/Library/LaunchAgents/com.daily_stock_analysis.intraday.plist`
4. 执行：

```bash
launchctl bootout "gui/$(id -u)" ~/Library/LaunchAgents/com.daily_stock_analysis.intraday.plist 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.daily_stock_analysis.intraday.plist
launchctl kickstart -k "gui/$(id -u)/com.daily_stock_analysis.intraday"
```

查看状态：

```bash
launchctl print "gui/$(id -u)/com.daily_stock_analysis.intraday"
```
