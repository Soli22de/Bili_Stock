# Bili_Stock - AI量化交易系统

## 🎯 项目愿景
基于B站实盘UP主数据的AI驱动量化交易系统，专注于小资金激进打板策略，实现AI自我进化的交易决策。

## ✨ 核心特性
- **多平台数据融合**：B站实盘视频 + 雪球投资观点 + 东方财富实时数据
- **AI自我进化**：遗传算法优化交易策略，强化学习实时调整
- **实时打板识别**：龙头股识别、情绪分析、资金流向追踪
- **深度学习风控**：多维度风险控制，动态止损机制

## 🚀 快速开始

### 环境要求
- Python 3.12+
- GPU支持（可选，用于深度学习加速）

### 安装依赖
```bash
pip install -r requirements.txt
```

### 数据收集
```bash
# 收集B站UP主数据
python scripts/discover_active_trading_ups.py --collect

# 分析UP主交易信号
python scripts/discover_active_trading_ups.py --analyze
```

### 运行回测
```bash
python core/backtest_engine.py
```

## 📊 系统架构

### 数据层
- `BiliCollector`: B站实盘视频数据采集
- `XueQiuCollector`: 雪球投资观点采集（开发中）
- `EastMoneyCollector`: 东方财富实时数据（开发中）

### AI策略层
- `EvolutionaryTradingAI`: 遗传算法策略优化
- `DeepSeekAnalyzer`: DeepSeek模型集成分析
- `LimitUpAnalyzer`: 涨停板模式识别

### 执行层
- `IntradayTrader`: 日内交易执行
- `RiskEngine`: 实时风险控制
- `BacktestEngine`: 策略回测验证

## 📈 策略特点

### 小资金激进打板
- **集中持仓**：单支股票30-50%仓位
- **高频换手**：日级别持仓，追求复利增长
- **严格止损**：-8%无条件止损，单日最大回撤控制3%
- **龙头聚焦**：只做市场最强龙头股

### AI进化机制
- **遗传算法**：策略参数自动优化
- **多模态学习**：文本、视频、数据融合分析
- **实时反馈**：基于交易结果的策略调整

## 🔒 安全说明

本项目为**私有交易系统**，包含：
- 敏感API密钥配置（已.gitignore）
- 实盘交易逻辑
- 专有算法实现

**注意**：请勿公开敏感配置和实盘交易细节。

## 📝 文档目录

- [架构设计](ARCHITECTURE.md) - 详细系统架构说明
- [算法文档](docs/algorithm/) - 核心交易算法详解
- [开发指南](CONTRIBUTING.md) - 代码贡献规范
- [更新日志](CHANGELOG.md) - 版本历史记录

## 🛠 开发状态

- ✅ B站数据采集 - 已完成
- ✅ 基础回测框架 - 已完成
- 🚧 AI策略进化 - 开发中
- 📋 多平台集成 - 规划中

## 📄 许可证

私有项目 - 仅限个人使用

---

**免责声明**：股市有风险，投资需谨慎。本系统仅为技术研究用途，不构成投资建议。