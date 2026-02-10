# 系统架构设计

## 🏗️ 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                   Bili_Stock 量化交易系统                   │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  数据采集层  │  │  AI策略层   │  │     执行风控层       │  │
│  │  Data Layer │  │  AI Layer   │  │  Execution Layer    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 📊 数据流架构

### 数据采集流程
```
B站UP主发现 → 视频数据采集 → 关键帧提取 → OCR文本识别 → 交易信号提取
```

### AI决策流程  
```
多源信号输入 → 特征工程 → 深度学习分析 → 策略生成 → 风险控制 → 交易指令
```

## 🧩 核心模块详解

### 1. 数据采集层 (Data Layer)

#### BiliCollector - B站数据采集
```python
class BiliCollector:
    def discover_ups(self):
        """自动发现实盘UP主"""
        
    def collect_videos(self):
        """采集UP主视频数据"""
        
    def extract_keyframes(self):
        """提取视频关键帧"""
```

#### 多平台数据源（开发中）
- **XueQiuCollector**: 雪球投资观点采集
- **EastMoneyCollector**: 东方财富实时数据
- **NewsCollector**: 财经新闻情感分析

### 2. AI策略层 (AI Layer)

#### EvolutionaryTradingAI - 进化交易AI
```python
class EvolutionaryTradingAI:
    def evolve_strategies(self):
        """遗传算法策略进化"""
        
    def generate_signals(self):
        """生成交易信号"""
        
    def optimize_parameters(self):
        """参数优化"""
```

#### 深度学习模块
- **DeepSeekAnalyzer**: 大语言模型分析
- **VideoAnalyzer**: 视频内容分析
- **SentimentAnalyzer**: 市场情绪分析

### 3. 执行风控层 (Execution Layer)

#### IntradayTrader - 日内交易执行
```python
class IntradayTrader:
    def execute_trades(self):
        """执行交易指令"""
        
    def monitor_positions(self):
        """持仓监控"""
        
    def manage_orders(self):
        """订单管理"""
```

#### RiskEngine - 风险控制
```python
class RiskEngine:
    def validate_signals(self):
        """信号风险验证"""
        
    def calculate_position_size(self):
        """仓位计算"""
        
    def enforce_stop_loss(self):
        """止损执行"""
```

## 🎯 交易策略架构

### 小资金激进打板策略

#### 选股逻辑
```python
def select_dragon_stocks():
    # 1. 连板数量 ≥ 2
    # 2. 封单金额 > 5000万  
    # 3. 板块热度前3
    # 4. 游资介入明显
    # 5. 市场情绪积极
```

#### 风控规则
```python
def risk_management_rules():
    # 单票最大仓位: 50%
    # 单日最大回撤: 3% 
    # 单笔止损: -8%
    # 空仓条件: 市场环境恶劣
```

## 🔄 工作流程

### 每日运行流程
1. **数据更新** (09:00) - 采集前日数据
2. **策略生成** (09:15) - AI生成当日策略
3. **盘中监控** (09:30-15:00) - 实时信号监控
4. **盘后分析** (15:00) - 绩效分析策略优化

### 回测验证流程
1. **历史数据** - 加载3年历史数据
2. **策略回测** - 多参数组合测试
3. **绩效评估** - 夏普比率、最大回撤等
4. **前向验证** - 样本外测试验证

## 📦 技术栈

### 核心框架
- **Python 3.12** - 主开发语言
- **Pandas/Numpy** - 数据处理
- **PyTorch** - 深度学习
- **FastAPI** - Web接口（可选）

### 数据采集
- **aiohttp** - 异步HTTP请求
- **OpenCV** - 视频处理
- **PaddleOCR** - 文字识别

### 数据分析
- **TA-Lib** - 技术指标
- **Scikit-learn** - 机器学习
- **Statsmodels** - 统计分析

## 🚀 扩展性设计

### 插件式架构
```python
# 策略插件接口
class StrategyPlugin:
    def generate_signals(self, data):
        pass
    
    def get_parameters(self):
        pass
```

### 数据源插件
```python
# 数据源插件接口  
class DataSourcePlugin:
    def fetch_data(self):
        pass
    
    def normalize_data(self):
        pass
```

## 🔒 安全架构

### 数据安全
- 敏感配置外部化
- API密钥加密存储
- 数据传输加密

### 系统安全
- 交易指令双重验证
- 风控规则硬性执行
- 异常行为监控报警

---

*最后更新: 2026-02-10*