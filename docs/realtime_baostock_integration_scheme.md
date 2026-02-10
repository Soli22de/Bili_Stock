# 实时BaoStock盘中观察集成方案

## 设计目标
为买入卖出信号提供实时BaoStock盘中观察能力，完全替代Tushare Pro依赖，实现盘中实时数据验证和信号强度动态调整。

## 核心模块设计

### 1. 实时行情获取模块 (RealTimeBaoStockFetcher)
```python
class RealTimeBaoStockFetcher:
    """实时BaoStock行情获取器 - 支持分时、分钟线、实时报价"""
    
    def __init__(self):
        self.bs = None
        self._login()
    
    def _login(self):
        """登录BaoStock"""
        import baostock as bs
        lg = bs.login()
        if lg.error_code == '0':
            self.bs = bs
            print("BaoStock实时行情服务已启动")
        else:
            raise Exception(f"BaoStock登录失败: {lg.error_msg}")
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """获取实时报价数据"""
        bs_codes = [self._to_bs_code(code) for code in codes]
        rs = self.bs.query_stock_quote(bs_codes)
        return rs.get_data()
    
    def get_minute_data(self, code: str, frequency: str = "5") -> pd.DataFrame:
        """获取分钟线数据 (1, 5, 15, 30, 60分钟)"""
        bs_code = self._to_bs_code(code)
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        
        rs = self.bs.query_history_k_data_plus(
            bs_code,
            "date,time,code,open,high,low,close,volume,amount,turn",
            start_date=start_time,
            end_date=end_time,
            frequency=frequency,
            adjustflag="3"
        )
        return rs.get_data()
    
    def _to_bs_code(self, code: str) -> str:
        """转换为BaoStock格式代码"""
        code = str(code).zfill(6)
        if code.startswith('6'): return f"sh.{code}"
        if code.startswith('0') or code.startswith('3'): return f"sz.{code}"
        if code.startswith('8') or code.startswith('4'): return f"bj.{code}"
        return f"sh.{code}"
```

### 2. 盘中信号验证器 (IntradaySignalValidator)
```python
class IntradaySignalValidator:
    """盘中信号实时验证器 - 基于BaoStock数据动态调整信号强度"""
    
    def __init__(self):
        self.fetcher = RealTimeBaoStockFetcher()
        self.validation_rules = self._load_validation_rules()
    
    def validate_signal(self, signal: Dict, current_time: datetime) -> Dict:
        """验证单个信号并返回增强后的信号"""
        code = signal['stock_code']
        
        try:
            # 获取实时数据
            realtime_data = self.fetcher.get_realtime_quotes([code])
            minute_data = self.fetcher.get_minute_data(code, "5")
            
            # 应用验证规则
            validation_score = self._apply_validation_rules(signal, realtime_data, minute_data)
            
            # 动态调整信号强度
            adjusted_strength = signal.get('strength', 0) * validation_score
            
            return {
                **signal,
                'real_time_validated': True,
                'validation_score': validation_score,
                'adjusted_strength': adjusted_strength,
                'last_validation_time': current_time,
                'realtime_price': realtime_data.iloc[0]['close'] if not realtime_data.empty else None,
                'price_change_pct': self._calculate_price_change(signal, realtime_data)
            }
            
        except Exception as e:
            print(f"信号验证失败 {code}: {e}")
            return {**signal, 'real_time_validated': False, 'validation_error': str(e)}
    
    def _apply_validation_rules(self, signal, realtime_data, minute_data) -> float:
        """应用盘中验证规则"""
        score = 1.0  # 初始分数
        
        # 规则1: 价格变动验证
        if not realtime_data.empty:
            current_price = float(realtime_data.iloc[0]['close'])
            expected_direction = 1 if signal['signal_type'] == 'BUY' else -1
            actual_direction = 1 if current_price > float(realtime_data.iloc[0]['open']) else -1
            
            if expected_direction == actual_direction:
                score *= 1.2  # 方向一致，增强信号
            else:
                score *= 0.8  # 方向不一致，减弱信号
        
        # 规则2: 成交量验证
        if not minute_data.empty:
            avg_volume = minute_data['volume'].mean()
            last_volume = minute_data['volume'].iloc[-1]
            
            if last_volume > avg_volume * 1.5:
                score *= 1.1  # 放量，增强信号
        
        # 规则3: 时间衰减 (离信号产生时间越久，分数越低)
        signal_time = pd.to_datetime(signal['timestamp'])
        time_diff = (datetime.now() - signal_time).total_seconds() / 3600  # 小时数
        time_decay = max(0.7, 1 - (time_diff * 0.1))  # 每小时衰减10%，最低0.7
        score *= time_decay
        
        return min(max(score, 0.5), 1.5)  # 限制在0.5-1.5范围内
```

### 3. 信号集成层 (extract_signals.py 增强)
```python
# 在 extract_signals.py 中添加实时验证集成

def extract_signals_with_realtime_validation(df_videos: pd.DataFrame) -> pd.DataFrame:
    """提取信号并集成实时BaoStock验证"""
    # 原有信号提取逻辑
    df_signals = extract_signals(df_videos)
    
    # 集成实时验证
    if config.ENABLE_REALTIME_VALIDATION:
        validator = IntradaySignalValidator()
        validated_signals = []
        
        for _, signal in df_signals.iterrows():
            validated_signal = validator.validate_signal(signal.to_dict(), datetime.now())
            validated_signals.append(validated_signal)
        
        df_signals = pd.DataFrame(validated_signals)
    
    return df_signals
```

## 执行方案 - GPT可执行指令

### 第一步：创建实时行情获取模块
**指令给GPT**: 
```
请创建文件 c:/jz_code/Bili_Stock/core/realtime_baostock.py
实现 RealTimeBaoStockFetcher 类，包含：
1. BaoStock登录/登出管理
2. 实时报价获取方法 get_realtime_quotes()
3. 分钟线数据获取方法 get_minute_data()
4. 代码格式转换工具方法
要求：完全使用BaoStock API，零Tushare依赖
```

### 第二步：创建盘中信号验证器
**指令给GPT**:
```
请创建文件 c:/jz_code/Bili_Stock/core/intraday_validator.py
实现 IntradaySignalValidator 类，包含：
1. 基于BaoStock数据的实时信号验证
2. 多重验证规则（价格方向、成交量、时间衰减）
3. 信号强度动态调整逻辑
4. 错误处理和降级机制
```

### 第三步：集成到信号提取流程
**指令给GPT**:
```
请修改 c:/jz_code/Bili_Stock/core/extract_signals.py：
1. 在文件顶部添加导入: from core.intraday_validator import IntradaySignalValidator
2. 在 SignalExtractor 类的 __init__ 方法中初始化验证器
3. 在 extract_signals 方法中添加实时验证开关
4. 确保所有信号都经过BaoStock实时验证
```

### 第四步：配置和测试
**指令给GPT**:
```
请更新 c:/jz_code/Bili_Stock/config.py：
1. 添加 ENABLE_REALTIME_VALIDATION = True
2. 添加 REALTIME_VALIDATION_INTERVAL = 300 (5分钟)
3. 创建测试脚本 test_realtime_integration.py 验证功能
```

## 预期收益

1. **实时性提升**: 盘中信号可实时获取BaoStock行情验证
2. **准确性提高**: 动态调整信号强度，减少虚假信号
3. **依赖净化**: 完全去除Tushare Pro依赖
4. **风险控制**: 盘中实时风控，避免追高杀跌

## 技术约束
- ✅ 仅使用BaoStock (无Tushare Pro)
- ✅ 支持OCR和LLM集成
- ✅ 保持现有信号格式兼容
- ✅ 错误降级机制确保系统稳定性