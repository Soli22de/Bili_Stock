# GPT执行指令：实时BaoStock盘中集成方案

## 第一步：创建实时行情获取模块

**指令**:
```
请创建文件 c:/jz_code/Bili_Stock/core/realtime_baostock.py
实现以下功能：

import pandas as pd
import baostock as bs
from datetime import datetime, timedelta
from typing import List, Optional
import logging

class RealTimeBaoStockFetcher:
    """实时BaoStock行情获取器"""
    
    def __init__(self):
        self.bs = None
        self._login()
    
    def _login(self) -> bool:
        """登录BaoStock，返回是否成功"""
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self.bs = bs
                logging.info("BaoStock登录成功")
                return True
            else:
                logging.error(f"BaoStock登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logging.error(f"BaoStock登录异常: {e}")
            return False
    
    def logout(self):
        """登出BaoStock"""
        if self.bs:
            bs.logout()
    
    def get_realtime_quotes(self, codes: List[str]) -> pd.DataFrame:
        """
        获取实时报价数据
        Returns DataFrame with columns: ['code', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        """
        if not self.bs:
            return pd.DataFrame()
        
        bs_codes = [self._to_bs_code(code) for code in codes]
        rs = self.bs.query_stock_quote(bs_codes)
        
        if rs.error_code == '0':
            df = rs.get_data()
            # 重命名列以保持一致性
            df = df.rename(columns={
                'code': 'stock_code',
                'tradeStatus': 'status'
            })
            return df
        else:
            logging.error(f"获取实时报价失败: {rs.error_msg}")
            return pd.DataFrame()
    
    def get_minute_data(self, code: str, frequency: str = "5", 
                       lookback_hours: int = 2) -> pd.DataFrame:
        """
        获取分钟线数据
        frequency: 1, 5, 15, 30, 60分钟
        Returns DataFrame with columns: ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        """
        if not self.bs:
            return pd.DataFrame()
        
        bs_code = self._to_bs_code(code)
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time = (datetime.now() - timedelta(hours=lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")
        
        rs = self.bs.query_history_k_data_plus(
            bs_code,
            "date,time,code,open,high,low,close,volume,amount,turn",
            start_date=start_time,
            end_date=end_time,
            frequency=frequency,
            adjustflag="3"
        )
        
        if rs.error_code == '0':
            df = rs.get_data()
            # 转换数据类型
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        else:
            logging.error(f"获取分钟数据失败 {code}: {rs.error_msg}")
            return pd.DataFrame()
    
    def _to_bs_code(self, code: str) -> str:
        """转换为BaoStock格式代码"""
        code = str(code).zfill(6)
        if code.startswith('6'): return f"sh.{code}"
        if code.startswith('0') or code.startswith('3'): return f"sz.{code}"
        if code.startswith('8') or code.startswith('4'): return f"bj.{code}"
        return f"sh.{code}"
    
    def __del__(self):
        self.logout()
```

要求：
1. 完整的错误处理和日志记录
2. 数据类型正确转换
3. 列名标准化
4. 资源清理（自动登出）
```

## 第二步：创建盘中信号验证器

**指令**:
```
请创建文件 c:/jz_code/Bili_Stock/core/intraday_validator.py
实现以下功能：

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List
import logging
from core.realtime_baostock import RealTimeBaoStockFetcher

class IntradaySignalValidator:
    """盘中信号实时验证器"""
    
    def __init__(self):
        self.fetcher = RealTimeBaoStockFetcher()
        self.validation_rules = self._get_default_rules()
    
    def validate_signals(self, signals: List[Dict]) -> List[Dict]:
        """批量验证信号列表"""
        validated_signals = []
        
        for signal in signals:
            try:
                validated_signal = self.validate_single_signal(signal)
                validated_signals.append(validated_signal)
            except Exception as e:
                logging.error(f"信号验证失败 {signal.get('stock_code', 'unknown')}: {e}")
                # 保持原信号，但标记验证失败
                signal['real_time_validated'] = False
                signal['validation_error'] = str(e)
                validated_signals.append(signal)
        
        return validated_signals
    
    def validate_single_signal(self, signal: Dict) -> Dict:
        """验证单个信号"""
        code = signal.get('stock_code')
        if not code:
            return {**signal, 'real_time_validated': False, 'validation_error': 'No stock code'}
        
        # 获取实时数据
        realtime_data = self.fetcher.get_realtime_quotes([code])
        minute_data = self.fetcher.get_minute_data(code, "5")
        
        # 应用验证规则
        validation_result = self._apply_validation_rules(signal, realtime_data, minute_data)
        
        # 构建验证后的信号
        validated_signal = {
            **signal,
            'real_time_validated': True,
            'validation_score': validation_result['score'],
            'adjusted_strength': signal.get('strength', 0) * validation_result['score'],
            'last_validation_time': datetime.now().isoformat(),
            'realtime_price': validation_result.get('current_price'),
            'price_change_pct': validation_result.get('price_change_pct'),
            'volume_ratio': validation_result.get('volume_ratio'),
            'validation_details': validation_result.get('details', {})
        }
        
        return validated_signal
    
    def _apply_validation_rules(self, signal: Dict, realtime_data: pd.DataFrame, 
                               minute_data: pd.DataFrame) -> Dict:
        """应用验证规则集合"""
        score = 1.0  # 初始分数
        details = {}
        
        # 规则1: 价格方向验证
        price_validation = self._validate_price_direction(signal, realtime_data)
        score *= price_validation['score']
        details['price_validation'] = price_validation
        
        # 规则2: 成交量验证
        volume_validation = self._validate_volume(signal, minute_data)
        score *= volume_validation['score']
        details['volume_validation'] = volume_validation
        
        # 规则3: 时间衰减
        time_decay = self._apply_time_decay(signal)
        score *= time_decay['score']
        details['time_decay'] = time_decay
        
        # 计算当前价格和涨跌幅
        current_price, price_change_pct = self._get_current_price_info(realtime_data)
        
        return {
            'score': min(max(score, 0.3), 2.0),  # 限制在0.3-2.0范围内
            'current_price': current_price,
            'price_change_pct': price_change_pct,
            'details': details
        }
    
    def _validate_price_direction(self, signal: Dict, realtime_data: pd.DataFrame) -> Dict:
        """验证价格方向一致性"""
        if realtime_data.empty:
            return {'score': 1.0, 'reason': 'no_data'}
        
        current_price = float(realtime_data.iloc[0]['close'])
        open_price = float(realtime_data.iloc[0]['open'])
        
        # 判断实际价格方向
        actual_direction = 1 if current_price > open_price else (-1 if current_price < open_price else 0)
        
        # 判断预期方向 (BUY=1, SELL=-1)
        expected_direction = 1 if signal.get('signal_type') == 'BUY' else (-1 if signal.get('signal_type') == 'SELL' else 0)
        
        if expected_direction == actual_direction:
            return {'score': 1.2, 'reason': 'direction_match'}
        elif actual_direction == 0:
            return {'score': 1.0, 'reason': 'no_change'}
        else:
            return {'score': 0.8, 'reason': 'direction_mismatch'}
    
    def _validate_volume(self, signal: Dict, minute_data: pd.DataFrame) -> Dict:
        """验证成交量"""
        if minute_data.empty or len(minute_data) < 10:
            return {'score': 1.0, 'reason': 'insufficient_data'}
        
        # 计算成交量比率 (最近5分钟平均 vs 前1小时平均)
        recent_volume = minute_data['volume'].tail(5).mean()
        historical_volume = minute_data['volume'].mean()
        
        if historical_volume == 0:
            return {'score': 1.0, 'reason': 'zero_volume'}
        
        volume_ratio = recent_volume / historical_volume
        
        if volume_ratio > 2.0:
            return {'score': 1.3, 'reason': 'high_volume', 'ratio': volume_ratio}
        elif volume_ratio > 1.5:
            return {'score': 1.1, 'reason': 'moderate_volume', 'ratio': volume_ratio}
        elif volume_ratio < 0.5:
            return {'score': 0.7, 'reason': 'low_volume', 'ratio': volume_ratio}
        else:
            return {'score': 1.0, 'reason': 'normal_volume', 'ratio': volume_ratio}
    
    def _apply_time_decay(self, signal: Dict) -> Dict:
        """应用时间衰减"""
        signal_time = pd.to_datetime(signal.get('timestamp', datetime.now()))
        time_diff_hours = (datetime.now() - signal_time).total_seconds() / 3600
        
        # 衰减曲线: 1小时内不衰减，之后每小时衰减5%
        if time_diff_hours <= 1:
            return {'score': 1.0, 'hours_ago': time_diff_hours}
        else:
            decay = max(0.7, 1 - (time_diff_hours - 1) * 0.05)
            return {'score': decay, 'hours_ago': time_diff_hours}
    
    def _get_current_price_info(self, realtime_data: pd.DataFrame) -> tuple:
        """获取当前价格信息"""
        if realtime_data.empty:
            return None, None
        
        current_price = float(realtime_data.iloc[0]['close'])
        open_price = float(realtime_data.iloc[0]['open'])
        
        if open_price == 0:
            return current_price, None
        
        price_change_pct = ((current_price - open_price) / open_price) * 100
        return current_price, price_change_pct
    
    def _get_default_rules(self) -> Dict:
        """获取默认验证规则"""
        return {
            'price_direction_weight': 1.0,
            'volume_weight': 0.8,
            'time_decay_weight': 0.5,
            'min_validation_score': 0.3,
            'max_validation_score': 2.0
        }
    
    def __del__(self):
        if hasattr(self, 'fetcher'):
            self.fetcher.logout()
```

要求：
1. 完整的验证规则体系
2. 详细的验证详情记录
3. 优雅的错误降级
4. 资源清理
```

## 第三步：集成到信号提取流程

**指令**:
```
请修改 c:/jz_code/Bili_Stock/core/extract_signals.py：

1. 在文件顶部添加导入：
```python
from core.intraday_validator import IntradaySignalValidator
import config
```

2. 在 SignalExtractor 类的 __init__ 方法中添加：
```python
def __init__(self, stock_map_path=config.STOCK_MAP_PATH):
    # 现有代码...
    
    # 初始化实时验证器
    self.validator = None
    if config.ENABLE_REALTIME_VALIDATION:
        try:
            self.validator = IntradaySignalValidator()
            print("实时BaoStock验证器初始化成功")
        except Exception as e:
            print(f"实时验证器初始化失败: {e}")
            self.validator = None
```

3. 在 extract_signals 方法末尾添加实时验证：
```python
def extract_signals(self, df_videos: pd.DataFrame) -> pd.DataFrame:
    # 现有信号提取逻辑...
    
    # 实时验证增强
    if (self.validator is not None and 
        config.ENABLE_REALTIME_VALIDATION and 
        not df_signals.empty):
        
        print(f"开始实时验证 {len(df_signals)} 个信号...")
        
        # 转换为字典列表进行验证
        signals_list = df_signals.to_dict('records')
        validated_signals = self.validator.validate_signals(signals_list)
        
        # 转换回DataFrame
        df_signals = pd.DataFrame(validated_signals)
        
        # 统计验证结果
        validated_count = sum(1 for s in validated_signals if s.get('real_time_validated', False))
        print(f"实时验证完成: {validated_count}/{len(validated_signals)} 个信号验证成功")
    
    return df_signals
```

4. 添加资源清理方法：
```python
def close(self):
    """清理资源"""
    if self.validator is not None:
        self.validator.__del__()
```

要求：
1. 配置开关控制 (ENABLE_REALTIME_VALIDATION)
2. 完整的错误处理
3. 验证统计信息
4. 资源清理
```

## 第四步：更新配置和测试

**指令**:
```
请更新 c:/jz_code/Bili_Stock/config.py：

1. 添加实时验证配置：
```python
# 实时BaoStock验证配置
ENABLE_REALTIME_VALIDATION = True  # 是否启用实时验证
REALTIME_VALIDATION_INTERVAL = 300  # 验证间隔(秒)
MIN_VALIDATION_SCORE = 0.5  # 最小验证分数阈值
MAX_VALIDATION_SCORE = 1.5  # 最大验证分数阈值
```

2. 创建测试脚本 c:/jz_code/Bili_Stock/scripts/test_realtime_validation.py：
```python
#!/usr/bin/env python3
"""测试实时BaoStock验证功能"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta
from core.intraday_validator import IntradaySignalValidator

def test_realtime_validation():
    """测试实时验证功能"""
    print("测试实时BaoStock验证功能...")
    
    # 创建测试信号
    test_signals = [
        {
            'stock_code': '600036',  # 招商银行
            'signal_type': 'BUY',
            'strength': 0.8,
            'timestamp': datetime.now().isoformat(),
            'reason': '测试买入信号'
        },
        {
            'stock_code': '000001',  # 平安银行
            'signal_type': 'SELL', 
            'strength': 0.6,
            'timestamp': (datetime.now() - timedelta(hours=2)).isoformat(),
            'reason': '测试卖出信号'
        }
    ]
    
    # 初始化验证器
    validator = IntradaySignalValidator()
    
    # 执行验证
    validated_signals = validator.validate_signals(test_signals)
    
    # 输出结果
    print(f"验证完成: {len(validated_signals)} 个信号")
    for i, signal in enumerate(validated_signals):
        print(f"\n信号 {i+1}:")
        print(f"  股票: {signal.get('stock_code')}")
        print(f"  类型: {signal.get('signal_type')}")
        print(f"  原强度: {signal.get('strength')}")
        print(f"  验证后强度: {signal.get('adjusted_strength')}")
        print(f"  验证分数: {signal.get('validation_score')}")
        print(f"  实时价格: {signal.get('realtime_price')}")
        print(f"  涨跌幅: {signal.get('price_change_pct')}%")
        print(f"  验证状态: {signal.get('real_time_validated')}")
    
    print("\n测试完成!")

if __name__ == "__main__":
    test_realtime_validation()
```

3. 运行测试验证功能：
```bash
cd c:/jz_code/Bili_Stock
python scripts/test_realtime_validation.py
```

要求：
1. 完整的配置选项
2. 详细的测试脚本
3. 验证功能正常工作
```

## 执行顺序

请按以下顺序执行上述指令：
1. 第一步：创建实时行情获取模块
2. 第二步：创建盘中信号验证器  
3. 第三步：集成到信号提取流程
4. 第四步：更新配置和测试

每完成一步后请验证功能正常再继续下一步。