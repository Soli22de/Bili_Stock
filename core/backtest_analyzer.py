#!/usr/bin/env python3
"""
回测绩效分析器 - 增强的性能评估指标
"""

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
import config

class BacktestAnalyzer:
    def __init__(self, report_path=config.BACKTEST_REPORT):
        self.report_path = report_path
        self.df = None
        self.metrics = {}
        
    def load_report(self) -> bool:
        """加载回测报告"""
        try:
            self.df = pd.read_csv(self.report_path)
            print(f"✅ 成功加载回测报告，共 {len(self.df)} 条记录")
            return True
        except FileNotFoundError:
            print("⚠️  回测报告文件未找到")
            return False
        except Exception as e:
            print(f"❌ 加载回测报告失败: {e}")
            return False
    
    def calculate_comprehensive_metrics(self) -> Dict:
        """计算全面的绩效指标"""
        if self.df is None:
            if not self.load_report():
                return {}
        
        executed_trades = self.df[self.df['status'] == 'EXECUTED']
        rejected_trades = self.df[self.df['status'] != 'EXECUTED']
        
        metrics = {
            'total_signals': len(self.df),
            'executed_trades': len(executed_trades),
            'rejected_trades': len(rejected_trades),
            'execution_rate': len(executed_trades) / len(self.df) if len(self.df) > 0 else 0
        }
        
        if len(executed_trades) > 0:
            # 基础指标
            metrics.update(self._calculate_basic_metrics(executed_trades))
            # 高级指标
            metrics.update(self._calculate_advanced_metrics(executed_trades))
            # 风险指标
            metrics.update(self._calculate_risk_metrics(executed_trades))
            # 时间分析
            metrics.update(self._calculate_time_metrics(executed_trades))
        
        # 拒绝原因分析
        metrics.update(self._analyze_rejections(rejected_trades))
        
        self.metrics = metrics
        return metrics
    
    def _calculate_basic_metrics(self, executed_trades: pd.DataFrame) -> Dict:
        """计算基础绩效指标"""
        pnl_series = executed_trades['pnl']
        
        return {
            'win_rate': (pnl_series > 0).mean(),
            'avg_pnl': pnl_series.mean(),
            'median_pnl': pnl_series.median(),
            'max_profit': pnl_series.max(),
            'max_loss': pnl_series.min(),
            'total_return': pnl_series.sum(),
            'profit_factor': abs(pnl_series[pnl_series > 0].sum() / 
                               pnl_series[pnl_series < 0].sum()) if len(pnl_series[pnl_series < 0]) > 0 else float('inf')
        }
    
    def _calculate_advanced_metrics(self, executed_trades: pd.DataFrame) -> Dict:
        """计算高级绩效指标"""
        pnl_series = executed_trades['pnl']
        
        # 夏普比率（简化版，假设无风险利率为0）
        sharpe_ratio = pnl_series.mean() / pnl_series.std() if pnl_series.std() > 0 else 0
        
        # 索提诺比率
        downside_returns = pnl_series[pnl_series < 0]
        sortino_ratio = pnl_series.mean() / downside_returns.std() if len(downside_returns) > 0 and downside_returns.std() > 0 else 0
        
        # Calmar比率（简化版）
        max_drawdown = executed_trades['max_drawdown'].min()
        calmar_ratio = pnl_series.mean() / abs(max_drawdown) if max_drawdown < 0 else 0
        
        return {
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            'expectancy': self._calculate_expectancy(executed_trades),
            'kelly_criterion': self._calculate_kelly_criterion(executed_trades)
        }
    
    def _calculate_risk_metrics(self, executed_trades: pd.DataFrame) -> Dict:
        """计算风险指标"""
        return {
            'max_drawdown': executed_trades['max_drawdown'].min(),
            'avg_drawdown': executed_trades['max_drawdown'].mean(),
            'win_loss_ratio': abs(executed_trades['pnl'][executed_trades['pnl'] > 0].mean() / 
                                 executed_trades['pnl'][executed_trades['pnl'] < 0].mean()) if len(executed_trades[executed_trades['pnl'] < 0]) > 0 else float('inf'),
            'volatility': executed_trades['pnl'].std(),
            'value_at_risk_95': np.percentile(executed_trades['pnl'], 5)
        }
    
    def _calculate_time_metrics(self, executed_trades: pd.DataFrame) -> Dict:
        """计算时间相关指标"""
        return {
            'avg_holding_days': executed_trades['holding_days'].mean(),
            'median_holding_days': executed_trades['holding_days'].median(),
            'max_holding_days': executed_trades['holding_days'].max(),
            'trades_per_month': len(executed_trades) / 3  # 假设3个月
        }
    
    def _calculate_expectancy(self, executed_trades: pd.DataFrame) -> float:
        """计算期望值"""
        winning_trades = executed_trades[executed_trades['pnl'] > 0]
        losing_trades = executed_trades[executed_trades['pnl'] < 0]
        
        win_rate = len(winning_trades) / len(executed_trades)
        avg_win = winning_trades['pnl'].mean() if len(winning_trades) > 0 else 0
        avg_loss = losing_trades['pnl'].mean() if len(losing_trades) > 0 else 0
        
        return (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))
    
    def _calculate_kelly_criterion(self, executed_trades: pd.DataFrame) -> float:
        """计算凯利准则"""
        win_rate = (executed_trades['pnl'] > 0).mean()
        avg_win = executed_trades[executed_trades['pnl'] > 0]['pnl'].mean()
        avg_loss = executed_trades[executed_trades['pnl'] < 0]['pnl'].mean()
        
        if avg_win > 0 and avg_loss < 0:
            return win_rate - ((1 - win_rate) / (avg_win / abs(avg_loss)))
        return 0
    
    def _analyze_rejections(self, rejected_trades: pd.DataFrame) -> Dict:
        """分析拒绝原因"""
        if len(rejected_trades) == 0:
            return {}
        
        rejection_reasons = rejected_trades['status'].value_counts().to_dict()
        
        # 解析具体原因
        detailed_reasons = {}
        for status, count in rejection_reasons.items():
            if status != 'EXECUTED':
                detailed_reasons[status] = count
        
        return {
            'rejection_breakdown': detailed_reasons,
            'main_rejection_reason': max(detailed_reasons, key=detailed_reasons.get) if detailed_reasons else 'None'
        }
    
    def generate_detailed_report(self) -> str:
        """生成详细文本报告"""
        if not self.metrics:
            self.calculate_comprehensive_metrics()
        
        report_lines = [
            "=" * 60,
            "📊 OCR策略回测详细报告",
            "=" * 60,
            f"报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "📈 绩效概览:",
            f"   总信号数: {self.metrics.get('total_signals', 0):,}",
            f"   执行交易: {self.metrics.get('executed_trades', 0):,} ({self.metrics.get('execution_rate', 0):.1%})",
            f"   胜率: {self.metrics.get('win_rate', 0):.2%}",
            f"   平均收益: {self.metrics.get('avg_pnl', 0):.2f}%",
            f"   总收益: {self.metrics.get('total_return', 0):.2f}%",
            "",
            "🎯 风险调整后收益:",
            f"   夏普比率: {self.metrics.get('sharpe_ratio', 0):.2f}",
            f"   索提诺比率: {self.metrics.get('sortino_ratio', 0):.2f}",
            f"   卡尔玛比率: {self.metrics.get('calmar_ratio', 0):.2f}",
            "",
            "⚠️  风险指标:",
            f"   最大回撤: {self.metrics.get('max_drawdown', 0):.2f}%",
            f"   平均回撤: {self.metrics.get('avg_drawdown', 0):.2f}%",
            f"   波动率: {self.metrics.get('volatility', 0):.2f}%",
            f"   95% VaR: {self.metrics.get('value_at_risk_95', 0):.2f}%",
            "",
            "⏰ 时间分析:",
            f"   平均持有天数: {self.metrics.get('avg_holding_days', 0):.1f}",
            f"   月均交易数: {self.metrics.get('trades_per_month', 0):.1f}",
            "",
            "🔍 拒绝分析:",
        ]
        
        # 添加拒绝原因
        rejection_breakdown = self.metrics.get('rejection_breakdown', {})
        for reason, count in rejection_breakdown.items():
            report_lines.append(f"   {reason}: {count}次")
        
        report_lines.extend([
            "",
            "💡 策略建议:",
            self._generate_strategy_advice(),
            "=" * 60
        ])
        
        return "\n".join(report_lines)
    
    def _generate_strategy_advice(self) -> str:
        """生成策略建议"""
        if not self.metrics or self.metrics.get('executed_trades', 0) == 0:
            return "数据不足，无法提供建议"
        
        advice = []
        
        # 基于胜率建议
        win_rate = self.metrics.get('win_rate', 0)
        if win_rate > 0.6:
            advice.append("✅ 胜率优秀，可以考虑增加仓位")
        elif win_rate < 0.4:
            advice.append("❌ 胜率偏低，需要优化选股标准")
        
        # 基于夏普比率建议
        sharpe = self.metrics.get('sharpe_ratio', 0)
        if sharpe > 1.0:
            advice.append("✅ 夏普比率良好，风险调整后收益优秀")
        elif sharpe < 0.5:
            advice.append("⚠️  夏普比率偏低，考虑降低波动性")
        
        # 基于回撤建议
        max_dd = self.metrics.get('max_drawdown', 0)
        if max_dd < -10:
            advice.append("❌ 最大回撤过大，需要加强风险控制")
        
        # 基于凯利准则建议
        kelly = self.metrics.get('kelly_criterion', 0)
        if kelly > 0.2:
            advice.append(f"✅ 凯利准则建议仓位: {kelly:.1%}")
        
        return "\n   ".join(advice) if advice else "策略表现中性，建议继续观察"
    
    def visualize_performance(self, save_path=None):
        """可视化绩效指标"""
        if self.df is None or len(self.df) == 0:
            print("没有数据可供可视化")
            return
        
        executed_trades = self.df[self.df['status'] == 'EXECUTED']
        if len(executed_trades) == 0:
            print("没有执行的交易可供可视化")
            return
        
        # 设置绘图风格
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. 收益分布直方图
        axes[0, 0].hist(executed_trades['pnl'], bins=20, alpha=0.7, color='skyblue')
        axes[0, 0].axvline(0, color='red', linestyle='--', alpha=0.8)
        axes[0, 0].set_title('收益分布')
        axes[0, 0].set_xlabel('收益率 (%)')
        axes[0, 0].set_ylabel('频次')
        
        # 2. 持有天数分布
        axes[0, 1].hist(executed_trades['holding_days'], bins=15, alpha=0.7, color='lightgreen')
        axes[0, 1].set_title('持有天数分布')
        axes[0, 1].set_xlabel('持有天数')
        axes[0, 1].set_ylabel('频次')
        
        # 3. 回撤分布
        axes[1, 0].hist(executed_trades['max_drawdown'], bins=15, alpha=0.7, color='lightcoral')
        axes[1, 0].set_title('最大回撤分布')
        axes[1, 0].set_xlabel('回撤幅度 (%)')
        axes[1, 0].set_ylabel('频次')
        
        # 4. 拒绝原因饼图
        if len(self.df) > len(executed_trades):
            rejected = self.df[self.df['status'] != 'EXECUTED']
            rejection_counts = rejected['status'].value_counts()
            axes[1, 1].pie(rejection_counts.values, labels=rejection_counts.index, 
                          autopct='%1.1f%%', startangle=90)
            axes[1, 1].set_title('拒绝原因分布')
        else:
            axes[1, 1].text(0.5, 0.5, '无拒绝交易', ha='center', va='center', 
                          transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('拒绝原因分布')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存到: {save_path}")
        
        plt.show()

def test_analysis():
    """测试分析功能"""
    analyzer = BacktestAnalyzer()
    
    if analyzer.load_report():
        metrics = analyzer.calculate_comprehensive_metrics()
        print("📊 计算出的指标:")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"{key}: {value:.4f}")
            else:
                print(f"{key}: {value}")
        
        print("\n" + "="*60)
        report = analyzer.generate_detailed_report()
        print(report)
        
        # 可视化
        analyzer.visualize_performance(save_path='data/backtest_visualization.png')

if __name__ == "__main__":
    test_analysis()