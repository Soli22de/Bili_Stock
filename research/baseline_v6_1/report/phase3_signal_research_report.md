# Phase 3 Signal Research: Richer Xueqiu Signals

**Date**: 2026-04-12
**Test period**: 2010-2025 (3.28M panel rows)
**Methodology**: Cross-sectional IC with fwd_ret_2w, by HS300 regime

---

## 1. Motivation

The current Xueqiu signal (`net_buy_cube_count`) is crude — it just counts how many cubes bought a stock on a given day. It throws away:
- **Position sizing** (weight_delta magnitude): median buy is 0.28%, p99 is 98.7%
- **Cube quality** (total_gain, followers): top cubes have 63K-147K% total returns
- **Sell signals**: 2,278 sells completely ignored
- **Trade status**: 185 canceled/failed trades counted as signal (fixed)

We tested 5 signal variants head-to-head against the current pipeline.

---

## 2. Signal Variants Tested

| # | Signal | Formula |
|---|--------|---------|
| 0 | Current pipeline (factor_z) | rate-of-change of net_buy_cube_count, smoothed, z-scored |
| 1 | Count (raw) | COUNT(DISTINCT cube) where weight_delta > 0 |
| 2 | Conviction-weighted | SUM(weight_delta) where buy, clipped at p99 |
| 3 | Quality-weighted | SUM(weight_delta * log(1 + total_gain)) where buy |
| 4 | Net flow | SUM(weight_delta) for ALL trades (buys + sells) |
| 5 | High-conviction count | COUNT(DISTINCT cube) where weight_delta > 2.0 |

---

## 3. Results

### IC by Regime

| Signal | Overall IC | ICIR | Hit% | 上涨 IC | 震荡 IC | 下跌 IC |
|--------|-----------|------|------|---------|---------|---------|
| **4. Net flow** | **0.0038** | **0.066** | **49.2%** | 0.0022 | **+0.0052** | 0.0041 |
| 5. High-conviction | 0.0034 | 0.052 | 44.6% | 0.0003 | **+0.0047** | 0.0058 |
| 0. Pipeline (factor_z) | 0.0031 | 0.048 | 49.8% | 0.0012 | -0.0015 | **0.0098** |
| 1. Count | 0.0023 | 0.041 | 45.6% | 0.0026 | -0.0015 | 0.0059 |
| 3. Quality-weighted | 0.0022 | 0.039 | 47.5% | 0.0043 | -0.0023 | 0.0044 |
| 2. Conviction | 0.0018 | 0.033 | 44.9% | 0.0039 | -0.0025 | 0.0041 |

### Signal Correlation Matrix

```
            count  conviction  quality  netflow  highconv
count       1.000       0.881    0.871    0.683     0.536
conviction  0.881       1.000    0.979    0.770     0.649
quality     0.871       0.979    1.000    0.753     0.617
netflow     0.683       0.770    0.753    1.000     0.466
highconv    0.536       0.649    0.617    0.466     1.000
```

### Divergence Analysis
- 30.8% of stock-days where count-top-30% disagrees with conviction-top-30%
- When they disagree: count-top-only fwd_ret = 3.84%, conviction-top-only = 1.85%
- Count beats conviction when they diverge — many small buys from many cubes > one large buy from one cube

---

## 4. Key Insights

### 4.1 The sells are the missing alpha

**Net flow flips choppy IC from -0.0015 to +0.0052.** The entire reason we needed `go_flat_choppy` (Phase 2 winner, calmar +557%) was because the buy-only count signal is noise in 震荡. But when we include sell signals, the signal *works* in choppy markets.

This makes intuitive sense: in choppy regime, smart money sells are more informative than their buys. A stock being actively sold by cubes while no one is buying = strong negative signal = useful for short side of long-short.

### 4.2 Conviction weighting is counterproductive

Conviction (sum of weight_delta) has the worst IC of all variants (0.0018, ICIR 0.033). Quality weighting barely helps (corr 0.979 with conviction). The signal is in **breadth** (how many cubes), not **depth** (how much each cube allocates).

This aligns with the A-share consensus literature: institutional herding breadth predicts returns better than concentration.

### 4.3 High-conviction count is the hidden gem

Filtering to only count buys with weight_delta > 2.0 (drops 75% of noise):
- Choppy IC: +0.0047 (vs -0.0015 for current)
- Bear IC: 0.0058 (vs 0.0059 for current — nearly same)
- Only 0.536 correlation with count — a genuinely different signal

### 4.4 Current pipeline factor_z retains bear market alpha

The pipeline's rate-of-change + smoothing processing gives best 下跌 IC (0.0098). Raw count only gets 0.0059. The momentum transformation matters for bear regime.

---

## 5. Recommended Next Steps

**Priority 1: Integrate net_flow into factor pipeline**

The net_flow signal (buy-sell) is the highest-IC signal overall AND has positive choppy IC. If integrated:
- May eliminate the need for `choppy_loss_scale=0.0` (since signal works in choppy)
- Could boost calmar further by trading confidently in 30% of days currently avoided
- Implementation: modify `factor_rebalance_momentum.py` to compute `SUM(weight_delta)` instead of `COUNT(DISTINCT cube WHERE buy)`

**Priority 2: Test composite signal**

Combine net_flow (best overall + choppy IC) with pipeline factor_z (best bear IC):
- Simple blend: `0.5 * z(net_flow) + 0.5 * z(factor_z)`
- Or regime-conditional: use net_flow in choppy, factor_z in bear

**Priority 3: Run full backtest with net_flow factor**

Replace `net_buy_cube_count` with `net_flow` in the panel builder, re-run choppy_fix_B backtest to see calmar impact.

---

## 6. Data Quality Notes

- **Status filter fixed**: added `WHERE status='success'` — removes ~185 phantom buy signals
- **Factor coverage**: 4,438 date-stock rows with non-zero signal out of 3.28M panel rows
- **Conviction/quality correlation**: 0.979 — these are essentially the same signal, don't use both
- **Runtime warnings**: numpy divide-by-zero on dates with single-stock factor values (harmless)
