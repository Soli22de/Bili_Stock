# Bili_Stock — Claude Project Context

## What This Is

A-share quant system that tracks **Xueqiu smart-money consensus signals** — when 2+ elite portfolios buy the same stock within a 5-day window, that's a long signal. The core insight: in choppy/bear regimes, follow the consensus; in bull regimes, flip the signal (contrarian).

- **55,000+ cubes** in cubes.db, 291 with rebalancing data, 1,373 unique stocks, 2014-2026
- **8,070 successful trades** (3,836 buys, 2,278 sells)
- **Live validation win rate: 53.76%** (Smart Money paper trades, Jan 2025–Feb 2026)

---

## Production Config: `choppy_fix_B` + `B_goflat_choppy`

Centralized in `research/baseline_v6_1/prod_config.py`.

| Param | Value | Meaning |
|---|---|---|
| `hold_step` | 12 | rebalance every 12 **business** days |
| `cap_non_up` | 0.10 | max 10% of picks from one industry (non-bull) |
| `cap_up` | 0.20 | max 20% from one industry (bull regime) |
| `liq_other` | 0.60 | keep top 60% by liquidity via `liq_rank_pct` |
| `non_up_vol_q` | 0.65 | vol filter: keep stocks ≤ 65th pct of \|ret20d\| |
| `choppy_loss_scale` | 0.0 | Phase 2 winner: asymmetric choppy scaling |
| `choppy_loss_floor` | 0.0 | minimum scale in losing choppy (configurable) |

**Active baseline tag**: `choppy_fix_B_hold12_cap10_B_goflat_choppy`

**Baseline files** (in `research/baseline_v6_1/output/`):
- `choppy_fix_B_hold12_cap10_B_goflat_choppy_group_ret_2010_2025.csv`
- `choppy_fix_B_hold12_cap10_B_goflat_choppy_holdings_2010_2025.csv`
- `choppy_fix_B_hold12_cap10_B_goflat_choppy_risk_log_2010_2025.csv`

**NOTE**: These files were generated BEFORE bug fixes (hs300_ret20, liq_rank_pct, bday). Need regeneration.

---

## Phase Roadmap

| Phase | Status | What |
|---|---|---|
| Phase 1 | ✅ Done | Wire choppy_fix_B as production, E3 baostock fallback |
| Phase 2 | ✅ Done | Choppy optimization: asymmetric scale calmar 0.10→0.48 |
| Phase 3 | ✅ Done | Gray pipeline wired to prod_config, signal IC research, net_flow tested |
| Phase 4 | 🔄 In progress | Pipeline bug fixes, data quality, re-run backtests |

**Phase 2 results** (see `report/phase2_research_report.md`):
- SRF v1 (replace gate): FAILED — all negative calmar
- SRF v2 (within-gate re-rank): marginal +19%, not worth complexity
- ADX regime classifier: made things worse, reverted
- **choppy_loss_scale=0.0 (asymmetric)**: calmar 0.10→0.48 — WINNER
- True go-flat (zero ALL choppy): calmar 0.21 — worse than asymmetric

**Phase 3 results** (see `report/phase3_signal_research_report.md`):
- Net flow IC=+0.005 in choppy, but **failed in backtest** (calmar negative)
- IC ≠ backtest: pipeline transforms (neutralization, liq filter) destroyed signal
- Conviction/quality weighting: counterproductive, breadth > depth
- Sell signal may work as **negative filter** (not primary signal)

**Phase 4 bug fixes** (commit `3935aaf` + current):
- hs300_ret20 was never in panel → overheat detection was dead code
- liq_rank_pct was never created → liq_other parameter had no effect
- choppy_loss_scale floor was hardcoded 0.30 → now configurable
- Open price column used wrong index → intraday reversal factor was wrong
- Factor builder used calendar days → now uses business days

---

## Key Files

### Core Backtest Engine
| File | Role |
|---|---|
| `research/baseline_v6_1/code/run_baseline_v6_v61_suite.py` | **Main engine.** `_build_rebalance()`, `_pick_top()`, `_run_one()`, `_apply_risk_controls()`, `_metrics()` |
| `research/baseline_v5/code/run_baseline_v5_with_costs.py` | `_prepare_panel_v5()` — builds full panel from cubes.db, now creates `liq_rank_pct` |
| `research/baseline_v4/code/run_baseline_v4_2_up_filter.py` | `_load_hs300()` (now returns hs300_ret20), `_apply_liq_dynamic()`, `_select_top_with_industry_cap()` |
| `research/factors/factor_rebalance_momentum.py` | Signal construction: count or net_flow mode, now uses bdate_range |
| `research/data_prep/build_rebalance_momentum_panel.py` | Panel builder, `WHERE status='success'` filter |

### Gray Production Pipeline
| File | Role |
|---|---|
| `research/baseline_v6_1/code/run_gray_pipeline.py` | Orchestrator — imports ACTIVE_PATHS from prod_config |
| `research/baseline_v6_1/code/run_gray_daily.py` | Daily decision engine |
| `research/baseline_v6_1/code/run_gray_weekly.py` | Weekly report generator |
| `research/baseline_v6_1/prod_config.py` | **Single source of truth** for production params |

### Research Reports
| File | Content |
|---|---|
| `research/baseline_v6_1/report/phase2_research_report.md` | All Phase 2 experiments and findings |
| `research/baseline_v6_1/report/phase3_signal_research_report.md` | Signal IC + net_flow backtest results |
| `docs/quant_concepts_guide.md` | Plain-language guide to IC, Calmar, regime etc. |

### Data
| Path | Content |
|---|---|
| `data/cubes.db` | SQLite — `cubes` (55K portfolios) + `rebalancing_history` (8K+ records) |
| `data/stock_data/` | Per-stock OHLCV CSVs (columns: 日期, 开盘, 收盘, 成交额, etc.) |
| `data/market_cache/` | Liquidity / market data cache |

---

## Architecture

```
cubes.db (WHERE status='success')
  └── factor_rebalance_momentum.py   # net_buy_cube_count, factor_z (bdate_range)
        └── _attach_base_fields()    # + industry_l2, amount, ret20d_stock
              └── _industry_neutralize() # + factor_z_neu
                    └── _apply_liq_dynamic() + _load_hs300()
                          # + regime, hs300_ret20, liq_rank_pct
                          └── _enrich_from_stock_data()
                                # + vol_price_div5d, ret_intra5d, hv20_hv60_ratio

panel → _run_one(hold_step, liq_other, cap_non_up, cap_up, risk_cfg)
          ├── filter by liq_rank_pct <= liq_other
          └── _build_rebalance()
                └── _pick_top(day, regime, ...)
                      └── rank>=0.7 threshold (Xueqiu gate = the alpha)
          └── _apply_costs() → _apply_risk_controls() → _metrics()

_apply_risk_controls:
  ├── market_hot (hs300_ret20 quantile) → scale 0.5/0.7 in overheated bull
  ├── drawdown_brake (dd_soft/mid/hard) → scale 0.5/0.6/0.75
  └── choppy_loss_scale → scale losing 震荡 periods (floor configurable)
```

---

## Signal Design

**factor_use direction** (intentional):
- Bull (上涨): `-factor_z_raw` → contrarian (consensus picks are overbought; IC=+0.001)
- Choppy/Bear: `factor_z_neu` → momentum (follow smart money; IC=+0.010 in bear)

**Signal variants tested** (Phase 3):
- Count (current): IC=0.0023 overall, choppy IC=-0.0015
- Net flow: IC=0.0038 overall, choppy IC=+0.0052 — but **failed in backtest**
- Conviction/quality: worse than count (breadth > depth)

---

## Conventions

- **Regime**: `上涨` = bull (ret20>2%), `震荡` = choppy, `下跌` = bear (ret20<-2%)
- **File naming**: `{strategy_tag}_{metric}_{start}_{end}.csv`
- **Output dirs**: `research/baseline_vX/output/`, reports in `research/baseline_vX/report/`
- **Vol filter**: NaN `ret20d_stock` → treated as infinite vol (filtered out)
- **Long backtests**: Run in terminal (not Claude Code background), ~70min per experiment

## What NOT to Do

- Don't call `_prepare_panel_v5()` repeatedly — it's slow (3.3M rows). Cache the result.
- Don't change production params without running a full 2010-2025 backtest first.
- Don't overwrite `choppy_fix_B_*` output files with experimental runs.
- Don't run backtests >10min as Claude Code background tasks — they timeout.
- Don't trust IC alone — always validate with full backtest (net_flow lesson).
