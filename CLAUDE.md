# Bili_Stock — Claude Project Context

## What This Is

A-share quant system that tracks **Xueqiu smart-money consensus signals** — when 2+ elite portfolios buy the same stock within a 5-day window, that's a long signal. The core insight: in choppy/bear regimes, follow the contrarian reversal; in bull regimes, flip the signal.

- **55,000+ rebalancing records** from cubes.db, 1,373 unique stocks, 2014-2026  
- **Production Calmar: 0.282** (choppy_fix_B branch, 2019-2025)  
- **Live validation win rate: 53.76%** (Smart Money paper trades, Jan 2025–Feb 2026)

---

## Production Branch: `choppy_fix_B`

| Param | Value | Meaning |
|---|---|---|
| `hold_step` | 12 | rebalance every 12 trading days |
| `cap_non_up` | 0.10 | max 10% of picks from one industry (non-bull) |
| `cap_up` | 0.20 | max 20% from one industry (bull regime) |
| `liq_other` | 0.60 | keep top 60% by liquidity |
| `non_up_vol_q` | 0.65 | vol filter: keep stocks ≤ 65th pct of \|ret20d\| |
| `choppy_loss_scale` | 0.50 | halve position in choppy losing streaks |

**Baseline files** (in `research/baseline_v6_1/output/`):
- `choppy_fix_B_hold12_cap10_group_ret_2010_2025.csv` — return series (gray pipeline reads this)
- `choppy_fix_B_hold12_cap10_holdings_2010_2025.csv`
- `choppy_fix_B_hold12_cap10_risk_log_2010_2025.csv`

---

## Phase Roadmap

| Phase | Status | What |
|---|---|---|
| Phase 1 | ✅ Done (commit `dee87bc`) | Wire choppy_fix_B as production, E3 baostock fallback, 91/91 tests green |
| Phase 2 | 🔄 In progress | SRF scoring + Top-K selection; live validation shows +2.33% edge |
| Phase 3 | ⬜ Planned | Wire Phase 2 winner into gray pipeline, live forward testing |

**Phase 2 current state:**
- `_srf_score()` and modified `_pick_top(top_k, use_srf)` implemented in `run_baseline_v6_v61_suite.py`
- Grid search running: `run_phase2_topk_srf_grid.py` sweeps `top_k ∈ {5,8,10,12,15,20,None}`
- Live validation: SRF places battle picks at **86.5th pct** (t=16, p≈0); top-half vs bot-half edge +2.33%
- **Known bug (being fixed):** NaN vol filter — `ret20d_stock` NaN → quantile=0 → wrong filter path

---

## Key Files

### Core Backtest Engine
| File | Role |
|---|---|
| `research/baseline_v6_1/code/run_baseline_v6_v61_suite.py` | **Main engine.** `_srf_score()`, `_pick_top()`, `_build_rebalance()`, `_run_one()`, `_metrics()` |
| `research/baseline_v5/code/run_baseline_v5_with_costs.py` | `_prepare_panel_v5()` — builds the full panel from cubes.db |
| `research/baseline_v5/code/run_baseline_v4_2_up_filter.py` | `_select_top_with_industry_cap()`, `_attach_base_fields()`, `_apply_liq_dynamic()` |

### Phase 2 Scripts
| File | Role |
|---|---|
| `research/baseline_v6_1/code/run_phase2_topk_srf_grid.py` | Grid search — sweeps top_k, saves group_ret + holdings + risk_log for best |
| `research/baseline_v6_1/code/run_live_validation.py` | Validates SRF vs battle_trades ground truth |

### Gray Production Pipeline
| File | Role |
|---|---|
| `research/baseline_v6_1/code/run_gray_pipeline.py` | Orchestrator — calls daily + weekly as subprocesses |
| `research/baseline_v6_1/code/run_gray_daily.py` | Daily decision engine; `--baseline` arg controls which CSV to use |
| `research/baseline_v6_1/code/run_gray_weekly.py` | Weekly report generator |

### Data
| Path | Content |
|---|---|
| `data/cubes.db` | 221MB SQLite — `cubes` table (portfolio metadata) + `rebalancing_history` (8720 records) |
| `data/battle_trades_*.csv` | Live paper trading ground truth, Jan 2025–Feb 2026, 388 BUY / 378 SELL |
| `data/stock_data/` | Per-stock OHLCV CSVs from BaoStock |
| `data/market_cache/` | Liquidity / market data cache |
| `research/baseline_v1/data_delivery/` | `industry_mapping_v2.csv`, `liquidity_daily_v1.csv` |

### Validation
```bash
python scripts/analysis/validate_pipeline.py   # 91/91 tests — run after any core change
```

---

## Architecture: Panel → _pick_top → Metrics

```
cubes.db
  └── build_rebalance_momentum_panel()   # net_buy_cube_count, factor_z
        └── _attach_base_fields()        # + industry_l2, amount, ret20d_stock
              └── _industry_neutralize() # + factor_z_neu
                    └── _apply_liq_dynamic() + _load_hs300() # + regime, liq_rank_pct

panel → _run_one(hold_step, liq_other, cap_non_up, cap_up, risk_cfg)
          └── _build_rebalance()
                └── _pick_top(day, regime, ..., top_k, use_srf)
                      ├── use_srf=False → rank>=0.7 threshold (legacy)
                      └── use_srf=True  → _srf_score() + Top-K + industry cap
          └── _apply_costs() → _apply_risk_controls() → _metrics()
```

**SRF score components** (cross-sectional z-score, within each rebalancing day):
- 40% `factor_z_neu` — Xueqiu smart-money consensus (≈ main_net_inflow)
- 30% `ret20d_stock` — 20-day price momentum
- 20% `amount` — volume proxy
- 10% `net_buy_cube_count` — same-day buying pulse (≈ DDX)

---

## Gray Pipeline Operation

```bash
# Full daily run (uses default baseline)
python research/baseline_v6_1/code/run_gray_pipeline.py

# With Phase 2 SRF winner (replace top10 with actual winner)
python research/baseline_v6_1/code/run_gray_daily.py \
  --baseline research/baseline_v6_1/output/choppy_fix_B_hold12_cap10_srf_top10_group_ret_2010_2025.csv

# Bootstrap live data from baseline (first-time setup)
python research/baseline_v6_1/code/run_gray_daily.py --bootstrap-sample-days 72
```

Holdings and risk_log paths are **auto-derived** from `--baseline` by replacing `_group_ret_` with `_holdings_` / `_risk_log_` (fixed in gray pipeline, no hard-coded paths).

---

## Conventions

- **Regime**: `上涨` = bull, `震荡` = choppy, `下跌` = bear (from HS300 20-day return)
- **factor_use**: `-factor_z_raw` in bull (contrarian), `factor_z_neu` otherwise (momentum)
- **File naming**: `{strategy_tag}_{metric}_{start}_{end}.csv` — e.g. `choppy_fix_B_hold12_cap10_group_ret_2010_2025.csv`
- **Output dirs**: always `research/baseline_vX/output/`, reports in `research/baseline_vX/report/`
- **Vol filter**: NaN `ret20d_stock` → treated as infinite vol (filtered out), NOT filled with 0

## What NOT to Do

- Don't call `_prepare_panel_v5()` repeatedly — it's slow (3.3M rows). Cache the result in the same script.
- Don't add new baseline versions (`v7`, etc.) without a research folder with `code/output/report/` structure.
- Don't change `cap_non_up`, `hold_step`, or `non_up_vol_q` in gray pipeline without running a full 2010-2025 backtest first.
- The `choppy_fix_B_*` output files are the production source of truth — don't overwrite them with experimental runs.

---

## Recent History

- `dee87bc` — Phase 1: wire choppy_fix_B as prod, E3 fallback, 91/91 tests
- Phase 2 in progress: SRF + TopK, live validation done, grid search running
- Battle trades: Smart Money 53.76% win, Dumb Money 52.60% (Jan 2025–Feb 2026)
