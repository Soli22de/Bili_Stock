---
name: "risk-log-policy-check"
description: "Validates live_risk_log triggers against pause/reduce policy and checks decision reasons. Invoke after gray runs, risk rule updates, or when user asks risk-policy verification."
---

# Risk Log Policy Check

Use this skill to verify whether `live_risk_log.csv` is correctly reflected in gray decisions and reports.

## Invoke When

- After running gray daily pipeline.
- After changing risk trigger mapping or thresholds.
- When user asks why action is pause/reduce/hold.

## Inputs

- `research/baseline_v6_1/output/live/live_risk_log.csv`
- `research/baseline_v6_1/output/live/gray_deployment_decision.csv`
- `research/baseline_v6_1/output/live/daily_report.md`
- Optional:
  - `research/baseline_v6_1/output/live/live_input_quality_report.md`

## Policy Baseline

- Severe triggers should force pause:
  - `portfolio_stop`
  - `drawdown_brake`
  - `risk_pause`
  - `trading_halt`
- Moderate trigger accumulation can force reduce:
  - `overheat_brake`
  - `stock_stop`
  - `industry_stop`
  - `concentration_limit`
- `risk_scale` checks:
  - `min < 0.4` => pause
  - `avg < 0.6` => reduce

## Procedure

1. Load latest decision and extract:
   - `action`
   - `reasons`
2. Restrict `live_risk_log.csv` to recent cycle dates (same cycle-bars window used by daily decision).
3. Compute expected policy flags:
   - expected_pause
   - expected_reduce
4. Compare expected flags vs actual action:
   - Pause expected but action not pause => mismatch
   - Reduce expected but action is hold/upgrade => mismatch
5. Verify reason traceability:
   - If pause/reduce expected, `reasons` should include mapped risk reason token.
6. Output compact audit summary.

## Result Rules

- PASS:
  - Action matches expected policy priority
  - Reason traceability present
- WARN:
  - Action matches, but reasons missing/unclear
  - No recent risk rows available
- FAIL:
  - Action violates expected risk policy

## Output Template

- Policy status: PASS/WARN/FAIL
- Expected flags: pause/reduce
- Actual action: value
- Reason traceability: PASS/WARN
- Evidence:
  - severe trigger count
  - moderate trigger count
  - risk_scale min/avg
- Next action:
  - Keep policy mapping
  - Fix reason mapping
  - Fix action priority logic
