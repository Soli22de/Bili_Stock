---
name: "gray-daily-ops"
description: "Runs baseline v6.1 gray pipeline and validates decision/report consistency. Invoke for daily ops, stale-data checks, or when user asks for one-click gray execution."
---

# Gray Daily Ops

Use this skill to execute and verify the baseline v6.1 gray operation flow with a fixed checklist.

## Invoke When

- User asks to run gray daily/weekly pipeline.
- User asks for one-click gray execution and health check.
- User asks to verify stale-data guard and decision consistency.

## Scope

- Target path: `research/baseline_v6_1/output/live`
- Pipeline entry: `research/baseline_v6_1/code/run_gray_pipeline.py`
- Decision files:
  - `gray_deployment_decision.csv`
  - `gray_deployment_decision_eval.csv`
- Reports:
  - `daily_report.md`
  - `weekly_report.md`
  - `live_input_quality_report.md`

## Standard Procedure

1. Run pipeline:
   - `python research/baseline_v6_1/code/run_gray_pipeline.py --live-dir research/baseline_v6_1/output/live --auto-fill-from-baseline 0 --strict-live-freshness 1`
2. Run offline evaluator:
   - `python research/baseline_v6_1/code/evaluate_gray_deployment.py --live research/baseline_v6_1/output/live/live_equity.csv --baseline research/baseline_v6_1/output/base_E_foundation_group_ret_2010_2025.csv --risk research/baseline_v6_1/output/live/live_risk_log.csv --cycle-bars 12 --output research/baseline_v6_1/output/live/gray_deployment_decision_eval.csv`
3. Validate required outputs exist and are readable.
4. Compare online/offline decision fields:
   - `action`
   - `reasons`
   - `gap_now`
   - `gap_prev`
   - `cycle_mdd`
   - `oos_sortino`
5. Summarize:
   - pipeline success/failure
   - decision consistency result
   - stale-data warnings from `live_input_quality_report.md`
   - current action and reasons

## Fail-Fast Rules

- If pipeline fails, return error summary immediately and stop deeper checks.
- If decision mismatch appears, mark result as `WARN` and include both records.
- If quality report is `WARN` with `live_equity_stale`, highlight stale guard impact.

## Output Template

- Pipeline status: PASS/WARN/FAIL
- Online decision: action + reasons
- Offline decision: action + reasons
- Consistency: PASS/WARN
- Input quality: PASS/WARN + warning list
- Next action:
  - Keep run cadence
  - Refresh live inputs
  - Inspect risk trigger mapping
