---
name: "decision-consistency-check"
description: "Compares gray online/offline decisions and highlights drift. Invoke after gray runs, logic changes, or when user asks to verify decision consistency."
---

# Decision Consistency Check

Use this skill to verify that gray online decision output and offline evaluator output remain aligned.

## Invoke When

- After running `run_gray_pipeline.py`.
- After changing decision logic in `run_gray_daily.py` or `evaluate_gray_deployment.py`.
- When user asks to verify no decision drift.

## Inputs

- `research/baseline_v6_1/output/live/live_equity.csv`
- `research/baseline_v6_1/output/base_E_foundation_group_ret_2010_2025.csv`
- `research/baseline_v6_1/output/live/live_risk_log.csv`
- `research/baseline_v6_1/output/live/gray_deployment_decision.csv`

## Procedure

1. Run offline evaluator to generate compare file:
   - `python research/baseline_v6_1/code/evaluate_gray_deployment.py --live research/baseline_v6_1/output/live/live_equity.csv --baseline research/baseline_v6_1/output/base_E_foundation_group_ret_2010_2025.csv --risk research/baseline_v6_1/output/live/live_risk_log.csv --cycle-bars 12 --output research/baseline_v6_1/output/live/gray_deployment_decision_eval.csv`
2. Read and compare these columns:
   - `action`
   - `reasons`
   - `gap_now`
   - `gap_prev`
   - `cycle_mdd`
   - `oos_sortino`
3. Mark result:
   - `PASS` if all fields match
   - `WARN` if any field differs

## Fail Handling

- If evaluator command fails, return `FAIL` with stderr tail.
- If files are missing, return `FAIL` and list missing paths.
- If mismatch exists, return both records in compact form.

## Output Template

- Consistency status: PASS/WARN/FAIL
- Online decision: action + reasons
- Offline decision: action + reasons
- Diff fields: field list or `none`
- Next action:
  - Keep current logic
  - Re-check risk priority order
  - Re-run with same inputs for deterministic check
