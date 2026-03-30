import os
import subprocess
import sys
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _run(script_path: str):
    cmd = [sys.executable, script_path]
    subprocess.run(cmd, check=True, cwd=ROOT)


def _compile_stage_a(rep_dir: str, out_dir: str):
    core = os.path.join(out_dir, "core_metrics_baseline_v6_1_2019_2025.csv")
    if not os.path.exists(core):
        return
    x = pd.read_csv(core)
    x.to_csv(os.path.join(rep_dir, "phase_a_baseline_metrics.csv"), index=False, encoding="utf-8-sig")


def _compile_final_report(rep_dir: str):
    paths = {
        "A": os.path.join(rep_dir, "phase_a_baseline_metrics.csv"),
        "B": os.path.join(rep_dir, "phase_b_risk_control_report.md"),
        "C1": os.path.join(rep_dir, "e3_focus_report.md"),
        "C2": os.path.join(rep_dir, "e3_2_micro_tuning_report.md"),
        "C3": os.path.join(rep_dir, "e3_2_light_tuning_report.md"),
        "D1": os.path.join(rep_dir, "oos_elimination_report.md"),
        "D2": os.path.join(rep_dir, "phase_d_validation_report.md"),
        "E1": os.path.join(rep_dir, "phase_e_special_diagnostics_report.md"),
    }
    out = os.path.join(rep_dir, "phase_a_to_e_execution_summary.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 A-E 阶段执行汇总\n\n")
        f.write("- 阶段A：基线冻结+评估指标扩展（含Sortino/Downside/MDD持续期/CVaR95）。\n")
        f.write("- 阶段B：风控主模块执行（过热刹车/组合止损/行业与个股止损/集中度约束/对冲触发）。\n")
        f.write("- 阶段C：E3版本族微调（E3_1~E3_3，E3_2_1~E3_2_6）。\n")
        f.write("- 阶段D：样本外与滚动验证+参数稳健性+偏差审计+可信度评分。\n")
        f.write("- 阶段E：五项专项诊断CSV与汇总报告。\n\n")
        f.write("## 产物检查\n\n")
        for k, p in paths.items():
            f.write(f"- 阶段{k}: {'已生成' if os.path.exists(p) else '缺失'} - {p}\n")
        f.write("\n## 决策门槛与淘汰规则\n\n")
        f.write("- 样本外淘汰规则已执行：连续12个月跑输基准/Calmar<0/MDD>30%。\n")
        f.write("- 停机回退建议：若新版收益提升但回撤恶化，回退上一稳定版本。\n")
    print(out)


def main():
    rep_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(rep_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_baseline_v6_v61_suite.py"))
    _compile_stage_a(rep_dir, out_dir)
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_phase_b_risk_controls.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_minimal_experiment_set.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_focus_experiments.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_2_micro_tuning.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_2_light_tuning.py"))
    _run(os.path.join(ROOT, "scripts", "summary_key_strategies_2019_2025.py"))
    _run(os.path.join(ROOT, "scripts", "build_synthetic_benchmark.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "evaluate_oos_and_elimination.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_phase_d_validation.py"))
    _run(os.path.join(ROOT, "scripts", "analyze_drawdown_2024_2025.py"))
    _run(os.path.join(ROOT, "research", "baseline_v6_1", "code", "generate_phase_e_report.py"))
    _compile_final_report(rep_dir)


if __name__ == "__main__":
    main()
