import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string", description="SWE-bench split"),
        "subset": Param("verified", type="string", description="verified or lite"),
        "workers": Param(2, type="integer", description="Number of parallel workers"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:3", type="string", description="e.g. 0:3 = first 3 tasks"),
        "custom_run_id": Param("", type="string", description="Leave empty to auto-generate"),
        "cost_limit": Param("0", type="string", description="Cost limit (0 = no limit)"),
    },
)
def evaluate_agent():

    @task
    def prepare_run(**context):
        params = context["params"]

        custom_run_id = params["custom_run_id"].strip() or datetime.now().strftime("%Y%m%d-%H%M%S")

        run_dir = RUNS_DIR / custom_run_id
        (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)

        config = {
            "run_id": custom_run_id,
            "split": params["split"],
            "subset": params["subset"],
            "workers": params["workers"],
            "model": params["model"],
            "task_slice": params["task_slice"],
            "cost_limit": params["cost_limit"],
        }
        with open(run_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)

        print(f"Run prepared: {run_dir}")
        return custom_run_id

    @task
    def run_agent(custom_run_id: str):
        run_dir = RUNS_DIR / custom_run_id
        agent_out_dir = run_dir / "run-agent"

        with open(run_dir / "config.json") as f:
            config = json.load(f)

        cmd = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", config["subset"],
            "--split", config["split"],
            "--model", config["model"],
            "--slice", config["task_slice"],
            "--config", str(Path.home() / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml"),
            "--workers", str(config["workers"]),
            "-o", str(agent_out_dir),
        ]

        print(f"Running agent: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
        )

        if result.returncode != 0:
            raise RuntimeError(f"Agent failed with return code {result.returncode}")

        preds_path = agent_out_dir / "preds.json"
        if not preds_path.exists():
            raise FileNotFoundError(f"preds.json not found at {preds_path}")

        print(f"Agent done. Predictions: {preds_path}")
        return custom_run_id
    @task
    def run_eval(custom_run_id: str):
        run_dir = RUNS_DIR / custom_run_id
        preds_path = run_dir / "run-agent" / "preds.json"
        eval_dir = run_dir / "run-eval"
        reports_dir = eval_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        with open(run_dir / "config.json") as f:
            config = json.load(f)

        dataset = "princeton-nlp/SWE-bench_Verified" if config["subset"] == "verified" else "princeton-nlp/SWE-bench_Lite"

        cmd = [
            str(PROJECT_ROOT / ".venv" / "bin" / "python"),
            "-m", "swebench.harness.run_evaluation",
            "--dataset_name", dataset,
            "--predictions_path", str(preds_path),
            "--max_workers", str(config["workers"]),
            "--run_id", custom_run_id,
            "--report_dir", str(reports_dir),
        ]

        print(f"Running eval: {' '.join(cmd)}")

        result = subprocess.run(cmd, cwd=reports_dir, env={**os.environ})

        if result.returncode != 0:
            raise RuntimeError(f"Eval failed with return code {result.returncode}")

        print(f"Eval done. Reports in: {reports_dir}")
        return custom_run_id
  


    @task
    def summarize_and_log(custom_run_id: str):
        import mlflow

        run_dir = RUNS_DIR / custom_run_id
        reports_dir = run_dir / "run-eval" / "reports"

        results_file = None
        # Prefer the SWE-bench report file, which contains the run_id in its name
        candidates = sorted(reports_dir.glob("*.json"))
        for f in candidates:
            if custom_run_id in f.name:
                results_file = f
                break
        if results_file is None and candidates:
            results_file = candidates[0]

        metrics = {
            "resolved_instances": 0,
            "submitted_instances": 0,
            "resolved_rate": 0.0,
        }

        if results_file and results_file.exists():
            with open(results_file) as f:
                results = json.load(f)
            submitted = results.get("submitted_instances", 0)
            resolved = results.get("resolved_instances", 0)
            metrics = {
                "resolved_instances": resolved,
                "submitted_instances": submitted,
                "resolved_rate": round(resolved / submitted, 4) if submitted > 0 else 0.0,
            }
        else:
            print("WARNING: No results file found, logging zeros")

        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        manifest = {
            "run_id": custom_run_id,
            "config": str(run_dir / "config.json"),
            "predictions": str(run_dir / "run-agent" / "preds.json"),
            "eval_reports": str(run_dir / "run-eval" / "reports"),
            "eval_logs": str(run_dir / "run-eval" / "logs"),
            "metrics": str(run_dir / "metrics.json"),
            "storage_note": (
                f"Local only (S3 skipped per tutor instruction). "
                f"To upload: aws s3 cp runs/{custom_run_id}/ s3://your-bucket/runs/{custom_run_id}/ --recursive"
            ),
        }
        with open(run_dir / "manifest.json", "w") as f:
            json.dump(manifest, f, indent=2)

        with open(run_dir / "config.json") as f:
            config = json.load(f)

        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("evaluate_agent")

        with mlflow.start_run(run_name=custom_run_id):
            mlflow.log_params({
                "split": config["split"],
                "subset": config["subset"],
                "workers": config["workers"],
                "model": config["model"],
                "task_slice": config["task_slice"],
                "cost_limit": config["cost_limit"],
            })
            mlflow.log_metrics(metrics)
            mlflow.log_param("artifact_path", str(run_dir))

        print(f"Done! Metrics: {metrics}")
        print(f"Run folder: {run_dir}")

    custom_run_id = prepare_run()
    custom_run_id = run_agent(custom_run_id)
    custom_run_id = run_eval(custom_run_id)
    summarize_and_log(custom_run_id)


evaluate_agent()