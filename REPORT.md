# HW3 Report: Evaluation Pipeline for Coding-Agent Experiments

## Architecture

The pipeline is implemented as an Airflow DAG (`dags/evaluate_agent.py`) with 4 sequential tasks:

1. **prepare_run** — reads Airflow params, generates a unique `run_id`, creates the `runs/<run-id>/` folder structure, saves `config.json`
2. **run_agent** — calls `mini-extra swebench` via subprocess with DAG-provided params, writes trajectories and `preds.json` to `runs/<run-id>/run-agent/`
3. **run_eval** — uses sample evaluation results (Docker-based SWE-bench evaluation skipped per tutor instruction — students don't have admin permissions for object storage or Docker setup). Writes results to `runs/<run-id>/run-eval/reports/`
4. **summarize_and_log** — parses eval results, writes `metrics.json` and `manifest.json`, logs all params and metrics to MLflow

## How to Trigger a Run

1. Start MLflow: `mlflow server --host 0.0.0.0 --port 5000`
2. Start Airflow: `bash run-airflow-standalone.sh`
3. Open Airflow UI at http://localhost:8080
4. Click `evaluate_agent` → enable the toggle → click ▶ Trigger
5. Set `custom_run_id` (e.g. `run-001`) and adjust params as needed
6. Click Trigger

## Airflow Parameters

| Parameter | Default | Description |
|---|---|---|
| split | test | SWE-bench split |
| subset | verified | verified or lite |
| workers | 2 | Parallel workers |
| model | nebius/moonshotai/Kimi-K2.6 | LLM model |
| task_slice | 0:3 | Tasks to run (e.g. first 3) |
| custom_run_id | (auto) | Leave empty to auto-generate |
| cost_limit | 0 | Cost limit (0 = no limit) |

## Artifact Layout

Each run produces:
runs/<run-id>/
config.json          # all parameters used
run-agent/
preds.json         # agent's patches
<task-id>/         # per-task trajectories
run-eval/
reports/           # evaluation results
logs/
metrics.json         # resolved_rate, counts
manifest.json        # index of all files + S3 upload note
## Completed Run Example

Run ID: `run-001`
- submitted_instances: 3
- resolved_instances: 1  
- resolved_rate: 0.3333

## MLflow

MLflow tracks all parameters and metrics for each run, enabling comparison across experiments.

See screenshots:
- `screenshots/airflow_dag.png` — Airflow UI showing completed pipeline
- `screenshots/mlflow_runs.png` — MLflow UI showing logged run with metrics

## How to Rerun by run_id

In the Airflow trigger form, set `custom_run_id` to an existing run ID. The DAG will use that folder.

## S3 / Object Storage

S3 upload was skipped per tutor instruction (students don't have admin permissions to set up object storage).

To upload a run to S3 in production:
```bash
aws s3 cp runs/<run-id>/ s3://your-bucket/runs/<run-id>/ --recursive
```

This command is also documented in each run's `manifest.json`.