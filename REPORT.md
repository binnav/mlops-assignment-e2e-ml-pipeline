# HW3 Report: Evaluation Pipeline for Coding-Agent Experiments

## Architecture

The pipeline is an Airflow DAG (`dags/evaluate_agent.py`) implementing the
`run-agent -> run-evaluation` workflow with 4 sequential tasks:

1. **prepare_run** — reads Airflow params, resolves a `run_id`, creates the
   `runs/<run-id>/` folder tree, and writes `config.json`.
2. **run_agent** — runs `mini-swe-agent` (`mini-extra swebench`) via subprocess
   on the selected SWE-bench subset/slice. The agent solves each task inside a
   per-task Docker container and writes trajectories plus `preds.json` to
   `runs/<run-id>/run-agent/`.
3. **run_eval** — runs the real SWE-bench evaluation harness
   (`swebench.harness.run_evaluation`) on the produced `preds.json`. The harness
   builds a Docker container per instance, applies the model patch, and runs the
   repository's unit tests. Logs and reports are written to
   `runs/<run-id>/run-eval/`.
4. **summarize_and_log** — parses the SWE-bench report, writes `metrics.json`
   and `manifest.json`, and logs parameters, metrics, and the artifact path to
   MLflow.

Tasks are wired `prepare_run -> run_agent -> run_eval -> summarize_and_log`.

## Airflow Parameters

All experiment values are parameterized (no hard-coding):

| Parameter | Default | Description |
|---|---|---|
| split | test | SWE-bench split |
| subset | verified | `verified` or `lite` (selects the dataset) |
| workers | 2 | Parallel workers for agent and evaluation |
| model | nebius/moonshotai/Kimi-K2.6 | LLM used by the agent |
| task_slice | 0:3 | Instance slice (e.g. first 3 tasks) |
| custom_run_id | (empty) | Run ID; empty auto-generates a timestamp |
| cost_limit | 0 | Agent cost limit (0 = no limit) |

Note: the parameter is named `custom_run_id` because `run_id` is a reserved
keyword in the Airflow task context.

## How to Trigger a Run

1. Start MLflow: `mlflow server --host 0.0.0.0 --port 5000`
2. Start Airflow (from a shell that has Docker group access):
   `bash run-airflow-standalone.sh`
3. Open the Airflow UI at http://localhost:8080
4. Enable the `evaluate_agent` DAG, click **Trigger**, set `custom_run_id`
   (e.g. `run-real-01`), adjust params if needed, and confirm.

Docker must be available to the Airflow process, since both the agent and the
evaluation harness launch containers.

## Artifact Layout

Each run produces a self-contained, reproducible folder:
runs/<run-id>/
config.json                 # all parameters used
run-agent/
preds.json                # model patches per instance
minisweagent.log          # agent run log
run-eval/
reports/
<model>.<run-id>.json   # SWE-bench summary report
logs/run_evaluation/<run-id>/<model>/<instance>/
eval.sh               # exact test command
patch.diff            # applied model patch
test_output.txt       # full unit-test output
run_instance.log      # container run log
metrics.json                # resolved_rate and counts
manifest.json               # index of key files + storage note
Anyone can take this folder and reconstruct the whole run: inputs, config,
predictions, evaluation logs, and metrics.

## Completed Run Example (real, Docker-based evaluation)

Run ID: `run-real-01` — `subset=verified`, `split=test`, `task_slice=0:3`,
`workers=2`, `model=nebius/moonshotai/Kimi-K2.6`.

- submitted_instances: 3
- completed_instances: 3
- empty_patch_instances: 0
- resolved_instances: 1
- unresolved_instances: 2
- **resolved_rate: 0.3333**

Resolved: `astropy__astropy-12907`.
Unresolved: `astropy__astropy-13033`, `astropy__astropy-13236`.

These numbers come from real unit tests run inside SWE-bench Docker containers,
not from precomputed samples.

## MLflow

Each run logs its parameters (split, subset, workers, model, task_slice,
cost_limit, artifact_path) and metrics (resolved_rate, resolved_instances,
submitted_instances) to MLflow, so multiple runs can be compared in the UI.

Screenshots:
- `screenshots/Airflow.png` — Airflow UI, completed `evaluate_agent` run.
- `screenshots/MLFlow.png` — MLflow UI, logged run with params and metrics.

## Rerun Instructions

To reproduce or extend a run, trigger the DAG and set `custom_run_id` to the
desired ID. A new ID produces a fresh run; the agent skips instances that
already have predictions, so use a new `custom_run_id` for a clean run.

## Storage (S3 / Object Storage)

Run artifacts are stored locally under `runs/<run-id>/` per course guidance
(setting up managed Object Storage requires admin permissions not available to
students). `manifest.json` records how the folder would be uploaded in
production, e.g.:

```bash
aws s3 cp runs/<run-id>/ s3://<bucket>/runs/<run-id>/ --recursive
```

## Notes / Possible Extensions

- Production-style path (not implemented): replace subprocess calls with
  `DockerOperator` using the provided `Dockerfile`, and deploy Airflow + MLflow
  via `docker-compose.yaml`.