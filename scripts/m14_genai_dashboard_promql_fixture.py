"""Execute the GenAI receiver/worker dashboard's actual PromQL with promtool."""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "deployments/grafana/dashboards/genai-runtime.json"

def main():
    dashboard = json.loads(DASHBOARD.read_text())
    panels = {p["id"]: p for p in dashboard["panels"]}
    assert set(panels) == set(range(1, 15)), "dashboard panel IDs changed"
    assert len(dashboard["__inputs"]) == 1
    def expr(panel, target=0):
        return panels[panel]["targets"][target]["expr"]

    receiver = 'instance="receiver:4318",job="kubernetes-pods"'
    worker = 'instance="worker:9109",job="genai-observation-worker"'
    series = [
        {"series": f'up{{{receiver}}}', "values": "1 1 0 1 1 1"},
        {"series": f'up{{{worker}}}', "values": "1 1 0 1 1 1"},
        {"series": f'llm_telemetry_genai_receiver_up{{{receiver}}}', "values": "1 1 stale 1 1 1"},
        {"series": f'llm_telemetry_genai_receiver_admission_requests_total{{{receiver}}}', "values": "0 100 200 300 400 500"},
        {"series": f'llm_telemetry_genai_receiver_admission_accepted_total{{{receiver}}}', "values": "0 90 180 270 360 450"},
        {"series": f'llm_telemetry_genai_receiver_admission_rejected_total{{{receiver}}}', "values": "0 5 10 15 20 25"},
        {"series": f'llm_telemetry_genai_receiver_admission_failed_total{{{receiver}}}', "values": "0 5 10 15 20 25"},
        {"series": f'llm_telemetry_genai_receiver_admission_duration_seconds_count{{{receiver}}}', "values": "0 10 20 30 40 50"},
        {"series": f'llm_telemetry_genai_receiver_admission_duration_seconds_sum{{{receiver}}}', "values": "0 0.5 1 1.5 2 2.5"},
        {"series": f'llm_telemetry_genai_worker_up{{{worker}}}', "values": "1 1 stale 1 1 1"},
        {"series": f'llm_telemetry_genai_worker_queue_length_available{{{worker}}}', "values": "1 0 0 0 1 1"},
        {"series": f'llm_telemetry_genai_worker_queue_pending_depth{{{worker}}}', "values": "25 25 25 25 0 0"},
        {"series": f'llm_telemetry_genai_worker_queue_terminal_depth{{{worker}}}', "values": "2 2 2 2 0 0"},
        {"series": f'llm_telemetry_genai_worker_queue_capacity{{{worker}}}', "values": "100 100 100 100 100 100"},
        {"series": f'llm_telemetry_genai_worker_oldest_pending_age_seconds{{{worker}}}', "values": "60 60 60 60 0 0"},
        {"series": f'llm_telemetry_genai_worker_last_successful_delivery_timestamp_seconds{{{worker}}}', "values": "0 0 0 0 240 240"},
        {"series": f'llm_telemetry_genai_worker_acknowledged_total{{{worker}}}', "values": "0 90 180 270 360 450"},
        {"series": f'llm_telemetry_genai_worker_retryable_total{{{worker}}}', "values": "0 10 20 30 40 50"},
        {"series": f'llm_telemetry_genai_worker_terminal_total{{{worker}}}', "values": "0 0 0 0 0 0"},
        {"series": f'llm_telemetry_genai_worker_export_duration_seconds_count{{{worker}}}', "values": "0 10 20 30 40 50"},
        {"series": f'llm_telemetry_genai_worker_export_duration_seconds_sum{{{worker}}}', "values": "0 1 2 3 4 5"},
        {"series": f'llm_telemetry_genai_worker_errors_total{{{worker}}}', "values": "0 0 0 0 0 0"},
    ]
    tests = []
    def check(panel, at, labels, value):
        tests.append({"expr": expr(panel), "eval_time": at,
                      "exp_samples": [] if value is None else [{"labels": labels, "value": value}]})
    check(1, "0m", '{__name__="up",instance="receiver:4318",job="kubernetes-pods"}', 1)
    check(1, "2m", '{__name__="up",instance="receiver:4318",job="kubernetes-pods"}', 0)
    check(4, "2m", None, None)
    check(4, "5m", '{instance="receiver:4318"}', 5)
    check(5, "5m", '{instance="receiver:4318"}', 0.05)
    check(6, "2m", '{__name__="up",instance="worker:9109",job="genai-observation-worker"}', 0)
    check(7, "0m", '{instance="worker:9109",job="genai-observation-worker"}', 25)
    check(7, "1m", None, None)
    check(7, "2m", None, None)
    check(7, "4m", '{instance="worker:9109",job="genai-observation-worker"}', 0)
    check(10, "0m", None, None)
    check(10, "4m", '{instance="worker:9109",job="genai-observation-worker"}', 0)
    check(12, "5m", '{instance="worker:9109",job="genai-observation-worker"}', 10)
    check(13, "5m", '{instance="worker:9109",job="genai-observation-worker"}', 0.1)
    print(json.dumps({"rule_files": [], "evaluation_interval": "1m", "tests": [
        {"interval": "1m", "input_series": series, "promql_expr_test": tests}
    ]}, indent=2))

if __name__ == "__main__":
    main()
