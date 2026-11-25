"""Run the exact dashboard PromQL against a deterministic promtool fixture."""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "deployments/grafana/dashboards/kafka-operator.json"
LABELS = 'group="g",topic="t",partition="0",job="lag-exporter",instance="exporter:9108"'
TARGET = 'job="lag-exporter",instance="exporter:9108"'
PREFIX = "llm_telemetry_kafka_"
def main():
    dashboard = json.loads(DASHBOARD.read_text())
    panels = {p["id"]: p for p in dashboard["panels"]}
    assert set(panels) == set(range(1, 8))
    assert "__inputs" not in dashboard
    for panel in panels.values():
        assert panel["datasource"]["uid"] == "prometheus"
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "prometheus"
    series = [
        {"series": f'up{{{TARGET}}}', "values": "1 1 0 1"},
        {"series": f'llm_telemetry_lag_scrape_success{{{TARGET}}}', "values": "1 0 1 1"},
    ]
    metrics = {3: ("lag_offsets", "12 12 12 0", 12, 0),
               4: ("retention_headroom_seconds", "30 30 30 stale", 30, None),
               5: ("oldest_unconsumed_age_seconds", "20 20 20 stale", 20, None),
               6: ("data_gap_offsets", "2 2 2 0", 2, 0),
               7: ("retention_risk", "1 1 1 stale", 1, None)}
    tests = []
    for i, (name, values, first, last) in metrics.items():
        labels = LABELS + (',status="warning"' if i == 7 else "")
        metric = PREFIX + name
        series.append({"series": f'{metric}{{{labels}}}', "values": values})
        if i == 7:
            series.append({"series": f'{metric}{{{LABELS},status="no_backlog"}}', "values": "stale stale stale 1"})
        for minute, expected in enumerate([first, None, None, last]):
            expected_series = [] if expected is None else [{"labels": f'{metric}{{{labels}}}', "value": expected}]
            if i == 7 and minute == 3:
                expected_series = [{"labels": f'{metric}{{{LABELS},status="no_backlog"}}', "value": 1}]
            tests.append({"expr": panels[i]["targets"][0]["expr"], "eval_time": f"{minute}m", "exp_samples": expected_series})
    for i, metric, values in [(1, "up", [1, 1, 0, 1]), (2, "llm_telemetry_lag_scrape_success", [1, 0, None, 1])]:
        for minute, expected in enumerate(values):
            tests.append({"expr": panels[i]["targets"][0]["expr"], "eval_time": f"{minute}m",
                          "exp_samples": [] if expected is None else [{"labels": f'{metric}{{{TARGET}}}', "value": expected}]})
    print(json.dumps({"rule_files": [], "evaluation_interval": "1m", "tests": [
        {"interval": "1m", "input_series": series, "promql_expr_test": tests}
    ]}, indent=2))

if __name__ == "__main__":
    main()
