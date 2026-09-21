from pathlib import Path

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1

def test_run_pipeline_with_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"

    data = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "Payment request processed successfully"
        },
        {
            "timestamp": "2026-09-20T10:05:00",
            "service": "payment-service",
            "response_time_ms": 610,
            "cpu_percent": 75,
            "memory_percent": 70,
            "log_level": "ERROR",
            "message": "Payment service timeout"
        }
    ]

    import json

    data_file.write_text(json.dumps(data), encoding="utf-8")

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["events_consumed"] == []


def test_run_pipeline_with_no_anomalies(tmp_path):
    data_file = tmp_path / "service_data.json"

    data = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "Payment request processed successfully"
        }
    ]

    import json

    data_file.write_text(json.dumps(data), encoding="utf-8")

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 1
    assert result["anomalies_detected"] == []
    assert result["events_consumed"] == []


def test_load_data(tmp_path):
    import json
    from src.aiops_pipeline import load_data

    data_file = tmp_path / "data.json"

    data = [
        {
            "service": "payment-service",
            "response_time_ms": 100
        }
    ]

    data_file.write_text(json.dumps(data), encoding="utf-8")

    result = load_data(str(data_file))

    assert result == data


def test_anomaly_detector_detects_cpu_memory_and_warning():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "order-service",
        "response_time_ms": 100,
        "cpu_percent": 90,
        "memory_percent": 90,
        "log_level": "WARNING",
        "message": "Resource usage warning"
    }

    event = detector.detect(record)

    assert event is not None
    assert "High CPU utilization" in event["reasons"]
    assert "High memory utilization" in event["reasons"]
    assert "Error log detected" in event["reasons"]


def test_area_of_circle_negative_radius():
    import pytest
    from src.calculations import area_of_circle

    with pytest.raises(ValueError, match="Radius cannot be negative"):
        area_of_circle(-1)


def test_fibonacci_negative_number():
    import pytest
    from src.calculations import get_nth_fibonacci

    with pytest.raises(ValueError, match="n cannot be negative"):
        get_nth_fibonacci(-1)


def test_fibonacci_larger_number():
    from src.calculations import get_nth_fibonacci

    assert get_nth_fibonacci(10) == 55


def test_producer_rejects_empty_event():
    topic = EventTopic("test-events")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []

def test_topic_clear_removes_messages():
    topic = EventTopic("test-events")

    topic.publish({"type": "ANOMALY"})
    assert len(topic.get_messages()) == 1

    topic.clear()

    assert topic.get_messages() == []

def test_aiops_pipeline_main_block(tmp_path):
    import json
    import subprocess
    import sys

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    data_file = data_dir / "service_data.json"

    data = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 610,
            "cpu_percent": 75,
            "memory_percent": 70,
            "log_level": "ERROR",
            "message": "Payment service timeout"
        }
    ]

    data_file.write_text(json.dumps(data), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "src/aiops_pipeline.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True
    )

    assert result.returncode == 0


def test_aiops_pipeline_main_block():
    import subprocess
    import sys
    import os

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    result = subprocess.run(
        [sys.executable, "src/aiops_pipeline.py"],
        capture_output=True,
        text=True,
        env=env
    )

    assert result.returncode == 0
    assert "AIOps Pipeline Result" in result.stdout
    assert "Records processed:" in result.stdout
    assert "Anomalies detected:" in result.stdout
    assert "Events consumed:" in result.stdout
    assert "Detected Events:" in result.stdout

