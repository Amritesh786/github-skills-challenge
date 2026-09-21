import json
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


def test_warning_logs_are_flagged_as_anomalies():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "search-service",
        "response_time_ms": 150,
        "cpu_percent": 40,
        "memory_percent": 45,
        "log_level": "WARNING",
        "message": "Warning from downstream service"
    }

    event = detector.detect(record)

    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_run_pipeline_processes_service_records(tmp_path):
    file_path = tmp_path / "records.json"

    records = [
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

    file_path.write_text(json.dumps(records), encoding="utf-8")

    result = run_pipeline(str(file_path))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["anomalies_detected"][0]["service"] == "payment-service"
    assert result["events_consumed"] == []


def test_event_topic_can_clear_messages():
    topic = EventTopic("demo-topic")
    topic.publish({"type": "TEST"})

    topic.clear()

    assert topic.get_messages() == []