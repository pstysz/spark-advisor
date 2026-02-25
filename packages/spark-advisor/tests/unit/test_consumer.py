from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from spark_advisor.analysis.advice_processor import AdviceProcessor
from spark_advisor.model.output import AnalysisResult
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata
from spark_advisor_shared.util.threading import background_worker
from tests.factories import make_job, make_rule_result


def _make_job_envelope() -> KafkaEnvelope:
    job = make_job()
    return KafkaEnvelope(
        metadata=MessageMetadata(source="hs-poller"),
        payload=job.model_dump(mode="json"),
    )


def _make_analysis_result() -> AnalysisResult:
    job = make_job()
    return AnalysisResult(
        app_id=job.app_id,
        job=job,
        rule_results=[make_rule_result()],
    )


class TestAnalysisConsumer:
    def test_processes_message_and_commits(self) -> None:
        source = MagicMock()
        sink = MagicMock()
        advisor = MagicMock()

        envelope = _make_job_envelope()
        advisor.run.return_value = _make_analysis_result()

        call_count = 0

        def poll_side_effect(timeout: float = 1.0) -> KafkaEnvelope | None:
            nonlocal call_count
            call_count += 1
            return envelope if call_count == 1 else None

        source.poll.side_effect = poll_side_effect

        consumer = AdviceProcessor(source, sink, advisor)
        with background_worker(consumer.run):
            time.sleep(0.5)

        advisor.run.assert_called_once()
        source.commit.assert_called_once()
        sink.send.assert_called_once()

        sent_envelope = sink.send.call_args[0][0]
        assert sent_envelope.metadata.source == "spark-advisor"

    def test_skips_none_messages(self) -> None:
        source = MagicMock()
        sink = MagicMock()
        advisor = MagicMock()
        source.poll.return_value = None

        consumer = AdviceProcessor(source, sink, advisor)
        with background_worker(consumer.run):
            time.sleep(0.3)

        advisor.run.assert_not_called()
        source.commit.assert_not_called()
        sink.send.assert_not_called()

    @patch("spark_advisor.analysis.advice_processor.logger")
    def test_transient_error_does_not_commit(self, mock_logger: MagicMock) -> None:
        source = MagicMock()
        sink = MagicMock()
        advisor = MagicMock()

        envelope = _make_job_envelope()
        advisor.run.side_effect = RuntimeError("LLM unavailable")

        call_count = 0

        def poll_side_effect(timeout: float = 1.0) -> KafkaEnvelope | None:
            nonlocal call_count
            call_count += 1
            return envelope if call_count == 1 else None

        source.poll.side_effect = poll_side_effect

        consumer = AdviceProcessor(source, sink, advisor)
        with background_worker(consumer.run):
            time.sleep(0.5)

        source.commit.assert_not_called()
        sink.send.assert_not_called()
        mock_logger.exception.assert_called_once()

    @patch("spark_advisor.analysis.advice_processor.logger")
    def test_validation_error_commits_and_skips(self, mock_logger: MagicMock) -> None:
        source = MagicMock()
        sink = MagicMock()
        advisor = MagicMock()

        bad_envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="hs-poller"),
            payload={"not_a_valid_job": True},
        )

        call_count = 0

        def poll_side_effect(timeout: float = 1.0) -> KafkaEnvelope | None:
            nonlocal call_count
            call_count += 1
            return bad_envelope if call_count == 1 else None

        source.poll.side_effect = poll_side_effect

        consumer = AdviceProcessor(source, sink, advisor)
        with background_worker(consumer.run):
            time.sleep(0.5)

        source.commit.assert_called_once()
        sink.send.assert_not_called()
        mock_logger.exception.assert_called_once()

    def test_result_envelope_contains_analysis_payload(self) -> None:
        source = MagicMock()
        sink = MagicMock()
        advisor = MagicMock()

        envelope = _make_job_envelope()
        advisor.run.return_value = _make_analysis_result()

        consumer = AdviceProcessor(source, sink, advisor)
        consumer._process(envelope)

        sent_envelope = sink.send.call_args[0][0]
        assert "rule_results" in sent_envelope.payload
        assert sent_envelope.payload["rule_results"][0]["rule_id"] == "data_skew"
