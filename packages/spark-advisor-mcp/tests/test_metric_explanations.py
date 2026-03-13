from spark_advisor_mcp.metric_explanations import METRIC_DESCRIPTIONS, format_metric_explanation
from spark_advisor_models.config import Thresholds
from spark_advisor_models.defaults import DEFAULT_THRESHOLDS


class TestMetricDescriptions:
    def test_all_metrics_have_descriptions(self) -> None:
        for name, desc in METRIC_DESCRIPTIONS.items():
            assert desc, f"Metric {name} has empty description"


class TestFormatMetricExplanation:
    def test_known_metric(self) -> None:
        result = format_metric_explanation("gc_time_percent", 35.0, DEFAULT_THRESHOLDS)

        assert "## Metric: `gc_time_percent`" in result
        assert "35.00" in result
        assert "garbage collection" in result

    def test_unknown_metric(self) -> None:
        result = format_metric_explanation("nonexistent_metric", 0.0, DEFAULT_THRESHOLDS)

        assert "Unknown metric" in result
        assert "Known metrics:" in result

    def test_bytes_metric_formatted(self) -> None:
        result = format_metric_explanation("spill_to_disk_bytes", 1073741824.0, DEFAULT_THRESHOLDS)

        assert "1.0 GB" in result

    def test_all_known_metrics_produce_output(self) -> None:
        for metric_name in METRIC_DESCRIPTIONS:
            result = format_metric_explanation(metric_name, 100.0, DEFAULT_THRESHOLDS)
            assert f"## Metric: `{metric_name}`" in result


class TestAssessment:
    def test_critical_gc(self) -> None:
        result = format_metric_explanation("gc_time_percent", 50.0, DEFAULT_THRESHOLDS)

        assert "### Assessment" in result
        assert "Critical" in result

    def test_warning_gc(self) -> None:
        result = format_metric_explanation("gc_time_percent", 25.0, DEFAULT_THRESHOLDS)

        assert "### Assessment" in result
        assert "Warning" in result

    def test_healthy_gc(self) -> None:
        result = format_metric_explanation("gc_time_percent", 5.0, DEFAULT_THRESHOLDS)

        assert "### Assessment" in result
        assert "Healthy" in result

    def test_critical_spill(self) -> None:
        result = format_metric_explanation("spill_to_disk_bytes", 2 * 1024 * 1024 * 1024, DEFAULT_THRESHOLDS)

        assert "Critical" in result

    def test_warning_spill(self) -> None:
        result = format_metric_explanation("spill_to_disk_bytes", 200 * 1024 * 1024, DEFAULT_THRESHOLDS)

        assert "Warning" in result

    def test_healthy_spill(self) -> None:
        result = format_metric_explanation("spill_to_disk_bytes", 10 * 1024 * 1024, DEFAULT_THRESHOLDS)

        assert "Healthy" in result

    def test_critical_skew(self) -> None:
        result = format_metric_explanation("data_skew_ratio", 15.0, DEFAULT_THRESHOLDS)

        assert "Critical" in result

    def test_warning_skew(self) -> None:
        result = format_metric_explanation("data_skew_ratio", 7.0, DEFAULT_THRESHOLDS)

        assert "Warning" in result

    def test_healthy_skew(self) -> None:
        result = format_metric_explanation("data_skew_ratio", 2.0, DEFAULT_THRESHOLDS)

        assert "Healthy" in result

    def test_no_assessment_for_metric_without_thresholds(self) -> None:
        result = format_metric_explanation("shuffle_read_bytes", 1000.0, DEFAULT_THRESHOLDS)

        assert "### Assessment" not in result

    def test_slot_utilization_has_description_no_assessment(self) -> None:
        result = format_metric_explanation("slot_utilization_percent", 30.0, DEFAULT_THRESHOLDS)

        assert "## Metric: `slot_utilization_percent`" in result
        assert "### Assessment" not in result

    def test_warning_memory_utilization(self) -> None:
        result = format_metric_explanation("memory_utilization_percent", 85.0, DEFAULT_THRESHOLDS)

        assert "Warning" in result

    def test_custom_thresholds_are_respected(self) -> None:
        custom = Thresholds(gc_warning_percent=50.0, gc_critical_percent=80.0)

        result = format_metric_explanation("gc_time_percent", 35.0, custom)

        assert "Healthy" in result

    def test_custom_thresholds_trigger_warning(self) -> None:
        custom = Thresholds(gc_warning_percent=10.0, gc_critical_percent=20.0)

        result = format_metric_explanation("gc_time_percent", 15.0, custom)

        assert "Warning" in result

    def test_critical_memory_utilization(self) -> None:
        result = format_metric_explanation("memory_utilization_percent", 97.0, DEFAULT_THRESHOLDS)

        assert "Critical" in result
