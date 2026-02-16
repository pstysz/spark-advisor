from anthropic.types import ToolParam

from spark_advisor.config import Thresholds as Thresholds

ANALYSIS_TOOL: ToolParam = {
    "name": "submit_analysis",
    "description": "Submit the Spark job analysis results.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "1-2 sentence overview of the job's health and key issues.",
            },
            "severity": {
                "type": "string",
                "enum": ["critical", "warning", "info"],
                "description": "Overall severity: critical, warning, or info.",
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "priority": {
                            "type": "integer",
                            "description": "Priority rank (1 = highest impact).",
                        },
                        "title": {
                            "type": "string",
                            "description": "Short title for the recommendation.",
                        },
                        "parameter": {
                            "type": "string",
                            "description": (
                                "Spark config parameter (e.g. spark.sql.shuffle.partitions). "
                                'Use "code_change" for non-config recommendations.'
                            ),
                        },
                        "current_value": {
                            "type": "string",
                            "description": "Current value of the parameter.",
                        },
                        "recommended_value": {
                            "type": "string",
                            "description": "Recommended new value.",
                        },
                        "explanation": {
                            "type": "string",
                            "description": "Brief explanation of the mechanism (2-3 sentences).",
                        },
                        "estimated_impact": {
                            "type": "string",
                            "description": (
                                'Quantified estimate (e.g. "~30% reduction in Stage 4 duration").'
                            ),
                        },
                        "risk": {
                            "type": "string",
                            "description": "Potential downsides of this change.",
                        },
                    },
                    "required": [
                        "priority",
                        "title",
                        "parameter",
                        "current_value",
                        "recommended_value",
                        "explanation",
                        "estimated_impact",
                        "risk",
                    ],
                },
            },
            "causal_chain": {
                "type": "string",
                "description": "Description of how problems are related, if applicable.",
            },
        },
        "required": ["summary", "severity", "recommendations", "causal_chain"],
    },
}

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert Apache Spark performance engineer with 15 years of experience \
tuning Spark jobs on YARN and Kubernetes.

Your task: analyze Spark job metrics and generate concrete, actionable recommendations.

RULES:
1. Always provide SPECIFIC config values, not generalities.
   BAD: "Increase partition count"
   GOOD: "Change spark.sql.shuffle.partitions from 200 to 800"

2. Prioritize recommendations from highest to lowest impact. Return at most 7.

3. For each recommendation provide:
   - parameter: the Spark config parameter (use "code_change" for non-config recommendations)
   - current_value: what it is now
   - recommended_value: what it should be
   - explanation: brief explanation of the mechanism (2-3 sentences)
   - estimated_impact: quantified estimate (e.g., "~30% reduction in Stage 4 duration")
   - risk: potential downsides

4. If you see related problems, describe the causal chain.
   Example: "Skew in Stage 3 causes spill in Stage 4, which increases GC time"

5. If no significant issues are found, return severity "info" with a brief summary \
and an empty recommendations list. Do not invent problems.

6. Use the submit_analysis tool to return your results.

TECHNICAL CONTEXT:
- spark.sql.shuffle.partitions default=200; target ~{target_partition_mb}MB per partition
- Spill to disk > 0 means insufficient memory for shuffle/aggregation
- Task duration skew: WARNING if max/median > {skew_warning}x, CRITICAL if > {skew_critical}x
- GC time: WARNING if > {gc_warning}% of task time, CRITICAL if > {gc_critical}%
- Executor CPU utilization < {min_cpu}% = over-provisioning
- AQE (Adaptive Query Execution) can auto-handle skew if enabled (default since Spark 3.2)
"""


def build_system_prompt(thresholds: Thresholds | None = None) -> str:
    t = thresholds or Thresholds()
    return _SYSTEM_PROMPT_TEMPLATE.format(
        target_partition_mb=t.target_partition_size_bytes // (1024 * 1024),
        skew_warning=t.skew_warning_ratio,
        skew_critical=t.skew_critical_ratio,
        gc_warning=t.gc_warning_percent,
        gc_critical=t.gc_critical_percent,
        min_cpu=t.min_cpu_utilization_percent,
    )


SYSTEM_PROMPT = build_system_prompt()
