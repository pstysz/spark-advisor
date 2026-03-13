from spark_advisor_models.config import AiSettings, Thresholds

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_THRESHOLDS = Thresholds()
DEFAULT_AI_SETTINGS = AiSettings()

NATS_FETCH_JOB_SUBJECT = "fetch.job"
NATS_ANALYZE_REQUEST_SUBJECT = "analyze.request"
NATS_ANALYZE_AGENT_REQUEST_SUBJECT = "analyze.agent.request"
NATS_ANALYZE_RESULT_SUBJECT = "analyze.result"
NATS_LIST_APPLICATIONS_SUBJECT = "list.applications"
