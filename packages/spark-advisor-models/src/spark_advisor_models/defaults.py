from spark_advisor_models.config import AiSettings, Thresholds

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_THRESHOLDS = Thresholds()
DEFAULT_AI_SETTINGS = AiSettings()

NATS_FETCH_JOB_SUBJECT = "job.fetch"
NATS_ANALYSIS_RUN_SUBJECT = "analysis.run"
NATS_ANALYSIS_RUN_AGENT_SUBJECT = "analysis.run.agent"
NATS_ANALYSIS_RESULT_SUBJECT = "analysis.result"
NATS_APPLICATIONS_LIST_SUBJECT = "apps.list"
NATS_ANALYSIS_SUBMIT_SUBJECT = "analysis.submit"
NATS_STORAGE_FETCH_SUBJECT = "storage.fetch"
NATS_FETCH_JOB_K8S_SUBJECT = "job.fetch.k8s"
