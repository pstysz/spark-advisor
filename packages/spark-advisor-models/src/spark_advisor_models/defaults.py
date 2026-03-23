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
NATS_STORAGE_FETCH_HDFS_SUBJECT = "storage.fetch.hdfs"
NATS_STORAGE_FETCH_S3_SUBJECT = "storage.fetch.s3"
NATS_STORAGE_FETCH_GCS_SUBJECT = "storage.fetch.gcs"
NATS_FETCH_JOB_K8S_SUBJECT = "job.fetch.k8s"
NATS_K8S_APPLICATIONS_LIST_SUBJECT = "k8s.applications.list"

DEFAULT_EVENT_LOGS_DIR = "/spark-events"

STORAGE_FETCH_SUBJECTS: dict[str, str] = {
    "hdfs": NATS_STORAGE_FETCH_HDFS_SUBJECT,
    "s3": NATS_STORAGE_FETCH_S3_SUBJECT,
    "gcs": NATS_STORAGE_FETCH_GCS_SUBJECT,
}
