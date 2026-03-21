from pydantic import BaseModel, ConfigDict, Field


class SparkConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw: dict[str, str] = Field(default_factory=dict)

    def get(self, key: str, default: str = "") -> str:
        return self.raw.get(key, default)

    @property
    def executor_memory(self) -> str:
        return self.get("spark.executor.memory", "1g")

    @property
    def executor_cores(self) -> int:
        return int(self.get("spark.executor.cores", "1"))

    @property
    def shuffle_partitions(self) -> int:
        return int(self.get("spark.sql.shuffle.partitions", "200"))

    @property
    def dynamic_allocation_enabled(self) -> bool:
        return self.get("spark.dynamicAllocation.enabled", "false").lower() == "true"

    @property
    def aqe_enabled(self) -> bool:
        return self.get("spark.sql.adaptive.enabled", "false").lower() == "true"

    @property
    def serializer(self) -> str:
        return self.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    @property
    def broadcast_join_threshold(self) -> int:
        return int(self.get("spark.sql.autoBroadcastJoinThreshold", "10485760"))

    @property
    def dynamic_allocation_min_executors(self) -> str:
        return self.get("spark.dynamicAllocation.minExecutors")

    @property
    def dynamic_allocation_max_executors(self) -> str:
        return self.get("spark.dynamicAllocation.maxExecutors")

    @property
    def executor_memory_overhead(self) -> str:
        return self.get("spark.executor.memoryOverhead")

    @property
    def driver_memory(self) -> str:
        return self.get("spark.driver.memory", "1g")

    @property
    def driver_memory_overhead(self) -> str:
        return self.get("spark.driver.memoryOverhead")

    @property
    def has_explicit_aqe_config(self) -> bool:
        return "spark.sql.adaptive.enabled" in self.raw
