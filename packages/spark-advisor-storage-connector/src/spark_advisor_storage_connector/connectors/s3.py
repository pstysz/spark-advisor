from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Any

from aiobotocore.session import AioSession  # type: ignore[import-untyped]

from spark_advisor_storage_connector.config import S3Settings  # noqa: TC001
from spark_advisor_storage_connector.connectors.protocol import EventLogRef


class S3Connector:
    def __init__(self, settings: S3Settings) -> None:
        self._bucket = settings.bucket
        self._prefix = settings.prefix
        self._region = settings.region
        self._endpoint_url = settings.endpoint_url
        self._session: AioSession = AioSession()
        self._client: Any = None

    async def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        kwargs: dict[str, Any] = {"region_name": self._region}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        self._client = await self._session.create_client("s3", **kwargs).__aenter__()
        return self._client

    async def list_event_logs(self) -> list[EventLogRef]:
        client = await self._ensure_client()
        paginator = client.get_paginator("list_objects_v2")

        refs: list[EventLogRef] = []
        async for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                name = key.rsplit("/", 1)[-1]
                if not name:
                    continue
                modified: datetime = obj["LastModified"]
                refs.append(
                    EventLogRef(
                        path=key,
                        name=name,
                        size=obj.get("Size", 0),
                        modified_at=modified,
                    )
                )
        return refs

    async def read_event_log(self, ref: EventLogRef) -> bytes:
        client = await self._ensure_client()
        response = await client.get_object(Bucket=self._bucket, Key=ref.path)
        async with response["Body"] as stream:
            return await stream.read()  # type: ignore[no-any-return]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
