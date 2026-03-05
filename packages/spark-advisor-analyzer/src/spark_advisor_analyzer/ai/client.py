import os

import anthropic
import httpx
from anthropic.types import Message, MessageParam, ToolChoiceAutoParam, ToolChoiceToolParam, ToolParam


class AnthropicClient:
    def __init__(self, timeout: float) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY env variable is required for AI analysis")
        self._api_key = api_key
        self._timeout = timeout
        self._client: anthropic.Anthropic | None = None

    def open(self) -> "AnthropicClient":
        self._client = anthropic.Anthropic(
            api_key=self._api_key,
            timeout=httpx.Timeout(self._timeout),
        )
        return self

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "AnthropicClient":
        return self.open()

    def __exit__(self, *_: object) -> None:
        self.close()

    def create_message(
        self,
        *,
        model: str,
        max_tokens: int,
        system: str,
        messages: list[MessageParam],
        tools: list[ToolParam],
        tool_choice: ToolChoiceToolParam | ToolChoiceAutoParam,
    ) -> Message:
        if self._client is None:
            raise RuntimeError("Client not initialized — call open() or use as context manager")
        return self._client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
        )
