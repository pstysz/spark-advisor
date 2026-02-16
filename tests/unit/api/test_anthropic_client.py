from unittest.mock import MagicMock, patch

import pytest

from spark_advisor.api.anthropic_client import AnthropicClient


class TestAnthropicClient:
    def test_requires_api_key(self) -> None:
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ValueError, match="ANTHROPIC_API_KEY"),
        ):
            AnthropicClient()

    def test_must_be_used_in_with_block(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            client = AnthropicClient()
            with pytest.raises(RuntimeError, match="within 'with' block"):
                client.create_message(
                    model="test",
                    max_tokens=100,
                    system="test",
                    messages=[],
                    tools=[],
                    tool_choice={"type": "auto"},  # type: ignore[arg-type]
                )

    @patch("spark_advisor.api.anthropic_client.anthropic.Anthropic")
    def test_context_manager_creates_and_closes_client(
        self, mock_anthropic_cls: MagicMock
    ) -> None:
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            with AnthropicClient():
                mock_anthropic_cls.assert_called_once_with(api_key="sk-test")
            mock_client.close.assert_called_once()
