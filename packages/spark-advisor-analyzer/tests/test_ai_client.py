from unittest.mock import ANY, MagicMock, patch

import pytest

from spark_advisor_analyzer.ai.client import AnthropicClient


class TestAnthropicClient:
    def test_requires_api_key(self) -> None:
        with (
            patch.dict("os.environ", {}, clear=True),
            pytest.raises(ValueError, match="ANTHROPIC_API_KEY"),
        ):
            AnthropicClient(timeout=90.0)

    def test_must_be_used_in_with_block(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            client = AnthropicClient(timeout=90.0)
            with pytest.raises(RuntimeError, match="Client not initialized"):
                client.create_message(
                    model="test",
                    max_tokens=100,
                    system="test",
                    messages=[],
                    tools=[],
                    tool_choice={"type": "auto"},  # type: ignore[arg-type]
                )

    @patch("spark_advisor_analyzer.ai.client.anthropic.Anthropic")
    def test_context_manager_creates_and_closes_client(self, mock_anthropic_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            with AnthropicClient(timeout=90.0):
                mock_anthropic_cls.assert_called_once_with(api_key="sk-test", timeout=ANY)
            mock_client.close.assert_called_once()
