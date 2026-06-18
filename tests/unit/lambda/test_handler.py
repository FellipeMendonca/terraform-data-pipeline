"""Unit tests for the Lambda handler module."""

import importlib
from unittest.mock import MagicMock, patch

import pytest

# 'lambda' is a Python reserved keyword, so we use importlib to import the module
_handler = importlib.import_module("src.lambda.handler")
_determine_status = _handler._determine_status
_fetch_pokemon_list = _handler._fetch_pokemon_list
_is_timeout_threshold_reached = _handler._is_timeout_threshold_reached
_process_pokemon = _handler._process_pokemon
lambda_handler = _handler.lambda_handler


class TestIsTimeoutThresholdReached:
    """Tests for the _is_timeout_threshold_reached function."""

    def test_not_reached_when_plenty_of_time(self):
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 600_000  # 10 min left
        total_timeout_ms = 900_000  # 15 min total

        assert _is_timeout_threshold_reached(context, total_timeout_ms) is False

    def test_reached_when_below_20_percent(self):
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 100_000  # 100s left
        total_timeout_ms = 900_000  # 15 min total; 20% = 180s

        assert _is_timeout_threshold_reached(context, total_timeout_ms) is True

    def test_exactly_at_threshold(self):
        context = MagicMock()
        # 20% of 900_000 = 180_000
        context.get_remaining_time_in_millis.return_value = 180_000
        total_timeout_ms = 900_000

        # remaining == threshold, not strictly less than
        assert _is_timeout_threshold_reached(context, total_timeout_ms) is False

    def test_just_below_threshold(self):
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 179_999
        total_timeout_ms = 900_000

        assert _is_timeout_threshold_reached(context, total_timeout_ms) is True


class TestDetermineStatus:
    """Tests for the _determine_status function."""

    def test_success_when_all_fetched(self):
        assert _determine_status(10, [], False, 10) == "success"

    def test_partial_failure_when_some_failed(self):
        assert _determine_status(8, ["pikachu", "raichu"], False, 10) == "partial_failure"

    def test_partial_failure_when_timeout_reached(self):
        assert _determine_status(5, [], True, 10) == "partial_failure"

    def test_failure_when_none_succeeded(self):
        assert _determine_status(0, ["bulbasaur"], False, 1) == "failure"

    def test_failure_when_all_failed_no_timeout(self):
        # All pokemon attempted but all failed, no timeout
        assert _determine_status(0, ["a", "b", "c"], False, 3) == "failure"

    def test_partial_failure_when_timeout_with_zero_success(self):
        # Timeout reached before any could be processed
        assert _determine_status(0, [], True, 5) == "partial_failure"

    def test_success_when_list_is_empty(self):
        # No pokemon to fetch = trivially successful
        assert _determine_status(0, [], False, 0) == "success"


class TestFetchPokemonList:
    """Tests for the _fetch_pokemon_list function."""

    def test_returns_list_on_success(self):
        client = MagicMock()
        client.get_pokemon_list.return_value = [
            {"name": "bulbasaur", "url": "..."},
            {"name": "charmander", "url": "..."},
        ]
        result = _fetch_pokemon_list(client)
        assert result == [
            {"name": "bulbasaur", "url": "..."},
            {"name": "charmander", "url": "..."},
        ]

    def test_returns_none_on_exception(self):
        client = MagicMock()
        client.get_pokemon_list.side_effect = RuntimeError("Network error")
        result = _fetch_pokemon_list(client)
        assert result is None


class TestProcessPokemon:
    """Tests for the _process_pokemon function."""

    def _make_context(self, remaining_ms: int):
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = remaining_ms
        return context

    @patch("src.lambda.handler.write_pokemon_data")
    def test_all_pokemon_succeed(self, mock_write):
        mock_write.return_value = True
        client = MagicMock()
        client.get_pokemon_details.return_value = {"id": 1, "name": "bulbasaur"}

        pokemon_list = [{"name": "bulbasaur"}, {"name": "charmander"}]
        context = self._make_context(800_000)

        count, failed, timeout = _process_pokemon(
            client, pokemon_list, "test-bucket", "2024-01-15", context, 900_000
        )

        assert count == 2
        assert failed == []
        assert timeout is False

    @patch("src.lambda.handler.write_pokemon_data")
    def test_individual_failure_continues_processing(self, mock_write):
        mock_write.return_value = True
        client = MagicMock()
        # First pokemon fails, second succeeds
        client.get_pokemon_details.side_effect = [None, {"id": 2, "name": "charmander"}]

        pokemon_list = [{"name": "bulbasaur"}, {"name": "charmander"}]
        context = self._make_context(800_000)

        count, failed, timeout = _process_pokemon(
            client, pokemon_list, "test-bucket", "2024-01-15", context, 900_000
        )

        assert count == 1
        assert failed == ["bulbasaur"]
        assert timeout is False

    @patch("src.lambda.handler.write_pokemon_data")
    def test_s3_write_failure_marks_pokemon_as_failed(self, mock_write):
        mock_write.return_value = False
        client = MagicMock()
        client.get_pokemon_details.return_value = {"id": 1, "name": "bulbasaur"}

        pokemon_list = [{"name": "bulbasaur"}]
        context = self._make_context(800_000)

        count, failed, timeout = _process_pokemon(
            client, pokemon_list, "test-bucket", "2024-01-15", context, 900_000
        )

        assert count == 0
        assert failed == ["bulbasaur"]
        assert timeout is False

    @patch("src.lambda.handler.write_pokemon_data")
    def test_timeout_threshold_stops_processing(self, mock_write):
        mock_write.return_value = True
        client = MagicMock()
        client.get_pokemon_details.return_value = {"id": 1, "name": "bulbasaur"}

        pokemon_list = [{"name": "bulbasaur"}, {"name": "charmander"}, {"name": "squirtle"}]
        # Already past threshold
        context = self._make_context(100_000)  # < 20% of 900_000

        count, failed, timeout = _process_pokemon(
            client, pokemon_list, "test-bucket", "2024-01-15", context, 900_000
        )

        assert count == 0
        assert timeout is True


class TestLambdaHandler:
    """Integration tests for the lambda_handler function."""

    @patch.dict("os.environ", {"S3_BUCKET_NAME": "test-bucket"})
    @patch("src.lambda.handler.PokeAPIClient")
    @patch("src.lambda.handler.write_pokemon_data")
    def test_successful_execution(self, mock_write, mock_client_cls):
        mock_write.return_value = True
        mock_client = MagicMock()
        mock_client.get_pokemon_list.return_value = [{"name": "bulbasaur"}]
        mock_client.get_pokemon_details.return_value = {"id": 1, "name": "bulbasaur"}
        mock_client_cls.return_value = mock_client

        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 900_000

        event = {"execution_date": "2024-01-15"}
        result = lambda_handler(event, context)

        assert result["status"] == "success"
        assert result["pokemon_count"] == 1
        assert result["failed_pokemon"] == []
        assert result["s3_prefix"] == "bronze/2024/01/15/"

    @patch.dict("os.environ", {"S3_BUCKET_NAME": "test-bucket"})
    @patch("src.lambda.handler.PokeAPIClient")
    @patch("src.lambda.handler.write_pokemon_data")
    def test_partial_failure_on_individual_pokemon(self, mock_write, mock_client_cls):
        mock_write.return_value = True
        mock_client = MagicMock()
        mock_client.get_pokemon_list.return_value = [
            {"name": "bulbasaur"},
            {"name": "charmander"},
        ]
        mock_client.get_pokemon_details.side_effect = [
            None,  # bulbasaur fails
            {"id": 4, "name": "charmander"},  # charmander succeeds
        ]
        mock_client_cls.return_value = mock_client

        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 900_000

        event = {"execution_date": "2024-01-15"}
        result = lambda_handler(event, context)

        assert result["status"] == "partial_failure"
        assert result["pokemon_count"] == 1
        assert result["failed_pokemon"] == ["bulbasaur"]

    @patch.dict("os.environ", {"S3_BUCKET_NAME": "test-bucket"})
    @patch("src.lambda.handler.PokeAPIClient")
    def test_failure_when_list_fetch_fails(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get_pokemon_list.side_effect = RuntimeError("API down")
        mock_client_cls.return_value = mock_client

        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = 900_000

        event = {"execution_date": "2024-01-15"}
        result = lambda_handler(event, context)

        assert result["status"] == "failure"
        assert result["pokemon_count"] == 0

    def test_failure_when_no_execution_date(self):
        context = MagicMock()
        event = {}
        result = lambda_handler(event, context)

        assert result["status"] == "failure"
        assert result["pokemon_count"] == 0

    @patch.dict("os.environ", {}, clear=True)
    def test_failure_when_no_bucket_env_var(self):
        context = MagicMock()
        event = {"execution_date": "2024-01-15"}
        result = lambda_handler(event, context)

        assert result["status"] == "failure"
        assert result["pokemon_count"] == 0

    @patch.dict("os.environ", {"S3_BUCKET_NAME": "test-bucket"})
    @patch("src.lambda.handler.PokeAPIClient")
    @patch("src.lambda.handler.write_pokemon_data")
    def test_timeout_returns_partial_failure(self, mock_write, mock_client_cls):
        mock_write.return_value = True
        mock_client = MagicMock()
        mock_client.get_pokemon_list.return_value = [
            {"name": "bulbasaur"},
            {"name": "charmander"},
        ]
        mock_client.get_pokemon_details.return_value = {"id": 1, "name": "bulbasaur"}
        mock_client_cls.return_value = mock_client

        context = MagicMock()
        # First call captures the total timeout, subsequent calls show low remaining
        context.get_remaining_time_in_millis.side_effect = [900_000, 100_000, 100_000]

        event = {"execution_date": "2024-01-15"}
        result = lambda_handler(event, context)

        assert result["status"] == "partial_failure"
        assert result["s3_prefix"] == "bronze/2024/01/15/"
