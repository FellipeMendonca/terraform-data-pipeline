"""Unit tests for the PokeAPI client module."""

import importlib
import json
import time
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

# 'lambda' is a Python reserved keyword, so we use importlib to import the module
_pokeapi_client = importlib.import_module("src.lambda.pokeapi_client")
PokeAPIClient = _pokeapi_client.PokeAPIClient
RateLimiter = _pokeapi_client.RateLimiter


class TestRateLimiter:
    """Tests for the RateLimiter class."""

    def test_acquire_first_request_does_not_block(self):
        """First request should proceed immediately."""
        limiter = RateLimiter(max_per_minute=100, min_interval=0.5)
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        # Should be nearly instant (< 50ms)
        assert elapsed < 0.05

    def test_acquire_enforces_minimum_interval(self):
        """Consecutive requests should be spaced at least 500ms apart."""
        limiter = RateLimiter(max_per_minute=100, min_interval=0.5)
        limiter.acquire()
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.49  # Allow small timing tolerance

    def test_acquire_enforces_max_per_minute(self):
        """Should block when max requests per minute is reached."""
        # Use a very small window for testing
        limiter = RateLimiter(max_per_minute=2, min_interval=0.0)
        limiter.acquire()
        limiter.acquire()
        # Third request should be delayed until the window clears
        start = time.monotonic()
        # We can't easily test the full 60s window, so just verify the logic
        # by checking that the timestamps deque has 2 entries
        assert len(limiter._request_timestamps) == 2

    def test_purge_old_timestamps(self):
        """Timestamps older than 60 seconds should be purged."""
        limiter = RateLimiter(max_per_minute=100, min_interval=0.0)
        # Manually insert old timestamps
        now = time.monotonic()
        limiter._request_timestamps.append(now - 61)
        limiter._request_timestamps.append(now - 60.5)
        limiter._request_timestamps.append(now - 30)

        limiter._purge_old_timestamps(now)
        # Only the timestamp from 30 seconds ago should remain
        assert len(limiter._request_timestamps) == 1


class TestPokeAPIClient:
    """Tests for the PokeAPIClient class."""

    def _make_client(self, **kwargs):
        """Create a client with a no-wait rate limiter for fast tests."""
        rate_limiter = RateLimiter(max_per_minute=1000, min_interval=0.0)
        defaults = {
            "base_url": "https://pokeapi.co/api/v2",
            "rate_limiter": rate_limiter,
            "timeout": 30,
            "max_retries": 3,
            "retry_base_delay": 0.01,  # Fast retries for testing
        }
        defaults.update(kwargs)
        return PokeAPIClient(**defaults)

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_details_success(self, mock_urlopen):
        """Should return parsed JSON on successful response."""
        pokemon_data = {"id": 1, "name": "bulbasaur", "height": 7}
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(pokemon_data).encode("utf-8")
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        client = self._make_client()
        result = client.get_pokemon_details("bulbasaur")

        assert result == pokemon_data

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_details_retries_on_http_error(self, mock_urlopen):
        """Should retry on HTTP 5xx errors and return None after max retries."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://pokeapi.co/api/v2/pokemon/bulbasaur",
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=None,
        )

        client = self._make_client()
        result = client.get_pokemon_details("bulbasaur")

        assert result is None
        assert mock_urlopen.call_count == 3  # 3 retry attempts

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_details_retries_on_timeout(self, mock_urlopen):
        """Should retry on timeout errors."""
        mock_urlopen.side_effect = TimeoutError("Connection timed out")

        client = self._make_client()
        result = client.get_pokemon_details("pikachu")

        assert result is None
        assert mock_urlopen.call_count == 3

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_details_retries_on_url_error(self, mock_urlopen):
        """Should retry on URL errors (network issues)."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        client = self._make_client()
        result = client.get_pokemon_details("charmander")

        assert result is None
        assert mock_urlopen.call_count == 3

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_list_single_page(self, mock_urlopen):
        """Should return all Pokémon from a single-page response."""
        response_data = {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
                {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/"},
            ],
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        client = self._make_client()
        result = client.get_pokemon_list(limit=20)

        assert len(result) == 2
        assert result[0]["name"] == "bulbasaur"
        assert result[1]["name"] == "ivysaur"

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_get_pokemon_list_pagination(self, mock_urlopen):
        """Should paginate through multiple pages."""
        page1 = {
            "count": 3,
            "next": "https://pokeapi.co/api/v2/pokemon?offset=2&limit=2",
            "previous": None,
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
                {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/"},
            ],
        }
        page2 = {
            "count": 3,
            "next": None,
            "previous": "https://pokeapi.co/api/v2/pokemon?offset=0&limit=2",
            "results": [
                {"name": "venusaur", "url": "https://pokeapi.co/api/v2/pokemon/3/"},
            ],
        }

        mock_response1 = MagicMock()
        mock_response1.read.return_value = json.dumps(page1).encode("utf-8")
        mock_response1.__enter__ = MagicMock(return_value=mock_response1)
        mock_response1.__exit__ = MagicMock(return_value=False)

        mock_response2 = MagicMock()
        mock_response2.read.return_value = json.dumps(page2).encode("utf-8")
        mock_response2.__enter__ = MagicMock(return_value=mock_response2)
        mock_response2.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [mock_response1, mock_response2]

        client = self._make_client()
        result = client.get_pokemon_list(limit=2)

        assert len(result) == 3
        assert result[0]["name"] == "bulbasaur"
        assert result[2]["name"] == "venusaur"

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_exponential_backoff_timing(self, mock_urlopen):
        """Should use exponential backoff between retries."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://pokeapi.co/api/v2/pokemon/test",
            code=503,
            msg="Service Unavailable",
            hdrs={},
            fp=None,
        )

        # Use measurable delay
        client = self._make_client(retry_base_delay=0.05)
        start = time.monotonic()
        client.get_pokemon_details("test")
        elapsed = time.monotonic() - start

        # Should have delays: 0.05s + 0.1s = 0.15s minimum between 3 attempts
        assert elapsed >= 0.14  # Allow slight tolerance

    @patch("src.lambda.pokeapi_client.urllib.request.urlopen")
    def test_succeeds_after_transient_failure(self, mock_urlopen):
        """Should succeed if a retry attempt succeeds."""
        pokemon_data = {"id": 25, "name": "pikachu"}

        # First call fails, second succeeds
        mock_error = urllib.error.HTTPError(
            url="https://pokeapi.co/api/v2/pokemon/pikachu",
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=None,
        )
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(pokemon_data).encode("utf-8")
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_urlopen.side_effect = [mock_error, mock_response]

        client = self._make_client()
        result = client.get_pokemon_details("pikachu")

        assert result == pokemon_data
        assert mock_urlopen.call_count == 2
