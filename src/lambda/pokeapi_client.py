"""PokeAPI client with rate limiting and retry logic.

This module provides a rate-limited HTTP client for the PokeAPI REST API,
with exponential backoff retry logic and pagination support.

Classes:
    RateLimiter: Enforces rate limits (max 100 req/min, 500ms between requests).
    PokeAPIClient: HTTP client for PokeAPI with retry and pagination.
"""

import json
import logging
import time
import urllib.error
import urllib.request
from collections import deque

logger = logging.getLogger(__name__)

BASE_URL = "https://pokeapi.co/api/v2"
REQUEST_TIMEOUT = 30  # seconds per request
MAX_REQUESTS_PER_MINUTE = 100
MIN_INTERVAL_SECONDS = 0.5  # 500ms between consecutive requests
MAX_RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0  # base delay in seconds for exponential backoff


class RateLimiter:
    """Enforces rate limits for API requests.

    Ensures:
    - Maximum 100 requests per minute (sliding window).
    - Minimum 500 milliseconds between consecutive requests.
    """

    def __init__(
        self,
        max_per_minute: int = MAX_REQUESTS_PER_MINUTE,
        min_interval: float = MIN_INTERVAL_SECONDS,
    ):
        self._max_per_minute = max_per_minute
        self._min_interval = min_interval
        self._request_timestamps: deque = deque()
        self._last_request_time: float = 0.0

    def acquire(self) -> None:
        """Block until a request can be made within rate limits.

        This method enforces both the per-minute cap and the minimum
        interval between consecutive requests.
        """
        now = time.monotonic()

        # Enforce minimum interval between consecutive requests
        elapsed_since_last = now - self._last_request_time
        if self._last_request_time > 0 and elapsed_since_last < self._min_interval:
            sleep_time = self._min_interval - elapsed_since_last
            time.sleep(sleep_time)
            now = time.monotonic()

        # Enforce max requests per minute (sliding window)
        self._purge_old_timestamps(now)
        while len(self._request_timestamps) >= self._max_per_minute:
            # Wait until the oldest request in the window expires
            oldest = self._request_timestamps[0]
            wait_time = 60.0 - (now - oldest)
            if wait_time > 0:
                time.sleep(wait_time)
            now = time.monotonic()
            self._purge_old_timestamps(now)

        # Record this request
        self._request_timestamps.append(now)
        self._last_request_time = now

    def _purge_old_timestamps(self, now: float) -> None:
        """Remove timestamps older than 60 seconds from the window."""
        while self._request_timestamps and (now - self._request_timestamps[0]) >= 60.0:
            self._request_timestamps.popleft()


class PokeAPIClient:
    """HTTP client for PokeAPI with rate limiting, retry, and pagination.

    Uses urllib.request for HTTP calls (available in Lambda without
    additional dependencies).
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        rate_limiter: RateLimiter | None = None,
        timeout: int = REQUEST_TIMEOUT,
        max_retries: int = MAX_RETRY_ATTEMPTS,
        retry_base_delay: float = RETRY_BASE_DELAY,
    ):
        self._base_url = base_url.rstrip("/")
        self._rate_limiter = rate_limiter or RateLimiter()
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay

    def get_pokemon_list(self, limit: int = 20) -> list[dict]:
        """Paginate through /api/v2/pokemon to get the complete Pokémon list.

        Args:
            limit: Number of results per page (default 20, max typically 100).

        Returns:
            A list of dicts with 'name' and 'url' for each Pokémon.
        """
        all_pokemon = []
        offset = 0

        while True:
            endpoint = f"/pokemon?offset={offset}&limit={limit}"
            data = self._request(endpoint)

            if data is None:
                logger.error("Failed to fetch pokemon list at offset=%d", offset)
                break

            results = data.get("results", [])
            all_pokemon.extend(results)

            # Check if there are more pages
            if data.get("next") is None:
                break

            offset += limit

        logger.info("Fetched %d Pokémon from list endpoint", len(all_pokemon))
        return all_pokemon

    def get_pokemon_details(self, name: str) -> dict | None:
        """Fetch detailed data for a single Pokémon by name.

        Args:
            name: The Pokémon name (e.g., "bulbasaur").

        Returns:
            A dict with the Pokémon's full data, or None if all retries fail.
        """
        endpoint = f"/pokemon/{name}"
        data = self._request(endpoint)

        if data is None:
            logger.error("Failed to fetch details for Pokémon: %s", name)

        return data

    def _request(self, endpoint: str) -> dict | None:
        """Make an HTTP GET request with rate limiting and retry logic.

        Args:
            endpoint: The API path (e.g., "/pokemon?offset=0&limit=20").

        Returns:
            Parsed JSON response as a dict, or None if all retries exhausted.
        """
        url = f"{self._base_url}{endpoint}"

        for attempt in range(1, self._max_retries + 1):
            self._rate_limiter.acquire()

            try:
                request = urllib.request.Request(url, method="GET")
                request.add_header("Accept", "application/json")
                request.add_header("User-Agent", "pokemon-data-pipeline/1.0")

                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    body = response.read().decode("utf-8")
                    return json.loads(body)

            except urllib.error.HTTPError as e:
                status_code = e.code
                logger.warning(
                    "HTTP error %d for endpoint %s (attempt %d/%d)",
                    status_code,
                    endpoint,
                    attempt,
                    self._max_retries,
                )
                if attempt < self._max_retries:
                    self._backoff(attempt)
                else:
                    logger.error(
                        "Max retries reached for endpoint %s with HTTP %d",
                        endpoint,
                        status_code,
                    )
                    return None

            except urllib.error.URLError as e:
                logger.warning(
                    "URL error for endpoint %s (attempt %d/%d): %s",
                    endpoint,
                    attempt,
                    self._max_retries,
                    str(e.reason),
                )
                if attempt < self._max_retries:
                    self._backoff(attempt)
                else:
                    logger.error(
                        "Max retries reached for endpoint %s: %s",
                        endpoint,
                        str(e.reason),
                    )
                    return None

            except TimeoutError:
                logger.warning(
                    "Timeout (>%ds) for endpoint %s (attempt %d/%d)",
                    self._timeout,
                    endpoint,
                    attempt,
                    self._max_retries,
                )
                if attempt < self._max_retries:
                    self._backoff(attempt)
                else:
                    logger.error(
                        "Max retries reached for endpoint %s due to timeout",
                        endpoint,
                    )
                    return None

            except OSError as e:
                # Catch socket.timeout and other OS-level network errors
                logger.warning(
                    "Network error for endpoint %s (attempt %d/%d): %s",
                    endpoint,
                    attempt,
                    self._max_retries,
                    str(e),
                )
                if attempt < self._max_retries:
                    self._backoff(attempt)
                else:
                    logger.error(
                        "Max retries reached for endpoint %s: %s",
                        endpoint,
                        str(e),
                    )
                    return None

        return None

    def _backoff(self, attempt: int) -> None:
        """Apply exponential backoff delay.

        Delay = base_delay * 2^(attempt-1)
        Attempt 1 → 1s, Attempt 2 → 2s, Attempt 3 → 4s
        """
        delay = self._retry_base_delay * (2 ** (attempt - 1))
        logger.info("Backing off for %.1fs before retry", delay)
        time.sleep(delay)
