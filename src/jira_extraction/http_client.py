"""HTTP client helpers for communicating with Jira."""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional

import httpx

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class RetryConfig:
    """Configuration for HTTP retry behaviour."""

    max_attempts: int = 5
    backoff_factor: float = 0.5
    max_backoff: float = 10.0


class JiraHTTPClient:
    """Wrapper around :class:`httpx.Client` with Jira specific defaults."""

    def __init__(
        self,
        *,
        base_url: str,
        pat: str,
        ca_bundle: str | bool | None = None,
        timeout: tuple[float, float] = (5.0, 30.0),
        retry_config: RetryConfig | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {pat}"}
        verify: str | bool
        if ca_bundle:
            verify = ca_bundle
        elif ca_bundle is False:
            verify = False
        else:
            verify = True

        self._client = httpx.Client(
            base_url=base_url,
            timeout=httpx.Timeout(connect=timeout[0], read=timeout[1], write=timeout[1], pool=None),
            verify=verify,
            headers=headers,
            transport=transport,
        )
        self._retry_config = retry_config or RetryConfig()

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "JiraHTTPClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("POST", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        attempt = 0
        delay = self._retry_config.backoff_factor
        while True:
            attempt += 1
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:  # network level retry
                if attempt >= self._retry_config.max_attempts:
                    LOGGER.error("HTTP request failed", extra={"method": method, "path": path, "error": str(exc)})
                    raise
                self._sleep(delay)
                delay = self._next_delay(delay)
                continue

            if self._should_retry(response) and attempt < self._retry_config.max_attempts:
                LOGGER.warning(
                    "Retrying Jira request",
                    extra={"method": method, "path": path, "status_code": response.status_code, "attempt": attempt},
                )
                self._sleep(delay)
                delay = self._next_delay(delay)
                continue

            response.raise_for_status()
            return response

    def _should_retry(self, response: httpx.Response) -> bool:
        return response.status_code in {429, 502, 503, 504} or response.status_code >= 500

    def _sleep(self, delay: float) -> None:
        jitter = random.uniform(0, delay / 4)
        time.sleep(delay + jitter)

    def _next_delay(self, delay: float) -> float:
        return min(delay * 2, self._retry_config.max_backoff)


__all__ = ["JiraHTTPClient", "RetryConfig"]
