"""A very small subset of the httpx API used for the kata tests."""
from __future__ import annotations

import json as _json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urljoin, urlparse, urlunparse


class HTTPError(Exception):
    """Base HTTP error."""


class HTTPStatusError(HTTPError):
    def __init__(self, message: str, request: "Request", response: "Response") -> None:
        super().__init__(message)
        self.request = request
        self.response = response


class BaseTransport:
    """Base class for transports."""


class MockTransport(BaseTransport):
    def __init__(self, handler: Callable[["Request"], "Response"]) -> None:
        self._handler = handler

    def handle(self, request: "Request") -> "Response":
        return self._handler(request)


@dataclass(slots=True)
class Timeout:
    connect: float = 5.0
    read: float = 5.0
    write: float = 5.0
    pool: Optional[float] = None


class URL:
    def __init__(self, raw: str) -> None:
        self._parsed = urlparse(raw)

    @property
    def path(self) -> str:
        return self._parsed.path

    def __str__(self) -> str:  # pragma: no cover - trivial
        return urlunparse(self._parsed)


class Request:
    def __init__(self, method: str, url: str, *, headers: Mapping[str, str] | None = None, content: bytes | None = None) -> None:
        self.method = method.upper()
        self.url = URL(url)
        self.headers: Dict[str, str] = {k: v for k, v in (headers or {}).items()}
        self.content = content or b""


class Response:
    def __init__(
        self,
        status_code: int,
        *,
        json: Any | None = None,
        content: bytes | None = None,
        headers: Mapping[str, str] | None = None,
        request: Request | None = None,
    ) -> None:
        self.status_code = status_code
        if json is not None:
            self._json = json
            self.content = _json.dumps(json).encode("utf-8")
        else:
            self._json = None
            self.content = content or b""
        self.headers = {k: v for k, v in (headers or {}).items()}
        self.request = request

    def json(self) -> Any:
        if self._json is not None:
            return self._json
        if not self.content:
            return None
        return _json.loads(self.content.decode("utf-8"))

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = self.request or Request("GET", "")
            raise HTTPStatusError(f"HTTP {self.status_code}", request, self)


class Client:
    def __init__(
        self,
        *,
        base_url: str,
        timeout: Timeout | None = None,
        headers: Mapping[str, str] | None = None,
        verify: bool | str | None = True,
        transport: BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout or Timeout()
        self._headers = {k: v for k, v in (headers or {}).items()}
        self._transport = transport

    def request(self, method: str, path: str, **kwargs: Any) -> Response:
        json_payload = kwargs.get("json")
        data = kwargs.get("data")
        headers = dict(self._headers)
        if json_payload is not None:
            content = _json.dumps(json_payload).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        elif data is not None:
            if isinstance(data, bytes):
                content = data
            else:
                content = str(data).encode("utf-8")
        else:
            content = None
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        full_url = urljoin(self._base_url + "/", path.lstrip("/"))
        request = Request(method, full_url, headers=headers, content=content)
        if isinstance(self._transport, MockTransport):
            response = self._transport.handle(request)
            response.request = request
            return response
        try:
            req = urllib_request.Request(full_url, data=content, method=method.upper())
            for key, value in headers.items():
                req.add_header(key, value)
            with urllib_request.urlopen(req, timeout=self._timeout.connect) as raw:
                response = Response(
                    raw.status,
                    content=raw.read(),
                    headers=dict(raw.headers.items()),
                    request=request,
                )
        except urllib_error.HTTPError as exc:
            response = Response(
                exc.code,
                content=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers else {},
                request=request,
            )
        except urllib_error.URLError as exc:  # pragma: no cover - network failure path
            raise HTTPError(str(exc))
        response.raise_for_status()
        return response

    def close(self) -> None:  # pragma: no cover - nothing to close
        return None

    def __enter__(self) -> "Client":  # pragma: no cover - unused in tests
        return self

    def __exit__(self, *_exc: object) -> None:  # pragma: no cover - unused in tests
        self.close()


__all__ = [
    "BaseTransport",
    "Client",
    "HTTPError",
    "HTTPStatusError",
    "MockTransport",
    "Request",
    "Response",
    "Timeout",
]
