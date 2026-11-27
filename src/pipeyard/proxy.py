"""Forwards a signed-in browser's requests to a report app's container, HTTP and WebSocket.

The app only ever sees what a browser would send it, minus pipeyard's own credentials: the
sign-in cookie and any Authorization header are removed before forwarding, so a report
container never receives a token it could replay against the pipeyard API.
"""

import asyncio

import httpx
import websockets
from fastapi import HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from .auth import COOKIE

# Per-connection headers that must not be copied from one hop to the next.
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers",
    "transfer-encoding", "upgrade", "host", "content-length",
}  # fmt: skip
WEBSOCKET_HANDSHAKE = {
    "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
    "sec-websocket-protocol", "sec-websocket-accept",
}  # fmt: skip


def _forward_headers(headers, *, skip: set[str]) -> list[tuple[str, str]]:
    out = []
    for name, value in headers.items():
        lower = name.lower()
        if lower in skip or lower == "authorization":
            continue
        if lower == "cookie":
            kept = [c for c in value.split("; ") if not c.startswith(f"{COOKIE}=")]
            if not kept:
                continue
            value = "; ".join(kept)
        out.append((name, value))
    return out


def _target(scheme: str, address: str, path: str, query: str) -> str:
    return f"{scheme}://{address}{path}" + (f"?{query}" if query else "")


async def forward_http(client: httpx.AsyncClient, request: Request, address: str):
    headers = _forward_headers(request.headers, skip=HOP_BY_HOP)
    headers.append(("x-forwarded-host", request.headers.get("host", "")))
    headers.append(("x-forwarded-proto", request.url.scheme))
    body = await request.body()
    upstream_request = client.build_request(
        request.method,
        _target("http", address, request.url.path, request.url.query),
        headers=headers,
        content=body or None,
    )
    try:
        upstream = await client.send(upstream_request, stream=True)
    except httpx.TransportError as e:
        raise HTTPException(502, f"the report app did not answer ({type(e).__name__})") from e
    response_headers = {
        k: v
        for k, v in upstream.headers.items()
        if k.lower() not in ("connection", "keep-alive", "transfer-encoding")
    }
    return StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(upstream.aclose),
    )


async def forward_websocket(browser: WebSocket, address: str) -> None:
    url = _target("ws", address, browser.url.path, browser.url.query)
    headers = _forward_headers(browser.headers, skip=HOP_BY_HOP | WEBSOCKET_HANDSHAKE)
    try:
        upstream = await websockets.connect(
            url,
            subprotocols=browser.scope.get("subprotocols") or None,
            additional_headers=headers,
            max_size=None,
            open_timeout=10,
            proxy=None,  # the app is on a private address: never via an outbound proxy
        )
    except (OSError, websockets.WebSocketException, TimeoutError):
        await browser.close(code=1011)
        return
    await browser.accept(subprotocol=upstream.subprotocol)

    async def browser_to_app() -> None:
        while True:
            message = await browser.receive()
            if message["type"] == "websocket.disconnect":
                return
            data = message.get("text")
            await upstream.send(data if data is not None else message["bytes"])

    async def app_to_browser() -> None:
        async for data in upstream:
            if isinstance(data, str):
                await browser.send_text(data)
            else:
                await browser.send_bytes(data)

    tasks = [asyncio.create_task(browser_to_app()), asyncio.create_task(app_to_browser())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        # Collect both, so that a send to a browser that has just gone is not left unhandled.
        await asyncio.gather(*tasks, return_exceptions=True)
        await upstream.close()
        try:
            await browser.close()
        except (RuntimeError, WebSocketDisconnect):
            pass  # the browser side is already closed, e.g. the tab was shut
