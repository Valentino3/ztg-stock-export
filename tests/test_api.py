import httpx

from gn_stock_export.api import GNApiClient
from gn_stock_export.config import Credentials


def test_api_client_reauthenticates_after_401() -> None:
    credentials = Credentials(client_id=1, username="demo", password="secret")
    login_calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Authentication/Login":
            login_calls["count"] += 1
            return httpx.Response(200, text=f"token-{login_calls['count']}")
        if request.url.path == "/API_V1/GetCatalog":
            if request.headers.get("Authorization") == "Bearer token-1":
                return httpx.Response(401, json={"detail": "expired"})
            return httpx.Response(200, json=[{"item_id": 1}])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    transport = httpx.MockTransport(handler)
    with GNApiClient(credentials=credentials, transport=transport) as client:
        catalog = client.get_catalog()

    assert catalog == [{"item_id": 1}]
    assert login_calls["count"] == 2


def test_api_client_returns_usd_exchange_as_float() -> None:
    credentials = Credentials(client_id=1, username="demo", password="secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Authentication/Login":
            return httpx.Response(200, text="token-ok")
        if request.url.path == "/API_V1/GetUSDExchange":
            return httpx.Response(200, json={"cotizacionUSD": 1385.0})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    transport = httpx.MockTransport(handler)
    with GNApiClient(credentials=credentials, transport=transport) as client:
        usd_exchange = client.get_usd_exchange()

    assert usd_exchange == 1385.0
