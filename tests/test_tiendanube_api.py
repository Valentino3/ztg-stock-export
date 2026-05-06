import httpx
import pytest

from gn_stock_export.config import TiendaNubeCredentials
from gn_stock_export.tiendanube_api import TiendaNubeApiClient, TiendaNubeApiError


def test_tiendanube_api_client_uses_authentication_header_and_handle_lookup() -> None:
    credentials = TiendaNubeCredentials(
        store_id=12345,
        access_token="token-abc",
        user_agent="gn-stock-export-tests",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authentication"] == "bearer token-abc"
        assert request.headers["User-Agent"] == "gn-stock-export-tests"
        assert request.url.path == "/v1/12345/products"
        assert request.url.params["handle"] == "gn-100"
        return httpx.Response(
            200,
            json=[
                {
                    "id": 10,
                    "handle": {"es": "gn-100"},
                    "tags": "GN_SYNC, Audio",
                }
            ],
        )

    transport = httpx.MockTransport(handler)
    with TiendaNubeApiClient(credentials=credentials, transport=transport) as client:
        product = client.get_product_by_handle("gn-100")

    assert product is not None
    assert product["id"] == 10


def test_tiendanube_api_client_retries_429_before_succeeding() -> None:
    credentials = TiendaNubeCredentials(
        store_id=12345,
        access_token="token-abc",
        user_agent="gn-stock-export-tests",
    )
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, json={"message": "Too Many Requests"}, headers={"Retry-After": "0"})
        return httpx.Response(200, json=[{"id": 10, "handle": {"es": "gn-100"}}])

    transport = httpx.MockTransport(handler)
    with TiendaNubeApiClient(
        credentials=credentials,
        transport=transport,
        max_retries=2,
        retry_base_delay_seconds=0.0,
    ) as client:
        products = client.list_products()

    assert calls == 2
    assert products[0]["id"] == 10


def test_tiendanube_api_client_raises_after_429_retry_exhaustion() -> None:
    credentials = TiendaNubeCredentials(
        store_id=12345,
        access_token="token-abc",
        user_agent="gn-stock-export-tests",
    )
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={"message": "Too Many Requests"}, headers={"Retry-After": "0"})

    transport = httpx.MockTransport(handler)
    with TiendaNubeApiClient(
        credentials=credentials,
        transport=transport,
        max_retries=1,
        retry_base_delay_seconds=0.0,
    ) as client:
        with pytest.raises(TiendaNubeApiError):
            client.list_products()

    assert calls == 2


def test_tiendanube_api_client_deletes_product() -> None:
    credentials = TiendaNubeCredentials(
        store_id=12345,
        access_token="token-abc",
        user_agent="gn-stock-export-tests",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        assert request.url.path == "/v1/12345/products/99"
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with TiendaNubeApiClient(credentials=credentials, transport=transport) as client:
        client.delete_product(99)
