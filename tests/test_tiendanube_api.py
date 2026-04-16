import httpx

from gn_stock_export.config import TiendaNubeCredentials
from gn_stock_export.tiendanube_api import TiendaNubeApiClient


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
