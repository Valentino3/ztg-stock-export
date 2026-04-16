from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from gn_stock_export.config import TiendaNubeCredentials


class TiendaNubeApiError(RuntimeError):
    """Errores generales al consumir la API de Tienda Nube."""


@dataclass
class TiendaNubeApiClient:
    credentials: TiendaNubeCredentials
    base_url: str = "https://api.tiendanube.com/v1"
    timeout_seconds: float = 60.0
    transport: httpx.BaseTransport | None = None

    def __post_init__(self) -> None:
        self._client = httpx.Client(
            base_url=f"{self.base_url}/{self.credentials.store_id}",
            timeout=self.timeout_seconds,
            transport=self.transport,
            headers={
                "Accept": "application/json",
                "Authentication": f"bearer {self.credentials.access_token}",
                "User-Agent": self.credentials.user_agent,
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TiendaNubeApiClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def list_products(
        self,
        *,
        handle: str | None = None,
        page: int = 1,
        per_page: int = 200,
        fields: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, object] = {"page": page, "per_page": per_page}
        if handle:
            params["handle"] = handle
        if fields:
            params["fields"] = fields
        payload = self._request_json("GET", "/products", params=params)
        if not isinstance(payload, list):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio productos con formato invalido.")
        return payload

    def get_product_by_handle(self, handle: str) -> dict[str, Any] | None:
        products = self.list_products(handle=handle)
        for product in products:
            localized_handle = product.get("handle")
            if isinstance(localized_handle, dict) and handle in localized_handle.values():
                return product
        return products[0] if products else None

    def list_all_products(self, *, per_page: int = 200) -> list[dict[str, Any]]:
        page = 1
        products: list[dict[str, Any]] = []
        while True:
            chunk = self.list_products(page=page, per_page=per_page)
            if not chunk:
                return products
            products.extend(chunk)
            if len(chunk) < per_page:
                return products
            page += 1

    def create_product(self, payload: dict[str, Any]) -> dict[str, Any]:
        response_payload = self._request_json("POST", "/products", json=payload)
        if not isinstance(response_payload, dict):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio un producto invalido al crear.")
        return response_payload

    def update_product(self, product_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        response_payload = self._request_json("PUT", f"/products/{product_id}", json=payload)
        if not isinstance(response_payload, dict):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio un producto invalido al actualizar.")
        return response_payload

    def update_variant(self, product_id: int, variant_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        response_payload = self._request_json("PUT", f"/products/{product_id}/variants/{variant_id}", json=payload)
        if not isinstance(response_payload, dict):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio una variante invalida al actualizar.")
        return response_payload

    def list_product_images(self, product_id: int, *, page: int = 1, per_page: int = 200) -> list[dict[str, Any]]:
        payload = self._request_json(
            "GET",
            f"/products/{product_id}/images",
            params={"page": page, "per_page": per_page},
        )
        if not isinstance(payload, list):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio imagenes con formato invalido.")
        return payload

    def create_product_image(self, product_id: int, src: str, *, position: int | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"src": src}
        if position is not None:
            payload["position"] = position
        response_payload = self._request_json("POST", f"/products/{product_id}/images", json=payload)
        if not isinstance(response_payload, dict):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio una imagen invalida al crear.")
        return response_payload

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        json: dict[str, Any] | None = None,
    ) -> object:
        headers = {}
        if json is not None:
            headers["Content-Type"] = "application/json"

        response = self._client.request(
            method,
            path,
            params=params,
            json=json,
            headers=headers,
        )
        if response.is_error:
            raise TiendaNubeApiError(f"Fallo la llamada {path} ({response.status_code}): {response.text.strip()}")

        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise TiendaNubeApiError(f"La API de Tienda Nube devolvio JSON invalido para {path}.") from exc
