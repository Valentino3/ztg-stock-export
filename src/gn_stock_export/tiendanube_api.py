from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from typing import Any
import time

import httpx

from gn_stock_export.config import TiendaNubeCredentials


class TiendaNubeApiError(RuntimeError):
    """Errores generales al consumir la API de Tienda Nube."""


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
)


@dataclass
class TiendaNubeApiClient:
    credentials: TiendaNubeCredentials
    base_url: str = "https://api.tiendanube.com/v1"
    timeout_seconds: float = 60.0
    transport: httpx.BaseTransport | None = None
    max_retries: int = 5
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 60.0

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

    def list_categories(self, *, page: int = 1, per_page: int = 200) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/categories", params={"page": page, "per_page": per_page})
        if not isinstance(payload, list):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio categorias con formato invalido.")
        return payload

    def list_all_categories(self, *, per_page: int = 200) -> list[dict[str, Any]]:
        page = 1
        categories: list[dict[str, Any]] = []
        while True:
            chunk = self.list_categories(page=page, per_page=per_page)
            if not chunk:
                return categories
            categories.extend(chunk)
            if len(chunk) < per_page:
                return categories
            page += 1

    def create_category(self, name: str, *, parent_id: int | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": {"es": name}}
        if parent_id is not None:
            payload["parent"] = parent_id
        response_payload = self._request_json("POST", "/categories", json=payload)
        if not isinstance(response_payload, dict):
            raise TiendaNubeApiError("La API de Tienda Nube devolvio una categoria invalida al crear.")
        return response_payload

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

    def delete_product(self, product_id: int) -> None:
        self._request_json("DELETE", f"/products/{product_id}")

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

        retry_count = 0
        while True:
            try:
                response = self._client.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    headers=headers,
                )
            except RETRYABLE_EXCEPTIONS as exc:
                if retry_count >= self.max_retries:
                    raise TiendaNubeApiError(
                        f"Fallo la llamada {path}: la API no respondio despues de "
                        f"{self.max_retries + 1} intentos ({exc})."
                    ) from exc

                delay = self._backoff_delay_seconds(retry_count)
                if delay > 0:
                    time.sleep(delay)
                retry_count += 1
                continue

            if not response.is_error:
                break
            if response.status_code not in RETRYABLE_STATUS_CODES or retry_count >= self.max_retries:
                break

            delay = self._retry_delay_seconds(response, retry_count)
            if delay > 0:
                time.sleep(delay)
            retry_count += 1

        if response.is_error:
            raise TiendaNubeApiError(f"Fallo la llamada {path} ({response.status_code}): {response.text.strip()}")

        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise TiendaNubeApiError(f"La API de Tienda Nube devolvio JSON invalido para {path}.") from exc

    def _retry_delay_seconds(self, response: httpx.Response, retry_count: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            parsed_retry_after = self._parse_retry_after(retry_after)
            if parsed_retry_after is not None:
                return min(parsed_retry_after, self.retry_max_delay_seconds)

        delay = self.retry_base_delay_seconds * (2**retry_count)
        return min(delay, self.retry_max_delay_seconds)

    def _backoff_delay_seconds(self, retry_count: int) -> float:
        delay = self.retry_base_delay_seconds * (2**retry_count)
        return min(delay, self.retry_max_delay_seconds)

    @staticmethod
    def _parse_retry_after(value: str) -> float | None:
        clean_value = value.strip()
        if not clean_value:
            return None
        try:
            return max(float(clean_value), 0.0)
        except ValueError:
            pass

        try:
            target = parsedate_to_datetime(clean_value)
        except (TypeError, ValueError):
            return None
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        return max((target - datetime.now(timezone.utc)).total_seconds(), 0.0)
