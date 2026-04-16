from __future__ import annotations

from dataclasses import dataclass

import httpx

from gn_stock_export.config import Credentials


class GNApiError(RuntimeError):
    """Errores generales al consumir la API."""


class GNAuthenticationError(GNApiError):
    """Errores de autenticacion."""


@dataclass
class GNApiClient:
    credentials: Credentials
    base_url: str = "https://api.gruponucleosa.com"
    timeout_seconds: float = 60.0
    transport: httpx.BaseTransport | None = None

    def __post_init__(self) -> None:
        self._token: str | None = None
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            transport=self.transport,
            headers={
                "Accept": "application/json",
                "User-Agent": "gn-stock-export/0.1.0",
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "GNApiClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def authenticate(self) -> str:
        payload = {
            "id": self.credentials.client_id,
            "username": self.credentials.username,
            "password": self.credentials.password,
        }
        response = self._client.post("/Authentication/Login", json=payload)
        if response.is_error:
            raise GNAuthenticationError(
                f"Fallo la autenticacion ({response.status_code}): {response.text.strip()}"
            )

        token = response.text.strip().strip('"')
        if not token:
            raise GNAuthenticationError("La API no devolvio un token valido.")

        self._token = token
        return token

    def get_usd_exchange(self) -> float:
        payload = self._request_json("GET", "/API_V1/GetUSDExchange")
        if not isinstance(payload, dict) or "cotizacionUSD" not in payload:
            raise GNApiError("La respuesta de cotizacion USD no tiene el formato esperado.")

        try:
            return float(payload["cotizacionUSD"])
        except (TypeError, ValueError) as exc:
            raise GNApiError("La cotizacion USD no es numerica.") from exc

    def get_catalog(self) -> list[dict[str, object]]:
        payload = self._request_json("GET", "/API_V1/GetCatalog")
        if not isinstance(payload, list):
            raise GNApiError("La respuesta del catalogo no tiene el formato esperado.")
        return payload

    def _request_json(self, method: str, path: str, retry_on_auth: bool = True) -> object:
        if not self._token:
            self.authenticate()

        response = self._client.request(
            method,
            path,
            headers={"Authorization": f"Bearer {self._token}"},
        )
        if response.status_code == 401 and retry_on_auth:
            self.authenticate()
            return self._request_json(method, path, retry_on_auth=False)
        if response.is_error:
            raise GNApiError(f"Fallo la llamada {path} ({response.status_code}): {response.text.strip()}")

        try:
            return response.json()
        except ValueError as exc:
            raise GNApiError(f"La API devolvio JSON invalido para {path}.") from exc

