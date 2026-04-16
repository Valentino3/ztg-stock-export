from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
import tomllib


class ConfigError(ValueError):
    """Errores de configuracion de la app."""


class CredentialsError(ValueError):
    """Errores de credenciales del .env."""


ROUNDING_MODES = {"nearest", "up", "down"}
COST_FIELD_MODES = {"ars_neto", "usd_origen", "ars_final"}
IMAGE_SYNC_MODES = {"append_only"}


@dataclass(frozen=True)
class Credentials:
    client_id: int
    username: str
    password: str


@dataclass(frozen=True)
class TiendaNubeCredentials:
    store_id: int
    access_token: str
    user_agent: str


@dataclass(frozen=True)
class PricingConfig:
    use_api_usd_exchange: bool
    use_usd_override: bool
    usd_exchange_override: float
    margin_pct: float
    fixed_markup_ars: float
    rounding_step: float
    rounding_mode: str
    cost_field_mode: str


@dataclass(frozen=True)
class PublicationConfig:
    publish_with_stock_only: bool
    min_stock_to_publish: int
    free_shipping: bool
    product_physical: bool


@dataclass(frozen=True)
class ContentConfig:
    default_brand_when_empty: str
    seo_title_max_length: int
    seo_description_max_length: int
    description_prefix: str
    description_suffix: str


@dataclass(frozen=True)
class MappingConfig:
    brand_map_csv: Path
    category_map_csv: Path


@dataclass(frozen=True)
class DiffConfig:
    price_tolerance_ars: float


@dataclass(frozen=True)
class OutputConfig:
    output_dir: Path
    include_csv: bool
    include_xlsx: bool
    test_product_limit: int = 20


@dataclass(frozen=True)
class TiendaNubeSyncConfig:
    enabled: bool
    dry_run: bool
    managed_tag: str
    handle_prefix: str
    unpublish_missing: bool
    image_mode: str
    test_product_limit: int


@dataclass(frozen=True)
class AppConfig:
    pricing: PricingConfig
    publication: PublicationConfig
    content: ContentConfig
    mappings: MappingConfig
    diff: DiffConfig
    output: OutputConfig
    tiendanube_sync: TiendaNubeSyncConfig = TiendaNubeSyncConfig(
        enabled=False,
        dry_run=True,
        managed_tag="GN_SYNC",
        handle_prefix="gn",
        unpublish_missing=True,
        image_mode="append_only",
        test_product_limit=20,
    )

    def to_public_dict(self) -> dict[str, object]:
        return _serialize_public(asdict(self))


def load_app_config(path: str | Path) -> AppConfig:
    config_path = Path(path).resolve()
    if not config_path.exists():
        raise ConfigError(f"No existe el archivo de configuracion: {config_path}")

    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    root_dir = config_path.parent
    if _looks_like_new_config(raw):
        return _load_sectioned_config(raw, root_dir)
    return _load_legacy_config(raw, root_dir)


def load_credentials(path: str | Path) -> Credentials:
    env_path = Path(path).resolve()
    if not env_path.exists():
        raise CredentialsError(f"No existe el archivo de credenciales: {env_path}")

    values = _parse_env_file(env_path)
    missing = [key for key in ("NUCLEO_ID", "NUCLEO_USERNAME", "NUCLEO_PASSWORD") if not values.get(key)]
    if missing:
        joined = ", ".join(missing)
        raise CredentialsError(f"Faltan variables obligatorias en el .env: {joined}")

    try:
        client_id = int(values["NUCLEO_ID"])
    except ValueError as exc:
        raise CredentialsError("`NUCLEO_ID` debe ser un numero entero.") from exc

    return Credentials(
        client_id=client_id,
        username=values["NUCLEO_USERNAME"],
        password=values["NUCLEO_PASSWORD"],
    )


def load_tiendanube_credentials(path: str | Path) -> TiendaNubeCredentials:
    env_path = Path(path).resolve()
    if not env_path.exists():
        raise CredentialsError(f"No existe el archivo de credenciales: {env_path}")

    values = _parse_env_file(env_path)
    missing = [key for key in ("TIENDANUBE_STORE_ID", "TIENDANUBE_ACCESS_TOKEN") if not values.get(key)]
    if missing:
        joined = ", ".join(missing)
        raise CredentialsError(f"Faltan variables obligatorias de Tienda Nube en el .env: {joined}")

    try:
        store_id = int(values["TIENDANUBE_STORE_ID"])
    except ValueError as exc:
        raise CredentialsError("`TIENDANUBE_STORE_ID` debe ser un numero entero.") from exc

    return TiendaNubeCredentials(
        store_id=store_id,
        access_token=values["TIENDANUBE_ACCESS_TOKEN"],
        user_agent=values.get("TIENDANUBE_USER_AGENT", "").strip() or "gn-stock-export/0.1.0",
    )


def _looks_like_new_config(raw: dict[str, object]) -> bool:
    return any(
        section in raw for section in ("pricing", "publication", "content", "mappings", "diff", "output", "tiendanube_sync")
    )


def _load_sectioned_config(raw: dict[str, object], root_dir: Path) -> AppConfig:
    pricing_raw = _require_section(raw, "pricing")
    publication_raw = _require_section(raw, "publication")
    content_raw = _require_section(raw, "content")
    mappings_raw = _require_section(raw, "mappings")
    diff_raw = _require_section(raw, "diff")
    output_raw = _require_section(raw, "output")
    tiendanube_sync_raw = _optional_section(raw, "tiendanube_sync")

    pricing = PricingConfig(
        use_api_usd_exchange=_require_bool(pricing_raw, "use_api_usd_exchange"),
        use_usd_override=_require_bool(pricing_raw, "use_usd_override"),
        usd_exchange_override=_require_number(pricing_raw, "usd_exchange_override"),
        margin_pct=_require_number(pricing_raw, "margin_pct"),
        fixed_markup_ars=_require_number(pricing_raw, "fixed_markup_ars"),
        rounding_step=_require_number(pricing_raw, "rounding_step"),
        rounding_mode=_require_string(pricing_raw, "rounding_mode").lower(),
        cost_field_mode=_require_string(pricing_raw, "cost_field_mode").lower(),
    )
    publication = PublicationConfig(
        publish_with_stock_only=_require_bool(publication_raw, "publish_with_stock_only"),
        min_stock_to_publish=_require_int(publication_raw, "min_stock_to_publish"),
        free_shipping=_require_bool(publication_raw, "free_shipping"),
        product_physical=_require_bool(publication_raw, "product_physical"),
    )
    content = ContentConfig(
        default_brand_when_empty=_optional_string(content_raw, "default_brand_when_empty"),
        seo_title_max_length=_require_int(content_raw, "seo_title_max_length"),
        seo_description_max_length=_require_int(content_raw, "seo_description_max_length"),
        description_prefix=_optional_string(content_raw, "description_prefix"),
        description_suffix=_optional_string(content_raw, "description_suffix"),
    )
    mappings = MappingConfig(
        brand_map_csv=_resolve_path(_require_string(mappings_raw, "brand_map_csv"), root_dir),
        category_map_csv=_resolve_path(_require_string(mappings_raw, "category_map_csv"), root_dir),
    )
    diff = DiffConfig(
        price_tolerance_ars=_require_number(diff_raw, "price_tolerance_ars"),
    )
    output = OutputConfig(
        output_dir=_resolve_path(_require_string(output_raw, "output_dir"), root_dir),
        include_csv=_require_bool(output_raw, "include_csv"),
        include_xlsx=_require_bool(output_raw, "include_xlsx"),
        test_product_limit=_optional_int(output_raw, "test_product_limit", 20),
    )
    tiendanube_sync = TiendaNubeSyncConfig(
        enabled=_optional_bool(tiendanube_sync_raw, "enabled", False),
        dry_run=_optional_bool(tiendanube_sync_raw, "dry_run", True),
        managed_tag=_optional_string(tiendanube_sync_raw, "managed_tag") or "GN_SYNC",
        handle_prefix=_optional_string(tiendanube_sync_raw, "handle_prefix") or "gn",
        unpublish_missing=_optional_bool(tiendanube_sync_raw, "unpublish_missing", True),
        image_mode=(_optional_string(tiendanube_sync_raw, "image_mode") or "append_only").lower(),
        test_product_limit=_optional_int(tiendanube_sync_raw, "test_product_limit", 20),
    )
    return _validate_config(AppConfig(pricing, publication, content, mappings, diff, output, tiendanube_sync))


def _load_legacy_config(raw: dict[str, object], root_dir: Path) -> AppConfig:
    pricing = PricingConfig(
        use_api_usd_exchange=True,
        use_usd_override=False,
        usd_exchange_override=0.0,
        margin_pct=_require_number(raw, "margin_pct"),
        fixed_markup_ars=_require_number(raw, "fixed_markup_ars"),
        rounding_step=_require_number(raw, "rounding_step"),
        rounding_mode=_require_string(raw, "rounding_mode").lower(),
        cost_field_mode="ars_neto",
    )
    publication = PublicationConfig(
        publish_with_stock_only=True,
        min_stock_to_publish=1,
        free_shipping=False,
        product_physical=True,
    )
    content = ContentConfig(
        default_brand_when_empty="",
        seo_title_max_length=70,
        seo_description_max_length=160,
        description_prefix="",
        description_suffix="",
    )
    mappings = MappingConfig(
        brand_map_csv=(root_dir / "brand_map.csv").resolve(),
        category_map_csv=(root_dir / "category_map.csv").resolve(),
    )
    diff = DiffConfig(
        price_tolerance_ars=_require_number(raw, "price_tolerance_ars"),
    )
    output = OutputConfig(
        output_dir=_resolve_path(_require_string(raw, "output_dir"), root_dir),
        include_csv=_require_bool(raw, "include_csv"),
        include_xlsx=_require_bool(raw, "include_xlsx"),
        test_product_limit=20,
    )
    tiendanube_sync = TiendaNubeSyncConfig(
        enabled=False,
        dry_run=True,
        managed_tag="GN_SYNC",
        handle_prefix="gn",
        unpublish_missing=True,
        image_mode="append_only",
        test_product_limit=20,
    )
    return _validate_config(AppConfig(pricing, publication, content, mappings, diff, output, tiendanube_sync))


def _validate_config(config: AppConfig) -> AppConfig:
    pricing = config.pricing
    publication = config.publication
    content = config.content
    diff = config.diff
    output = config.output
    tiendanube_sync = config.tiendanube_sync

    if pricing.rounding_step <= 0:
        raise ConfigError("`pricing.rounding_step` debe ser mayor a 0.")
    if pricing.rounding_mode not in ROUNDING_MODES:
        raise ConfigError("`pricing.rounding_mode` debe ser `nearest`, `up` o `down`.")
    if pricing.cost_field_mode not in COST_FIELD_MODES:
        raise ConfigError("`pricing.cost_field_mode` debe ser `ars_neto`, `usd_origen` o `ars_final`.")
    if pricing.use_usd_override and pricing.usd_exchange_override <= 0:
        raise ConfigError("`pricing.usd_exchange_override` debe ser mayor a 0 cuando `use_usd_override` es true.")
    if not pricing.use_api_usd_exchange and not pricing.use_usd_override:
        raise ConfigError("Debe existir una fuente de cotizacion USD: API o override manual.")
    if publication.min_stock_to_publish < 0:
        raise ConfigError("`publication.min_stock_to_publish` no puede ser negativo.")
    if content.seo_title_max_length <= 0:
        raise ConfigError("`content.seo_title_max_length` debe ser mayor a 0.")
    if content.seo_description_max_length <= 0:
        raise ConfigError("`content.seo_description_max_length` debe ser mayor a 0.")
    if diff.price_tolerance_ars < 0:
        raise ConfigError("`diff.price_tolerance_ars` no puede ser negativo.")
    if not output.include_csv and not output.include_xlsx:
        raise ConfigError("Al menos uno entre `output.include_csv` o `output.include_xlsx` debe ser true.")
    if output.test_product_limit < 0:
        raise ConfigError("`output.test_product_limit` no puede ser negativo.")
    if not tiendanube_sync.managed_tag:
        raise ConfigError("`tiendanube_sync.managed_tag` no puede ser vacio.")
    if not tiendanube_sync.handle_prefix:
        raise ConfigError("`tiendanube_sync.handle_prefix` no puede ser vacio.")
    if tiendanube_sync.image_mode not in IMAGE_SYNC_MODES:
        raise ConfigError("`tiendanube_sync.image_mode` debe ser `append_only`.")
    if tiendanube_sync.test_product_limit < 0:
        raise ConfigError("`tiendanube_sync.test_product_limit` no puede ser negativo.")

    return config


def _parse_env_file(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.lstrip("\ufeff").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        clean_key = key.strip().strip('"').strip("'")
        clean_value = value.strip().strip('"').strip("'")
        result[clean_key] = clean_value
    return result


def _serialize_public(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _serialize_public(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_public(item) for item in value]
    return value


def _require_section(raw: dict[str, object], key: str) -> dict[str, object]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise ConfigError(f"`{key}` debe ser una seccion TOML valida.")
    return value


def _optional_section(raw: dict[str, object], key: str) -> dict[str, object]:
    value = raw.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"`{key}` debe ser una seccion TOML valida.")
    return value


def _resolve_path(value: str, root_dir: Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = root_dir / path
    return path.resolve()


def _require_number(raw: dict[str, object], key: str) -> float:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"`{key}` debe ser numerico.")
    return float(value)


def _require_int(raw: dict[str, object], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"`{key}` debe ser entero.")
    return value


def _optional_int(raw: dict[str, object], key: str, default: int) -> int:
    value = raw.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"`{key}` debe ser entero.")
    return value


def _require_string(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"`{key}` debe ser un texto no vacio.")
    return value.strip()


def _optional_string(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ConfigError(f"`{key}` debe ser texto.")
    return value.strip()


def _require_bool(raw: dict[str, object], key: str) -> bool:
    value = raw.get(key)
    if not isinstance(value, bool):
        raise ConfigError(f"`{key}` debe ser booleano.")
    return value


def _optional_bool(raw: dict[str, object], key: str, default: bool) -> bool:
    value = raw.get(key)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigError(f"`{key}` debe ser booleano.")
    return value
