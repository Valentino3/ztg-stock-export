from pathlib import Path

import pytest

from gn_stock_export.config import (
    ConfigError,
    CredentialsError,
    load_app_config,
    load_credentials,
    load_tiendanube_credentials,
)


def test_load_credentials_tolerates_spaces_quotes_and_noise(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        'NUCLEO_ID = 25\nNUCLEO_USERNAME = "demo-user"\nNUCLEO_PASSWORD = "demo-pass"\n"\n',
        encoding="utf-8",
    )

    credentials = load_credentials(env_path)

    assert credentials.client_id == 25
    assert credentials.username == "demo-user"
    assert credentials.password == "demo-pass"


def test_load_credentials_fails_when_required_keys_are_missing(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("NUCLEO_ID=1\n", encoding="utf-8")

    with pytest.raises(CredentialsError):
        load_credentials(env_path)


def test_load_tiendanube_credentials_supports_default_user_agent(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "TIENDANUBE_STORE_ID=987\nTIENDANUBE_ACCESS_TOKEN=abc123\n",
        encoding="utf-8",
    )

    credentials = load_tiendanube_credentials(env_path)

    assert credentials.store_id == 987
    assert credentials.access_token == "abc123"
    assert credentials.user_agent == "gn-stock-export/0.1.0"


def test_load_app_config_supports_new_sectioned_format(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[pricing]
use_api_usd_exchange = true
use_usd_override = true
usd_exchange_override = 1400.0
margin_pct = 15.0
fixed_markup_ars = 25.0
rounding_step = 10.0
rounding_mode = "up"
cost_field_mode = "ars_neto"

[publication]
publish_with_stock_only = true
min_stock_to_publish = 2
free_shipping = false
product_physical = true

[content]
default_brand_when_empty = "Generica"
seo_title_max_length = 65
seo_description_max_length = 150
description_prefix = "Promo"
description_suffix = "Fin"

[mappings]
brand_map_csv = "brand_map.csv"
category_map_csv = "category_map.csv"

[diff]
price_tolerance_ars = 1.5

[output]
output_dir = "exports"
include_csv = true
include_xlsx = false

[tiendanube_sync]
enabled = true
dry_run = false
managed_tag = "GN_SYNC"
handle_prefix = "gn"
unpublish_missing = true
image_mode = "append_only"
test_product_limit = 5
""".strip(),
        encoding="utf-8",
    )

    config = load_app_config(config_path)

    assert config.pricing.use_api_usd_exchange is True
    assert config.pricing.use_usd_override is True
    assert config.pricing.usd_exchange_override == 1400.0
    assert config.pricing.cost_field_mode == "ars_neto"
    assert config.publication.min_stock_to_publish == 2
    assert config.content.default_brand_when_empty == "Generica"
    assert config.diff.price_tolerance_ars == 1.5
    assert config.output.include_xlsx is False
    assert config.output.output_dir == (tmp_path / "exports").resolve()
    assert config.mappings.brand_map_csv == (tmp_path / "brand_map.csv").resolve()
    assert config.tiendanube_sync.enabled is True
    assert config.tiendanube_sync.dry_run is False
    assert config.tiendanube_sync.test_product_limit == 5


def test_load_app_config_accepts_legacy_flat_format(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "margin_pct = 10.0",
                "fixed_markup_ars = 0.0",
                "rounding_step = 1.0",
                'rounding_mode = "nearest"',
                "price_tolerance_ars = 0.5",
                'output_dir = "exports"',
                "include_csv = true",
                "include_xlsx = true",
            ]
        ),
        encoding="utf-8",
    )

    config = load_app_config(config_path)

    assert config.pricing.margin_pct == 10.0
    assert config.pricing.use_api_usd_exchange is True
    assert config.pricing.use_usd_override is False
    assert config.pricing.cost_field_mode == "ars_neto"
    assert config.publication.publish_with_stock_only is True
    assert config.content.seo_title_max_length == 70
    assert config.output.output_dir == (tmp_path / "exports").resolve()


def test_load_app_config_rejects_invalid_override_and_content_limits(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[pricing]
use_api_usd_exchange = false
use_usd_override = true
usd_exchange_override = 0.0
margin_pct = 15.0
fixed_markup_ars = 0.0
rounding_step = 1.0
rounding_mode = "nearest"
cost_field_mode = "ars_neto"

[publication]
publish_with_stock_only = true
min_stock_to_publish = 1
free_shipping = false
product_physical = true

[content]
default_brand_when_empty = ""
seo_title_max_length = 0
seo_description_max_length = 160
description_prefix = ""
description_suffix = ""

[mappings]
brand_map_csv = "brand_map.csv"
category_map_csv = "category_map.csv"

[diff]
price_tolerance_ars = 0.5

[output]
output_dir = "exports"
include_csv = true
include_xlsx = true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError):
        load_app_config(config_path)
