from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

from gn_stock_export.config import AppConfig


class SnapshotError(RuntimeError):
    """Errores con snapshots locales."""


@dataclass
class SnapshotData:
    path: Path
    generated_at: str
    usd_exchange: float
    frame: pd.DataFrame


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp_slug(value: datetime | str) -> str:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return "".join(char for char in value if char.isdigit())[:15] or "snapshot"
        return _format_timestamp_slug(parsed.astimezone(timezone.utc))
    return _format_timestamp_slug(value.astimezone(timezone.utc))


def save_snapshot(
    frame: pd.DataFrame,
    snapshot_dir: Path,
    generated_at: datetime,
    usd_exchange: float,
    config: AppConfig,
) -> Path:
    ensure_directory(snapshot_dir)
    path = snapshot_dir / f"stock_snapshot_{timestamp_slug(generated_at)}.json"
    payload = {
        "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
        "usd_exchange": usd_exchange,
        "config": config.to_public_dict(),
        "records": frame.to_dict(orient="records"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_snapshot(path: Path) -> SnapshotData:
    if not path.exists():
        raise SnapshotError(f"No existe el snapshot: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if "records" not in payload:
        raise SnapshotError(f"El snapshot no tiene registros: {path}")

    frame = pd.DataFrame(payload["records"])
    generated_at = str(payload.get("generated_at", ""))
    usd_exchange = float(payload.get("usd_exchange", 0.0))
    return SnapshotData(path=path, generated_at=generated_at, usd_exchange=usd_exchange, frame=frame)


def list_snapshot_paths(snapshot_dir: Path) -> list[Path]:
    if not snapshot_dir.exists():
        return []
    return sorted(snapshot_dir.glob("stock_snapshot_*.json"))


def _format_timestamp_slug(value: datetime) -> str:
    base = value.strftime("%Y%m%d_%H%M%S")
    if value.microsecond:
        return f"{base}_{value.microsecond:06d}"
    return base
