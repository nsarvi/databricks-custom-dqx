from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from importlib import resources

logger = logging.getLogger(__name__)


class ConfigUtil:
    """
    Behavior:
      - If spec == DEFAULT_DQX_CONFIG_FILE:
          1) Try load from packaged wheel resource (package:resource)
          2) If not found / not loadable, fall back to filesystem search
      - Else:
          Treat as filesystem path only (absolute -> CWD -> sys.path)

    Notes:
      - DEFAULT_DQX_CONFIG_FILE uses standard format: "package:resource"
        e.g. "idea4.configs:idea4_config.yaml"
    """

    DEFAULT_DQX_CONFIG_FILE: str = "idea4.configs:idea4_config.yaml"
    ENABLE_SYS_PATH_SEARCH: bool = True

    # ---------------- Public API ----------------

    @staticmethod
    def load_dqx_config(spec: str) -> Dict[str, Any]:
        """
        Load YAML into dict following the rules above.
        """
        if spec is None or not isinstance(spec, str) or not spec.strip():
            raise ValueError("config spec must be a non-empty string")

        spec = spec.strip()

        # ✅ Only for the default constant: try packaged first, then fallback to filesystem
        if spec == ConfigUtil.DEFAULT_DQX_CONFIG_FILE:
            try:
                return ConfigUtil._load_yaml(spec, prefer_packaged_default=True)
            except FileNotFoundError as e:
                # Re-raise with context (this only happens if both packaged and filesystem fail)
                raise FileNotFoundError(
                    f"Failed to load default DQX config. Tried packaged first, then filesystem.\n{e}"
                ) from e

        # ✅ For everything else: filesystem only
        return ConfigUtil._load_yaml(spec, prefer_packaged_default=False)

    # ---------------- Unified loader ----------------

    @staticmethod
    def _load_yaml(spec: str, *, prefer_packaged_default: bool) -> Dict[str, Any]:
        """
        Unified logic:
          - (optional) try packaged load if spec is a resource spec
          - then resolve filesystem and load
        """
        attempted = []

        # 1) Packaged attempt (only if requested AND spec looks like 'package:resource')
        if prefer_packaged_default and ":" in spec:
            attempted.append(f"packaged:{spec}")
            try:
                data = ConfigUtil._try_load_packaged_yaml(spec)
                if data is not None:
                    return data
            except Exception as ex:
                # Don't hard fail here; we fall back to filesystem
                logger.debug(f"Packaged load failed for {spec!r}: {ex}")

        # 2) Filesystem attempt (absolute -> CWD -> sys.path)
        path = ConfigUtil._resolve_filesystem_path(spec, attempted=attempted)
        attempted.append(f"filesystem:{path}")
        return ConfigUtil._load_yaml_from_path(path)

    @staticmethod
    def _try_load_packaged_yaml(resource_spec: str) -> Dict[str, Any] | None:
        """
        Try to load a YAML resource from the wheel. Returns dict if found; otherwise None.
        """
        package, resource = ConfigUtil._parse_resource_spec(resource_spec)
        traversable = resources.files(package).joinpath(resource)

        try:
            with resources.as_file(traversable) as p:
                p = Path(p)
                if not p.exists() or not p.is_file():
                    return None
                logger.debug(f"Loading packaged YAML: {package}/{resource} -> {p}")
                return ConfigUtil._load_yaml_from_path(p)
        except (ModuleNotFoundError, FileNotFoundError):
            return None

    @staticmethod
    def _load_yaml_from_path(path: Path) -> Dict[str, Any]:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}

    @staticmethod
    def _parse_resource_spec(resource_spec: str) -> tuple[str, str]:
        """
        Parse 'package:relative/path.yaml'
        """
        if ":" not in resource_spec:
            raise ValueError(f"Invalid resource spec (expected 'package:resource'): {resource_spec!r}")

        package, resource = resource_spec.split(":", 1)
        package = package.strip()
        resource = resource.strip().lstrip("/")

        if not package or not resource:
            raise ValueError(f"Invalid resource spec: {resource_spec!r}")

        return package, resource

    # ---------------- Filesystem resolution ----------------

    @staticmethod
    def _resolve_filesystem_path(spec: str, *, attempted: list[str] | None = None) -> Path:
        """
        Resolve a filesystem path by checking:
          1) absolute path
          2) CWD-relative
          3) sys.path directories (optional)
        """
        attempted = attempted if attempted is not None else []

        normalized = os.path.expandvars(os.path.expanduser(spec))
        p = Path(normalized)

        # 1) Absolute
        if p.is_absolute():
            attempted.append(str(p))
            if p.exists() and p.is_file():
                logger.debug(f"Found config at absolute path: {p}")
                return p

        # 2) CWD-relative
        cwd_candidate = (Path.cwd() / p).resolve()
        attempted.append(str(cwd_candidate))
        if cwd_candidate.exists() and cwd_candidate.is_file():
            logger.debug(f"Found config relative to CWD: {cwd_candidate}")
            return cwd_candidate

        # 3) sys.path directories
        if ConfigUtil.ENABLE_SYS_PATH_SEARCH:
            for entry in sys.path:
                if not entry:
                    continue
                base = Path(entry)
                if not base.exists() or not base.is_dir():
                    continue
                candidate = base / p
                attempted.append(str(candidate))
                if candidate.exists() and candidate.is_file():
                    logger.debug(f"Found config in sys.path: {candidate}")
                    return candidate

        raise FileNotFoundError(
            "Config file not found on filesystem.\n"
            f"  Given: {spec}\n"
            "  Tried:\n    " + "\n    ".join(attempted)
        )
