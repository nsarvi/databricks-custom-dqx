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
            load packaged wheel resource (package:resource)
      - Else:
            treat spec as filesystem path:
              1) absolute
              2) CWD-relative
              3) sys.path directories
    """

    # Standard, unambiguous packaged default (wheel resource)
    DEFAULT_DQX_CONFIG_FILE: str = "idea4.configs:idea4_config.yaml"

    ENABLE_SYS_PATH_SEARCH: bool = True

    @staticmethod
    def load_dqx_config(spec: str) -> Dict[str, Any]:
        if spec is None or not isinstance(spec, str) or not spec.strip():
            raise ValueError("config spec must be a non-empty string")

        spec = spec.strip()

        # ✅ Only load from wheel when the caller passes the default constant
        if spec == ConfigUtil.DEFAULT_DQX_CONFIG_FILE:
            return ConfigUtil._load_packaged_yaml(spec)

        # ✅ Otherwise treat it as a filesystem path (even if it contains ':')
        path = ConfigUtil._resolve_filesystem_path(spec)
        return ConfigUtil._load_yaml_file(path)

    # ---------------- Packaged default ----------------

    @staticmethod
    def _load_packaged_yaml(resource_spec: str) -> Dict[str, Any]:
        package, resource = ConfigUtil._parse_resource_spec(resource_spec)
        traversable = resources.files(package).joinpath(resource)

        try:
            with resources.as_file(traversable) as p:
                p = Path(p)
                if not p.exists() or not p.is_file():
                    raise FileNotFoundError
                logger.debug(f"Loading packaged default config: {package}/{resource}")
                return ConfigUtil._load_yaml_file(p)
        except ModuleNotFoundError as e:
            raise FileNotFoundError(f"Package not found: {package}") from e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Packaged default config not found: {package}/{resource}") from e

    @staticmethod
    def _parse_resource_spec(resource_spec: str) -> tuple[str, str]:
        """
        Parse 'package:relative/path.yaml'
        """
        if ":" not in resource_spec:
            raise ValueError(
                f"Invalid default resource spec (expected 'package:resource'): {resource_spec!r}"
            )
        package, resource = resource_spec.split(":", 1)
        package = package.strip()
        resource = resource.strip().lstrip("/")
        if not package or not resource:
            raise ValueError(f"Invalid default resource spec: {resource_spec!r}")
        return package, resource

    # ---------------- Filesystem resolution ----------------

    @staticmethod
    def _resolve_filesystem_path(path_str: str) -> Path:
        attempted = []

        normalized = os.path.expandvars(os.path.expanduser(path_str))
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
            "Config file not found.\n"
            f"  Given: {path_str}\n"
            "  Tried:\n    " + "\n    ".join(attempted)
        )

    # ---------------- YAML helper ----------------

    @staticmethod
    def _load_yaml_file(path: Path) -> Dict[str, Any]:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}
