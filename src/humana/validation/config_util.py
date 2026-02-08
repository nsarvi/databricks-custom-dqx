from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List

import yaml
from importlib import resources

logger = logging.getLogger(__name__)


class ConfigUtils:
    DEFAULT_CONFIGS_MODULE = "idea4.configs"  # <-- change to your package path

    @staticmethod
    def _normalize_path_str(path_str: str) -> str:
        # Expand "~" and env vars like "$HOME/..." or "${VAR}/..."
        return os.path.expandvars(os.path.expanduser(path_str))

    @staticmethod
    def _sys_path_dirs() -> List[Path]:
        """
        Yield sys.path entries that are real directories.
        Skips '', None, zip/egg files, and non-existent paths.
        """
        dirs: List[Path] = []
        for entry in sys.path:
            if not entry:  # '' or None
                continue
            try:
                p = Path(entry)
            except TypeError:
                continue
            if p.exists() and p.is_dir():
                dirs.append(p)
        return dirs

    @staticmethod
    def _resolve_path(path_str: str) -> Path:
        """
        Resolve a config file path by checking:
        1) Absolute path (after expanding ~ and env vars)
        2) CWD-relative path
        3) sys.path directories (directory entries only)
        4) Packaged config resources (wheel-owned) -> extracted to a temp file
        """
        attempted: List[str] = []

        normalized = ConfigUtils._normalize_path_str(path_str)
        p = Path(normalized)

        # 1) Absolute path
        if p.is_absolute():
            attempted.append(str(p))
            if p.exists() and p.is_file():
                logger.debug(f"Found config file at absolute path: {p}")
                return p

        # 2) CWD-relative path
        cwd_candidate = (Path.cwd() / p).resolve()
        attempted.append(str(cwd_candidate))
        if cwd_candidate.exists() and cwd_candidate.is_file():
            logger.debug(f"Found config file relative to CWD: {cwd_candidate}")
            return cwd_candidate

        # 3) sys.path directories
        for base in ConfigUtils._sys_path_dirs():
            candidate = (base / p)
            attempted.append(str(candidate))
            if candidate.exists() and candidate.is_file():
                logger.debug(f"Found config file in sys.path: {candidate}")
                return candidate

        # 4) Packaged fallback (works for wheel-installed configs)
        #    NOTE: If you *don't* package configs, you can delete this block.
        try:
            traversable = resources.files(ConfigUtils.DEFAULT_CONFIGS_MODULE).joinpath(str(p))
            # If it exists, convert to real path (may extract to temp)
            with resources.as_file(traversable) as extracted_path:
                extracted = Path(extracted_path)
                attempted.append(f"package:{ConfigUtils.DEFAULT_CONFIGS_MODULE}/{p}")
                if extracted.exists() and extracted.is_file():
                    logger.debug(
                        f"Found config file in package resources: "
                        f"{ConfigUtils.DEFAULT_CONFIGS_MODULE}/{p} -> {extracted}"
                    )
                    return extracted
        except (ModuleNotFoundError, FileNotFoundError):
            attempted.append(f"package:{ConfigUtils.DEFAULT_CONFIGS_MODULE}/{p}")

        error_msg = (
            f"Config file '{path_str}' not found. Attempted locations:\n"
            + "\n".join(f"  - {a}" for a in attempted)
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    @staticmethod
    def load_config(config_file: str, env_config_file: Optional[str] = None) -> Dict:
        """
        Load base config, optionally merge env config on top (env overrides base).
        """
        base_path = ConfigUtils._resolve_path(config_file)
        with open(base_path, "r") as f:
            config = yaml.safe_load(f) or {}

        if env_config_file:
            env_path = ConfigUtils._resolve_path(env_config_file)
            with open(env_path, "r") as f:
                env_cfg = yaml.safe_load(f) or {}
            # shallow merge; replace with deep merge if you need nested overriding
            config.update(env_cfg)

        return config
