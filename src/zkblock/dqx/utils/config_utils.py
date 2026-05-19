import importlib
import json
import os
import sys
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from zkblock.dqx.utils.logging_utils import LoggingHandler
from zkblock.dqx import yaml_constants as YC
from zkblock.dqx.dqx_rule_registry import DQRuleRegistry
from pyspark.sql.types import StructType
from importlib import resources
import copy


logger = LoggingHandler(__name__).get_logger()


class ConfigUtils:
    """Utility class for handling configurations."""
    DEFAULT_CONFIGS_MODULE = "zkblock.configs"

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
        path = Path(normalized)

        # 1) Absolute path
        if path.is_absolute():
            attempted.append(str(path))
            if path.exists() and path.is_file():
                logger.debug(f"Found config file at absolute path: {path}")
                return path

        # 2) CWD-relative path
        cwd_candidate = (Path.cwd() / path).resolve()
        attempted.append(str(cwd_candidate))
        if cwd_candidate.exists() and cwd_candidate.is_file():
            logger.debug(f"Found config file relative to CWD: {cwd_candidate}")
            return cwd_candidate

        # 3) sys.path directories
        for base in ConfigUtils._sys_path_dirs():
            file = (base / path)
            attempted.append(str(file))
            if file.exists() and file.is_file():
                logger.debug(f"Found config file in sys.path: {file}")
                return file

        # 4) Packaged fallback (works for wheel-installed configs)
        #    NOTE: If you *don't* package configs, you can delete this block.
        try:
            traversable = resources.files(YC.DEFAULT_DQX_CONFIG_FILE).joinpath(str(path))
            # If it exists, convert to real path (may extract to temp)
            with resources.as_file(traversable) as extracted_path:
                extracted = Path(extracted_path)
                attempted.append(f"package:{YC.DEFAULT_DQX_CONFIG_FILE}/{path}")
                if extracted.exists() and extracted.is_file():
                    logger.debug(
                        f"Found config file in package resources: "
                        f"{ConfigUtils.DEFAULT_CONFIGS_MODULE}/{path} -> {extracted}"
                    )
                    return extracted
        except (ModuleNotFoundError, FileNotFoundError):
            attempted.append(f"package:{ConfigUtils.DEFAULT_CONFIGS_MODULE}/{path}")

        error_msg = (
            f"Config file '{path_str}' not found. Attempted locations:\n"
            + "\n".join(f"  - {a}" for a in attempted)
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    @staticmethod
    def load_dqx_config(dqx_config_file: str = None) -> Dict:
        """
        Load YAML into dict following the rules above.
        """
        if dqx_config_file is None or not isinstance(dqx_config_file, str) or not dqx_config_file.strip():
            raise ValueError("config spec must be a non-empty string")

        dqx_config_file = dqx_config_file.strip()

        # Only for the default constant: try packaged first, then fallback to filesystem
        if dqx_config_file == YC.DEFAULT_DQX_CONFIG_FILE:
            try:
                return ConfigUtils._load_yaml(dqx_config_file, prefer_packaged_default=True)
            except FileNotFoundError as e:
                # Re-raise with context (this only happens if both packaged and filesystem fail)
                raise FileNotFoundError(
                    f"Failed to load default DQX config {YC.DEFAULT_DQX_CONFIG_FILE}. Tried packaged first, then filesystem.\n{e}"
                ) from e

        # For everything else: filesystem only
        return ConfigUtils._load_yaml(dqx_config_file, prefer_packaged_default=False)

    @staticmethod
    def _load_yaml(config_path: str, prefer_packaged_default: bool) -> Dict[str, Any]:
        """
        Unified logic:
        - (optional) try packaged load if spec is a resource spec
        - then resolve filesystem and load
        """
        attempted = []

        # 1) Packaged attempt (only if requested AND spec looks like 'package:resource')
        if prefer_packaged_default and ":" in config_path:
            attempted.append(f"packaged:{config_path}")
            try:
                data = ConfigUtils._load_packaged_yaml(config_path)
                if data is not None:
                    return data
            except Exception as ex:
                # Don't hard fail here; we fall back to filesystem
                logger.debug(f"Packaged load failed for {config_path!r}: {ex}")

        # 2) Filesystem attempt (absolute -> CWD -> sys.path)
        path = ConfigUtils._resolve_filesystem_path(config_path, attempted=attempted)
        attempted.append(f"filesystem:{path}")
        return ConfigUtils._load_yaml_from_path(path)

    @staticmethod
    def _load_packaged_yaml(resource_spec: str) -> Dict[str, Any] | None:
        """
        Try to load a YAML resource from the wheel. Returns dict if found; otherwise None.
        """
        package, resource = ConfigUtils._parse_resource_spec(resource_spec)
        traversable = resources.files(package).joinpath(resource)

        try:
            with resources.as_file(traversable) as p:
                p = Path(p)
                if not p.exists() or not p.is_file():
                    return None
                logger.debug(f"Loading packaged YAML: {package}/{resource} -> {p}")
                return ConfigUtils._load_yaml_from_path(p)
        except (ModuleNotFoundError, FileNotFoundError):
            logger.error(f"Packaged load failed for {resource_spec}: resource not found")
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

    @staticmethod
    def load_text_file(file_spec: str) -> str:
        """
        Resolve a text file using the same filesystem rules as YAML loaders
        and return its content. Raises FileNotFoundError / ValueError on issues.
        """
        if not file_spec or not str(file_spec).strip():
            raise ValueError("file_spec must be a non-empty string")

        file = ConfigUtils._resolve_filesystem_path(file_spec)  # reuses your resolver
        if not file.exists() or not file.is_file():
            raise FileNotFoundError(f"File not found: {file_spec} -> {file}")

        return file.read_text(encoding="utf-8")

    @staticmethod
    def _resolve_filesystem_path(config_file: str, *, attempted: list[str] | None = None) -> Path:
        """
        Resolve a filesystem path by checking:
          1) absolute path
          2) CWD-relative
          3) sys.path directories (optional)
        """
        attempted = attempted if attempted is not None else []

        normalized = os.path.expandvars(os.path.expanduser(config_file))
        p = Path(normalized)

        # Absolute
        if p.is_absolute():
            attempted.append(str(p))
            if p.exists() and p.is_file():
                logger.debug(f"Found config at absolute path: {p}")
                return p

        # CWD-relative
        cwd_candidate = (Path.cwd() / p).resolve()
        attempted.append(str(cwd_candidate))
        if cwd_candidate.exists() and cwd_candidate.is_file():
            logger.debug(f"Found config relative to CWD: {cwd_candidate}")
            return cwd_candidate

        # sys.path directories
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
            # could a directory
            if candidate.exists() and candidate.is_dir():
                logger.debug(f"Found directory in sys.path: {candidate}")
                return candidate

        raise FileNotFoundError(
            "Config file not found on filesystem.\n"
            f"  Given: {config_file}\n"
            "  Tried:\n    " + "\n    ".join(attempted)
        )

    @staticmethod
    def _load_rule_packs_from_file(yaml_content: Dict, config_dict: Dict) -> None:
        """Loads the rule packs into the config dict."""
        # Copy version and description if present
        if YC.VERSION_KEY in yaml_content:
            config_dict[YC.VERSION_KEY] = yaml_content[YC.VERSION_KEY]
        if YC.VERSION_DESCRIPTION_KEY in yaml_content:
            config_dict[YC.VERSION_DESCRIPTION_KEY] = yaml_content[YC.VERSION_DESCRIPTION_KEY]
        rule_packs = yaml_content.get(YC.RULE_PACKS_KEY, [])
        if not config_dict.get(YC.RULE_PACKS_KEY):
            config_dict[YC.RULE_PACKS_KEY] = {}
        for rule_pack in rule_packs:
            rule_pack_id = rule_pack.get(YC.ID_KEY)
            if rule_pack_id:
                config_dict[YC.RULE_PACKS_KEY][rule_pack_id] = rule_pack

    ## new ones
    @staticmethod
    def load_all_configs(config_path: str) -> Dict[str, Any]:
        """
        Load and merge all supported sections from a file or directory,
        now with:
        - imports: recursive, order-aware, cycle-safe
        - extends: flattened, order-preserving rule pack resolution

        Returns:
        {
          "rule_packs": { <pack_id>: <flattened pack dict> },
          "sources":    { "sources": { <source_id>: <original source dict> } }
        }
        """
        path = ConfigUtils._resolve_filesystem_path(config_path)
        if not path.exists():
            logger.error(f"Config path not found: {config_path}")
            raise FileNotFoundError(f"Config path not found: {config_path}")

        merged_rule_packs: Dict[str, Any] = {YC.RULE_PACKS_KEY: {}}
        merged_sources: Dict[str, Any] = {YC.SOURCES_KEY: {}}

        visited_files: Set[str] = set()
        import_stack: list[str] = []

        def _validate_and_merge_source(raw: Dict[str, Any], idx: int, yaml_dir: Optional[str]) -> None:
            """
            Minimal validation + optional sql_file -> subquery resolution.
            Stores the ORIGINAL dict keyed by source_id into merged_sources[YC.SOURCES_KEY].
            """
            if not isinstance(raw, dict):
                raise ValueError(f"sources[{idx}] must be an object")

            # Required fields
            source_id = raw.get(YC.SOURCE_ID_KEY)
            if not source_id or not str(source_id).strip():
                raise ValueError(f"sources[{idx}]: Missing required field '{YC.SOURCE_ID_KEY}'.")
            if YC.RULE_PACK_ID_KEY not in raw or not str(raw[YC.RULE_PACK_ID_KEY]).strip():
                raise ValueError(f"sources[{idx}] ({source_id}): Missing required field '{YC.RULE_PACK_ID_KEY}'.")

            # Enforce exactly one of table_name / subquery / sql_file
            table_name = raw.get(YC.TABLE_NAME_KEY)
            subquery = raw.get(YC.SUBQUERY_KEY)
            sql_file = raw.get(YC.SQL_FILE_KEY)
            chosen = [
                n for n, v in [(YC.TABLE_NAME_KEY, table_name), (YC.SUBQUERY_KEY, subquery), (YC.SQL_FILE_KEY, sql_file)]
                if v not in (None, "")
            ]
            if len(chosen) == 0:
                raise ValueError(f"sources[{idx}] ({source_id}): Provide one of 'table_name', 'subquery', or 'sql_file'.")
            if len(chosen) > 1:
                raise ValueError(
                    f"sources[{idx}] ({source_id}): 'table_name' is mutually exclusive with 'subquery' or 'sql_file'."
                )

            # Validate read_type (optional guardrail)
            read_type = (raw.get(YC.READ_TYPE_KEY) or YC.TABLE_KEY).strip()
            if read_type not in {YC.TABLE_KEY, YC.STREAM_KEY, YC.CDF_KEY}:
                raise ValueError(
                    f"sources[{idx}] ({source_id}): Invalid read_type '{read_type}'. "
                    "Allowed: ['cdf','stream','table']"
                )

            # Validate sql_file and subquery mutual exclusivity
            if sql_file and subquery:
                raise ValueError(
                    f"sources[{idx}] ({source_id}): 'sql_file' is mutually exclusive with 'subquery'."
                )
            if sql_file:
                raw[YC.SQL_FILE_TEXT_KEY] = ConfigUtils.load_text_file(sql_file)
            # Merge like rule_packs: keep original dict keyed by ID
            sid = str(source_id)
            if sid in merged_sources[YC.SOURCES_KEY]:
                logger.warning(f"Overriding source_id '{sid}' from another file-last one wins.")
            merged_sources[YC.SOURCES_KEY][sid] = raw

        def _process_yaml_file(full_path: str) -> None:
            """
            Re-usable, order-aware loader that:
              1) Recursively loads `imports` (depth-first)
              2) Loads this file's rule_packs and sources into merged maps
            """
            nonlocal visited_files, import_stack

            path = ConfigUtils._resolve_filesystem_path(full_path)
            yaml_file_path = str(path.resolve())

            # Circular import check
            if yaml_file_path in import_stack:
                chain = " -> ".join(import_stack + [yaml_file_path])
                raise ValueError(f"Circular YAML import detected: {chain}")
            if yaml_file_path in visited_files:
                return  # already processed

            import_stack.append(yaml_file_path)
            yaml_content = ConfigUtils._load_yaml(yaml_file_path, prefer_packaged_default=False)

            # 1) imports: recurse first, in order
            for imp_path in (yaml_content.get(YC.IMPORTS_KEY) or []):
                _process_yaml_file(imp_path)

            # 2) merge this file's rule_packs + sources
            ConfigUtils._load_rule_packs_from_file(yaml_content, merged_rule_packs)

            sources = yaml_content.get(YC.SOURCES_KEY)
            if sources:
                if not isinstance(sources, list):
                    raise ValueError("'sources' must be a list of mappings")
                for idx, raw in enumerate(sources):
                    _validate_and_merge_source(raw, idx, yaml_dir=os.path.dirname(yaml_file_path))

            visited_files.add(yaml_file_path)
            import_stack.pop()

        # --- Directory vs single file ---
        if path.is_dir():
            for f in sorted(os.listdir(path)):
                if f.endswith(".yaml") or f.endswith(".yml"):
                    _process_yaml_file(os.path.join(path, f))
        else:
            _process_yaml_file(config_path)

        # --- Post-pass: flatten 'extends' across all rule packs ---
        packs_map = merged_rule_packs.get(YC.RULE_PACKS_KEY, {})
        resolved_packs = ConfigUtils._resolve_rule_packs_extends(packs_map)
        logger.debug("Resolved rule packs: %s", json.dumps(resolved_packs, indent=2))
        return {
            YC.RULE_PACKS_KEY: resolved_packs,   # <pack_id> -> fully-flattened pack
            YC.SOURCES_KEY: merged_sources,      # unchanged shape
        }

    @staticmethod
    def _resolve_rule_packs_extends(all_packs: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Resolve 'extends' for every rule pack using DFS with cycle detection.
        Produces a flattened copy with rules merged & ordered.

        all_packs: { pack_id: { ... pack content including optional 'extends' ... } }
        returns:    { pack_id: { ... flattened pack, no 'extends' ... } }
        """
        resolved: Dict[str, Dict[str, Any]] = {}
        resolving: Set[str] = set()  # for cycle detection

        def _merge_columns(parent_cols: Dict[str, Any], child_cols: Dict[str, Any]) -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            # start with parent's structure
            for col, payload in (parent_cols or {}).items():
                out[col] = {
                    **(payload or {}),
                    YC.RULES_KEY: list((payload or {}).get(YC.RULES_KEY, []) or []),
                }

            # apply child modifications
            for col, payload in (child_cols or {}).items():
                child_rules = list((payload or {}).get(YC.RULES_KEY, []) or [])
                replace_ids = set((payload or {}).get(YC.REPLACE_RULES_KEY, []) or [])

                base = out.get(col, {YC.RULES_KEY: []})
                base_rules = list(base.get(YC.RULES_KEY, []) or [])

                # remove by id if requested
                if replace_ids:
                    base_rules = [r for r in base_rules if r.get(YC.ID_KEY) not in replace_ids]

                # append child rules keeping order
                merged_rules = base_rules + child_rules

                # merge back any other column-level keys (e.g., future 'filter' at column scope)
                merged_payload = dict(base)
                merged_payload[YC.RULES_KEY] = merged_rules
                # Keep/overwrite other child keys except replace_rules
                for k, v in (payload or {}).items():
                    if k in (YC.RULES_KEY, YC.REPLACE_RULES_KEY):
                        continue
                    merged_payload[k] = v

                out[col] = merged_payload

            return out

        def _merge_ordered_rules(parent_rules: List[Dict[str, Any]], child_rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """
            Merge rule lists preserving order:
            - start with parent's list
            - for each child rule:
              * if same id exists in parent, replace it (child overrides)
              * else append it
            """
            parent_rules = list(parent_rules or [])
            child_rules = list(child_rules or [])

            # index parent by id to allow replacement
            index = { r.get(YC.ID_KEY): i for i, r in enumerate(parent_rules) if r and r.get(YC.ID_KEY) is not None }

            result = parent_rules[:]  # preserve parent order
            for cr in child_rules:
                cid = cr.get(YC.ID_KEY)
                if cid is None:
                    # if rule has no id, just append
                    result.append(cr)
                    continue
                if cid in index:
                    # replace at the same position
                    pos = index[cid]
                    result[pos] = cr
                else:
                    # append new rule
                    index[cid] = len(result)
                    result.append(cr)

            return result

        def _merge_pack(parent: Dict[str, Any], child: Dict[str, Any]) -> Dict[str, Any]:
            """
            Merge a child pack onto a parent pack (both already flattened)
            Keys: version/description are documentation; child overrides if present.
                  fast_fail/category/subcategory/rule_type: child overrides if present.
                  columns: merged with replace_rules semantics and order preserved.
            """
            merged = {}

            # Copy parent first
            for k, v in (parent or {}).items():
                merged[k] = copy.deepcopy(v)

            # Overlay child's simple keys (override if provided)
            for k in [YC.VERSION_KEY, YC.VERSION_DESCRIPTION_KEY, YC.RULE_VERSION_KEY,
                      YC.CATEGORY_KEY, YC.SUBCATEGORY_KEY, YC.RULE_TYPE_KEY, YC.FAST_FAIL_KEY]:
                if child.get(k) is not None:
                    merged[k] = child[k]

            # Columns (special handling)
            merged[YC.COLUMNS_KEY] = _merge_columns(
                (parent or {}).get(YC.COLUMNS_KEY, {}),
                (child or {}).get(YC.COLUMNS_KEY, {}),
            )

            # Dataset rules (NEW special handling)
            merged[YC.DATASET_RULES_KEY] = _merge_ordered_rules(
                (parent or {}).get(YC.DATASET_RULES_KEY, []),
                (child or {}).get(YC.DATASET_RULES_KEY, []),
            )

            # Timeliness rules (business-level row matrix config)
            merged[YC.TIMELINESS_RULES_KEY] = _merge_ordered_rules(
                (parent or {}).get(YC.TIMELINESS_RULES_KEY, []),
                (child or {}).get(YC.TIMELINESS_RULES_KEY, []),
            )

            # Remove 'extends' from the final, it's flattened
            merged.pop(YC.EXTENDS_KEY, None)
            return merged

        def _dfs(pack_id: str) -> Dict[str, Any]:
            if pack_id in resolved:
                return resolved[pack_id]
            if pack_id in resolving:
                chain = " -> ".join(list(resolving) + [pack_id])
                raise ValueError(f"Circular 'extends' detected among rule packs: {chain}")

            if pack_id not in all_packs:
                raise KeyError(f"Unknown rule_pack id referenced in 'extends': {pack_id}")

            resolving.add(pack_id)
            node = all_packs[pack_id] or {}
            bases = node.get(YC.EXTENDS_KEY) or []

            # Start from an empty base, then merge each base in order
            acc: Dict[str, Any] = {}
            for base_id in bases:
                base_flat = _dfs(base_id)
                acc = _merge_pack(acc, base_flat)

            # Finally merge this node on top
            flat = _merge_pack(acc, node)
            resolving.remove(pack_id)
            resolved[pack_id] = flat
            return flat

        # Resolve all
        for pid in all_packs.keys():
            _dfs(pid)

        return resolved

    @staticmethod
    def load_rule_packs_configs(config_path: str) -> Dict:
        """
        Backward-compatible wrapper that returns only 'rule_packs'.
        Internally delegates to the unified loader.
        """
        all_cfg = ConfigUtils.load_all_configs(config_path)
        return all_cfg.get("rule_packs", {})

    @staticmethod
    def load_rule_configs(config_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Convenience: returns just the {source_id: source_dict} map.
        """
        all_cfg = ConfigUtils.load_all_configs(config_path)
        return ConfigUtils.get_sources_map(all_cfg)

    @staticmethod
    def get_sources_map(all_configs: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Returns the {source_id: source_obj} map.
        Accepts either the top-level dict from load_all_configs(), or a dict
        containing YC.SOURCES_KEY directly.
        """
        if YC.SOURCES_KEY in all_configs:
            # already the { YC.SOURCES_KEY: {...} } shape
            return all_configs[YC.SOURCES_KEY] or {}

        maybe = all_configs.get("sources")
        if isinstance(maybe, dict) and YC.SOURCES_KEY in maybe:
            return maybe[YC.SOURCES_KEY] or {}
        return {}

    @staticmethod
    def load_rule_packs_configs(config_path: str) -> Dict:
        """Loads the rule packs into the config dict."""
        p = ConfigUtils._resolve_filesystem_path(config_path)

        if not p.exists():
            logger.error(f"Config path not found: {config_path}")
            raise FileNotFoundError(f"Config path not found: {config_path}")
        config_dict = {}
        # Load and merge rule packs
        if p.is_dir():
            config_files = [file for file in os.listdir(p)
                            if file.endswith(".yaml") or file.endswith(".yml")]
            for filename in config_files:
                full_path = os.path.join(p, filename)
                yaml_content = ConfigUtils._load_yaml(full_path, prefer_packaged_default=False)
                ConfigUtils._load_rule_packs_from_file(yaml_content, config_dict)
        else:
            yaml_content = ConfigUtils._load_yaml(config_path, prefer_packaged_default=False)
            ConfigUtils._load_rule_packs_from_file(yaml_content, config_dict)

        return config_dict

    @staticmethod
    def _load_callable(callable_path):
        module_path, func_name = callable_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)

    @staticmethod
    def _build_registry(rule_config_paths: list, registry: DQRuleRegistry, custom_rule_paths: bool = None ) -> DQRuleRegistry:

        rule_mapping_yaml= {}
        for rule_config_path in rule_config_paths:
            if custom_rule_paths:
                rule_mapping_yaml = ConfigUtils._load_yaml(rule_config_path, prefer_packaged_default=False)
            else:
                rule_mapping_yaml = ConfigUtils._load_yaml(rule_config_path, prefer_packaged_default=True)

            # Row Rules
            for rule in rule_mapping_yaml.get(YC.ROW_RULES_KEY, []):
                rule_id = rule[YC.ID_KEY]
                callable_ref = rule[YC.CALLABLE_REF_KEY]
                func = ConfigUtils._load_callable(callable_ref)
                registry.register_row(rule_id, func)

            # Data-set Rules
            for rule in rule_mapping_yaml.get(YC.DATASET_RULES_KEY, []):
                rule_id = rule[YC.ID_KEY]
                callable_ref = rule[YC.CALLABLE_REF_KEY]
                func = ConfigUtils._load_callable(callable_ref)
                registry.register_dataset(rule_id, func)

        return registry

    @staticmethod
    def get_schema(schema_file: str) -> StructType:
        """Loads schema from JSON file."""
        # Get the absolute path of the current module
        p = ConfigUtils._resolve_filesystem_path(schema_file)

        if not p.exists():
            logger.error(f"Schema file not found: {schema_file}")
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(p) as file:
            return StructType.fromJson(json.load(file))
