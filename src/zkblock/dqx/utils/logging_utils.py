# logging_utils.py
import logging
import os
from typing import Optional


FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
)

# Top-level namespace; you can keep using class/module names, but we recommend
# anchoring under a common prefix to allow centralized control, e.g., "dqx.*".
DEFAULT_NAMESPACE = "dqx"


def _parse_level(level: Optional[str | int]) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        val = logging.getLevelName(level.upper())
        if isinstance(val, int):
            return val
    return logging.INFO


def configure_logging(
    *,
    level: Optional[str | int] = None,
    stream: Optional[object] = None,
    add_console_handler: bool = True,
    add_file_handler: bool = False,
    file_path: Optional[str] = None,
    file_mode: str = "a",
    propagate: bool = False,
    env_var: str = "DQX_LOG_LEVEL",
    namespace: str = DEFAULT_NAMESPACE,
    formatter: logging.Formatter = FORMATTER,
) -> None:
    """
    Configure the root namespace logger for the DQX framework. Call this ONCE
    from the application to control logging for all framework modules.

    Parameters
    ----------
    level : str|int|None
        Desired log level (e.g., "DEBUG", logging.INFO). If None, falls back to env var.
    stream : file-like|None
        Stream for console handler; defaults to sys.stderr if None.
    add_console_handler : bool
        Attach a StreamHandler if True (only if not already attached).
    add_file_handler : bool
        Attach a FileHandler if True (only if not already attached).
    file_path : str|None
        Path to the log file (required if add_file_handler=True).
    file_mode : str
        File mode for FileHandler, default "a".
    propagate : bool
        If True, allow logs to bubble to root (may duplicate if root has handlers).
    env_var : str
        Environment variable name to read level from when `level` is None.
    namespace : str
        The root logger namespace (default "dqx").
    formatter : logging.Formatter
        Formatter to apply to handlers (console/file).
    """
    if level is None:
        level = os.getenv(env_var)
    numeric_level = _parse_level(level)

    root_logger = logging.getLogger(namespace)
    root_logger.setLevel(numeric_level)

    # Attach a console handler exactly once
    if add_console_handler:
        has_console = any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers)
        if not has_console:
            ch = logging.StreamHandler(stream=stream)
            ch.setLevel(numeric_level)
            ch.setFormatter(formatter)
            root_logger.addHandler(ch)

    # Attach a file handler exactly once
    if add_file_handler:
        if not file_path:
            raise ValueError("file_path is required when add_file_handler=True")
        has_file = any(isinstance(h, logging.FileHandler) and getattr(h, "_dqx_path", None) == file_path
                       for h in root_logger.handlers)
        if not has_file:
            fh = logging.FileHandler(file_path, mode=file_mode, encoding="utf-8")
            fh.setLevel(numeric_level)
            fh.setFormatter(formatter)
            # mark to avoid duplicate same-file handlers on repeated configure calls
            setattr(fh, "_dqx_path", file_path)
            root_logger.addHandler(fh)

    root_logger.propagate = propagate


def set_level(level: str | int, namespace: str = DEFAULT_NAMESPACE) -> None:
    """Dynamically change the framework log level (and sync handler levels)."""
    numeric = _parse_level(level)
    lg = logging.getLogger(namespace)
    lg.setLevel(numeric)
    for h in lg.handlers:
        h.setLevel(numeric)


class LoggingHandler:
    """
    Handle logging for the ZkblockDQX engine.
    """

    def __init__(self, name: str | None = None, *, namespace: str = DEFAULT_NAMESPACE):
        """
        Args:
            name: The logger name. If it does not start with the namespace,
                  it will be prefixed (e.g., 'compiler' -> 'dqx.compiler').
            namespace: Root namespace for the framework (default 'dqx').
        """
        if not name:
            fq_name = namespace
        elif name.startswith(namespace):
            fq_name = name
        else:
            # Keep compatibility with passing in class names while still nesting under root
            # e.g., 'MyClass' -> 'dqx.MyClass'; 'dqx.rules' stays as-is.
            fq_name = f"{namespace}.{name}"

        self._logger: logging.Logger = logging.getLogger(fq_name)

    def get_logger(self) -> logging.Logger:
        """Return the underlying logger instance."""
        return self._logger
