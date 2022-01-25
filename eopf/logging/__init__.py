import json
import logging
import logging.config
from functools import cache
from typing import Any, Callable

_DEFAULT_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "[%(levelname)s][%(asctime)s][%(name)s] %(message)s",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "eopf": {
            "handlers": ["default"],
            "level": "DEBUG",
        },
    },
}


@cache
def load_logger(config_filename: str = "", format: str = "json") -> None:
    if format == "json":
        if config_filename:
            with open(config_filename, "r") as fp:
                config = json.load(fp)
        else:
            config = _DEFAULT_CONFIG
        return logging.config.dictConfig(config)
    return logging.config.fileConfig(config_filename)


def eopf_logger(msg: str = "{func} args={args} kwargs={kwargs}") -> Callable[..., Any]:
    load_logger()
    logger = logging.getLogger("eopf")

    def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        def apply(*args: Any, **kwargs: Any) -> Any:
            logger.info(msg.format(func=func, args=args, kwargs=kwargs))
            return func(*args, **kwargs)

        return apply

    return wrapper
