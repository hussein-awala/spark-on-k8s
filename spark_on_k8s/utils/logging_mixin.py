from __future__ import annotations

import logging


class LoggingMixin:
    logger: logging.Logger

    def __init__(self, *args, logger_name: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(logger_name or self.__class__.__name__)

    def log(self, msg, level: int = logging.INFO, should_print: bool = False):
        if should_print:
            print(msg)
        else:
            self.logger.log(level=level, msg=msg)
