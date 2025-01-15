from __future__ import annotations

import warnings


class WarningCache:
    _warning_ids = set()

    @classmethod
    def warn(
        cls, *, warning_id: str, message: str, category: type[Warning] = UserWarning, stacklevel: int = 1
    ):
        if warning_id not in cls._warning_ids:
            cls._warning_ids.add(warning_id)
            warnings.warn(message=message, category=category, stacklevel=stacklevel)


class LongAppNameWarning(UserWarning):
    pass
