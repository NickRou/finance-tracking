from __future__ import annotations

from .adapters import CapitalOneAdapter
from .base import InstitutionAdapter


_ADAPTERS: dict[str, InstitutionAdapter] = {
    "capitalone": CapitalOneAdapter(),
}


def get_adapter(institution: str) -> InstitutionAdapter:
    key = institution.strip().lower()
    adapter = _ADAPTERS.get(key)
    if adapter is None:
        supported = ", ".join(sorted(_ADAPTERS))
        raise ValueError(
            f"unsupported institution {institution!r}; supported: {supported}"
        )
    return adapter
