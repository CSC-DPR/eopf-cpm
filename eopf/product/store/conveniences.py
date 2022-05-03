import warnings
from typing import Any

from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_path


def convert(
    source: EOProductStore,
    target: EOProductStore,
    source_kwargs: dict[str, Any] = {},
    target_kwargs: dict[str, Any] = {},
) -> EOProductStore:
    """Help to convert a source store format to another by
    writting everything in the target store.

    `source` is open in 'r' mode, and `target` in 'w' mode.

    Parameters
    ----------
    source: EOProductStore
        store to read to retrieve data
    target: EOProductStore
        store to write retrived data
    source_kwargs: dict, optional
        specific arguments to open the source store
    target_kwargs: dict, optional
        specific arguments to open the write store

    Returns
    -------
    EOProductStore
    """
    from eopf.product import open_store
    from eopf.product.core import EOGroup

    def _convert(level: list[str]) -> None:
        if len(level) == 1:
            node_path = ""
            target.write_attrs(node_path, source[node_path].attrs)
        else:
            node_path = join_path(*level, sep=source.sep)
            target[join_path(*level, sep=target.sep)] = source[node_path]
        if source.is_group(node_path):
            for sublevel in source.iter(join_path(*level, sep=source.sep)):
                try:
                    _convert([*level, sublevel])
                except IndexError:
                    warnings.warn("Itering over missing files can cause inconsistency")
                    target[join_path(*level, sublevel, sep=target.sep)] = EOGroup()

    source_kwargs.setdefault("mode", "r")
    target_kwargs.setdefault("mode", "w")
    with (open_store(source, **source_kwargs), open_store(target, **target_kwargs)):
        _convert([""])
    return target
