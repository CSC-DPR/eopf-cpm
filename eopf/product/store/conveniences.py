from typing import Any

from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_path


def convert(
    source: EOProductStore,
    target: EOProductStore,
    source_kwargs: dict[str, Any] = {},
    target_kwargs: dict[str, Any] = {},
) -> EOProductStore:
    from eopf.product import open_store

    def _convert(level: list[str]):
        if len(level) == 1 and level[0] in ["", "/"]:
            node_path = ""
            target.write_attrs("", source[node_path].attrs)
        else:
            node_path = join_path(*level, sep=source.sep)
            target[join_path(*level, sep=target.sep)] = source[node_path]

        if source.is_group(node_path):
            for sublevel in source.iter(join_path(*level, sep=source.sep)):
                _convert([*level, sublevel])

    with (open_store(source, mode="r", **source_kwargs), open_store(target, mode="w", **target_kwargs)):
        _convert([""])
    return target
