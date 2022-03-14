import json

from netCDF4 import Dataset


class Netcdfdecoder:
    def __init__(self, url_or_obj: str, mode: str = "r"):
        if isinstance(url_or_obj, str):
            self._node = Dataset(url_or_obj, mode=mode)
        else:
            self._node = url_or_obj

    def __getitem__(self, key: str):
        return Netcdfdecoder(self._node[key])

    @property
    def attrs(self):
        result = dict()
        for key, value in self._node.__dict__.items():
            try:
                result[key] = json.loads(value)
            except json.decoder.JSONDecodeError:
                result[key] = value
        return result
