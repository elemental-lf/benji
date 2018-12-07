import threading
from typing import Dict, Tuple, Optional

import zstandard

from benji.config import Config, ConfigDict
from benji.transform.base import TransformBase


class Transform(TransformBase):

    def __init__(self, *, config: Config, name: str, module_configuration: ConfigDict) -> None:
        super().__init__(config=config, name=name, module_configuration=module_configuration)

        self.level: str = Config.get_from_dict(
            module_configuration,
            'level',
            types=int,
            check_func=lambda v: v >= 1 and v <= zstandard.MAX_COMPRESSION_LEVEL,
            check_message='Option level must be between 1 and {} (inclusive)'.format(zstandard.MAX_COMPRESSION_LEVEL))

        dict_data_file: str = Config.get_from_dict(module_configuration, 'dictDataFile', None, types=str)
        if dict_data_file:
            with open(dict_data_file, 'rb') as f:
                dict_data_content = f.read()
            self._dict_data = zstandard.ZstdCompressionDict(dict_data_content, dict_type=zstandard.DICT_TYPE_FULLDICT)
            self._dict_data.precompute_compress(self.level)
        else:
            self._dict_data = None

        self._compressors: zstandard.ZstdCompressor = {}
        self._decompressors: zstandard.ZstdDecompressor = {}
        self._identifier = name

    def _get_compressor(self) -> zstandard.ZstdCompressor:
        thread_id = threading.get_ident()

        if thread_id in self._compressors:
            return self._compressors[thread_id]

        if self._dict_data:
            cctx = zstandard.ZstdCompressor(
                level=self.level,
                dict_data=self._dict_data,
                write_checksum=False,  # We have our own checksum
                write_content_size=False)  # We know the uncompressed size
        else:
            cctx = zstandard.ZstdCompressor(
                level=self.level,
                write_checksum=False,  # We have our own checksum
                write_content_size=False)  # We know the uncompressed size

        self._compressors[thread_id] = cctx
        return cctx

    def _get_decompressor(self, dict_id: int = 0) -> zstandard.ZstdDecompressor:
        thread_id = threading.get_ident()

        if thread_id in self._decompressors:
            return self._decompressors[thread_id]

        if self._dict_data:
            dctx = zstandard.ZstdDecompressor(dict_data=self._dict_data)
        else:
            dctx = zstandard.ZstdDecompressor()

        self._decompressors[thread_id] = dctx
        return dctx

    def encapsulate(self, *, data: bytes) -> Tuple[Optional[bytes], Optional[Dict]]:
        data_encapsulated = self._get_compressor().compress(data)
        if len(data_encapsulated) < len(data):
            return data_encapsulated, {'original_size': len(data)}
        else:
            return None, None

    def decapsulate(self, *, data: bytes, materials: Dict) -> bytes:
        if 'original_size' not in materials:
            raise KeyError('Compression materials are missing required key original_size.')
        return self._get_decompressor().decompress(data, max_output_size=materials['original_size'])
