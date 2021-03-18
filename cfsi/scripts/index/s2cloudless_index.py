from hashlib import md5
from pathlib import Path
from typing import Dict

from datacube.model import Dataset as ODCDataset

from cfsi.scripts.index import ODCIndexer
from cfsi.utils.logger import create_logger

LOGGER = create_logger("s2cloudless_index")


class S2CloudlessIndexer(ODCIndexer):

    def __init__(self):
        """ Set up indexer """
        super().__init__("S2CloudlessIndexer")

    def generate_eo3_dataset_doc(self, l1c_dataset: ODCDataset, s2cloudless_masks: Dict[str, Path]):
        """ Generates and returns a s2cloudless eo3 metadata document """
        uri = self._generate_mask_uri(s2cloudless_masks)
        measurements = {name: {"path": str(mask_path)}
                        for name, mask_path in s2cloudless_masks.items()}
        properties, grids = self.generate_mask_properties(l1c_dataset)

        eo3 = {
            "id": md5(str(uri).encode("utf-8")).hexdigest(),
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "name": "s2_level1c_s2cloudless",
            },
            "crs": properties["crs"],
            "grids": {
                "default": {  # 10m
                    "shape": [grids["10"]["nrows"], grids["10"]["ncols"]],
                    "transform": grids["10"]["trans"],
                },
            },
            "measurements": measurements,
            "uri": uri,
            "properties": properties
        }
        return eo3
