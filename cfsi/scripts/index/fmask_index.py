from hashlib import md5
from pathlib import Path
from typing import Dict

from datacube.model import Dataset as ODCDataset

from cfsi.scripts.index import ODCIndexer


class FmaskIndexer(ODCIndexer):

    def __init__(self):
        super().__init__("FmaskIndexer")

    def generate_eo3_dataset_doc(self, l1c_dataset: ODCDataset, fmask_masks: Dict[str, Path]) -> Dict:
        """ Generates and returns a s2cloudless eo3 metadata document """
        uri = self._generate_mask_uri(fmask_masks)
        measurements = {name: {"path": str(mask_path)}
                        for name, mask_path in fmask_masks.items()}
        properties, grids = self.generate_mask_properties(l1c_dataset)

        eo3 = {
            "id": md5(str(uri).encode("utf-8")).hexdigest(),
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "name": "s2a_level1c_fmask",
            },
            "crs": properties["crs"],
            "grids": {
                "default": {  # 20m for fmask
                    "shape": [grids["20"]["nrows"], grids["20"]["ncols"]],
                    "transform": grids["20"]["trans"],
                },
            },
            "measurements": measurements,
            "uri": uri,
            "properties": properties
        }
        return eo3
