from hashlib import md5
from pathlib import Path
from typing import Dict

import rasterio
import xarray as xa

from cfsi.scripts.index import ODCIndexer
from cfsi.utils.logger import create_logger

LOGGER = create_logger("mosaic_index", level=10)


class MosaicIndexer(ODCIndexer):
    """ Index output cloudless mosaics to ODC """

    def __init__(self):
        super().__init__("MosaicIndexer")

    def index(self, mosaic_ds: xa.Dataset, file_path: Path):
        """ Create new cloudless mosaic ODCDataset from mosaic in Path """
        eo3 = self.generate_eo3_dataset_doc(mosaic_ds, file_path)
        dataset, exception = self.add_dataset(eo3)
        if not exception:
            LOGGER.info(f"Indexed cloudless mosaic {dataset}")

    @staticmethod
    def generate_eo3_dataset_doc(mosaic_ds: xa.Dataset, file_path: Path) -> Dict:
        """ Generates and returns a cloudless mosaic eo3 metadata document """
        with rasterio.open(file_path) as src:
            transform = src.meta["transform"]

        uri = f"file://{file_path}"
        eo3 = {
            "id": md5(str(uri).encode("utf-8")).hexdigest(),
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "name": "cloudless_mosaic",
            },
            "crs": mosaic_ds.attrs["crs"],
            "grids": {
                "default": {  # 10m
                    "shape": (mosaic_ds.dims["y"], mosaic_ds.dims["x"]),
                    "transform": [v for v in transform],
                },
            },
            "measurements": {
                "B01": {
                    "path": uri,
                    "band": 1
                },
                "B02": {
                    "path": uri,
                    "band": 2
                },
                "B03": {
                    "path": uri,
                    "band": 3
                },
                "B04": {
                    "path": uri,
                    "band": 4
                },
            },
            "uri": str(uri),
            "properties": {
                "tile_id": file_path.name,
                "eo:instrument": "MSI",
                "eo:platform": "SENTINEL-2",
                "odc:file_format": "GeoTIFF",
                "datetime": str(mosaic_ds.time.time.values),
            }
        }
        LOGGER.debug(eo3)
        return eo3
