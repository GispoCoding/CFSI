from hashlib import md5
from pathlib import Path
from typing import Dict, List

from datacube.model import Dataset as ODCDataset

from cfsi.scripts.index import ODCIndexer
from cfsi.utils.logger import create_logger
from cfsi.utils.utils import container_path_to_global_path  # TODO: add to init.py

LOGGER = create_logger("s2cloudless_index")


class S2CloudlessIndexer(ODCIndexer):

    def __init__(self, name: str = "S2CloudlessIndexer"):
        """ Set up indexer """
        super().__init__(name)

    def index(self, s2cloudless_output: Dict[ODCDataset, Dict[str, Path]]) -> List[ODCDataset]:
        """ Indexes s2cloudless output masks to ODC.
         :param s2cloudless_output: L1C ODCDataset: s2cloudless output mask file paths """
        indexed_datasets: List[ODCDataset] = []
        for l1c_dataset in s2cloudless_output:
            eo3_doc = self.generate_eo3_dataset_doc(l1c_dataset, s2cloudless_output[l1c_dataset])
            dataset, exception = self.add_dataset(eo3_doc)
            if not exception:
                indexed_datasets.append(dataset)
        return indexed_datasets

    def generate_eo3_dataset_doc(self, l1c_dataset: ODCDataset, masks: Dict[str, Path]):
        """ Generates and returns a s2cloudless eo3 metadata document """
        cloud_mask_path = masks["cloud_mask"]
        shadow_mask_path = masks["shadow_mask"]
        # TODO: handle writing to S3
        protocol = "file:/"
        swap_fs = False
        if swap_fs:  # TODO: determine when to swap between host and
            cloud_mask_path, shadow_mask_path = container_path_to_global_path(cloud_mask_path, shadow_mask_path)
        uri = protocol + str(cloud_mask_path.parent)

        l1c_uri = l1c_dataset.uris[0]
        l1c_metadata_uri = l1c_uri + "/metadata.xml"
        l2a_dataset_id = self.l2a_dataset_from_l1c(l1c_dataset).id

        l1c_metadata_doc = self.s3obj_to_etree(self.get_object_from_s3_uri(
            l1c_metadata_uri, RequestPayer="requester"))
        tile_metadata = self.read_s2_tile_metadata(l1c_metadata_doc)
        grids = self.read_s2_grid_metadata(l1c_metadata_doc)

        eo3 = {
            "id": md5(str(uri).encode("utf-8")).hexdigest(),
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "name": "s2a_level1c_s2cloudless",
            },
            "crs": tile_metadata.crs_code,
            "grids": {
                "default": {  # 10m
                    "shape": [grids["10"]["nrows"], grids["10"]["ncols"]],
                    "transform": grids["10"]["trans"],
                },
            },
            "measurements": {
                "cloud_mask": {
                    "path": str(cloud_mask_path),
                },
                "shadow_mask": {
                    "path": str(shadow_mask_path),
                },
            },
            "uri": str(uri),
            "properties": {
                "tile_id": tile_metadata.tile_id,
                "eo:instrument": "MSI",
                "eo:platform": "SENTINEL-2A",  # TODO: read A or B from metadata
                "odc:file_format": "JPEG2000",
                "datetime": tile_metadata.sensing_time,
                "odc:region_code": "".join(Path(l1c_uri).parts[3:6]),
                "mean_sun_zenith": tile_metadata.sun_zenith,
                "mean_sun_azimuth": tile_metadata.sun_azimuth,
                "cloudy_pixel_percentage": tile_metadata.cloudy_pixel_percentage,
                "s3_key": "/".join(Path(l1c_uri).parts[2:]),  # TODO: replace with urlparse
                "l2a_dataset_id": l2a_dataset_id,
            }
        }
        return eo3
