from hashlib import md5
from logging import DEBUG
from pathlib import Path
from typing import Dict, List, Union

from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset as ODCDataset
from datacube.utils import changes
from datacube.utils.changes import DocumentMismatchError

from .index import ODCIndexer
from ...utils.logger import create_logger

LOGGER = create_logger("s2cloudless-indexer", level=DEBUG)


class S2CloudlessIndexer(ODCIndexer):

    def __init__(self, name: str = "S2CloudlessIndexer"):
        """ Set up indexer """
        super().__init__(name)

    def index(self, s2cloudless_output: Dict[ODCDataset, List[Path]]):
        """ Indexes s2cloudless output masks to ODC.
         :param s2cloudless_output: L1C ODCDataset: s2cloudless output mask file paths """
        for l1c_dataset in s2cloudless_output:
            output_files = s2cloudless_output[l1c_dataset]
            eo3_doc = self.generate_eo3_dataset_doc(l1c_dataset, output_files)
            self.add_dataset(eo3_doc)

    def generate_eo3_dataset_doc(self, l1c_dataset: ODCDataset, file_paths: List[Path]):
        """ Generates and returns a s2cloudless eo3 metadata document """
        output_uri = "file:/" + str(file_paths[0].parent)  # TODO: handle S3 output
        l1c_uri = l1c_dataset.uris[0]
        l1c_metadata_uri = l1c_uri + "/metadata.xml"
        l2a_dataset_id = self.l2a_dataset_from_l1c(l1c_dataset).id

        LOGGER.debug(f"Getting L1C metadata.xml object: {l1c_metadata_uri}")
        l1c_metadata_doc = self.s3obj_to_etree(self.get_object_from_s3_uri(
            l1c_metadata_uri, ResponseCacheControl="no-cache", RequestPayer="requester"))
        tile_metadata = self.read_s2_tile_metadata(l1c_metadata_doc)
        grids = self.read_s2_grid_metadata(l1c_metadata_doc)

        eo3 = {
            "id": md5(str(output_uri).encode("utf-8")).hexdigest(),
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
                "B01": {
                    "grid": "default",
                    "path": str(file_paths[0]),
                },
                "B02": {
                    "grid": "default",
                    "path": str(file_paths[1]),
                },
            },
            "uri": str(output_uri),
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

    def add_dataset(self, doc: Dict, **kwargs) -> (ODCDataset, Union[Exception, None]):
        """ Adds dataset to dcIndex """
        uri = doc["uri"]
        LOGGER.info(f"Indexing {uri}")
        index = self.dc.index
        resolver = Doc2Dataset(index, **kwargs)
        dataset, err = resolver(doc, uri)
        if err is not None:
            LOGGER.error(f"Error indexing {uri}: {err}")
            return dataset, err
        try:
            index.datasets.add(dataset)
        except DocumentMismatchError:
            index.datasets.update(dataset, {tuple(): changes.allow_any})
        except Exception as err:
            LOGGER.error(f"Unhandled exception {err}")
            pass

        return dataset, err
