from logging import DEBUG
from queue import Queue
from pathlib import Path
from queue import Empty
from typing import Dict, Union, List

from xml.etree import ElementTree
from hashlib import md5

from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset as ODCDataset
from datacube.utils import changes

from cfsi import config
from cfsi.scripts.index import ODCIndexer
from cfsi.utils.logger import create_logger

from cfsi.constants import (GUARDIAN, L2A_BUCKET, S2_MEASUREMENTS, S2_PRODUCT_NAMES)

LOGGER = create_logger("s2-index", level=DEBUG)


class S2Indexer(ODCIndexer):
    """ Index Sentinel 2 scenes from AWS S3 """

    def __init__(self, name: str = "S2Indexer"):
        super().__init__(name)

    def index(self):
        """ Index S2 datasets to from S3 ODC """
        LOGGER.info("Indexing Sentinel 2 datasets from S3")
        for bucket_name in config.index.s2_index.s3_buckets:
            LOGGER.info(f"Indexing bucket {bucket_name}")
            self.index_s3_bucket(bucket_name)
            LOGGER.info(f"Bucket {bucket_name} indexed")

    def index_s3_bucket(self, bucket_name: str):
        """ Indexes the contents of a single S3 bucket to ODC """
        LOGGER.info("Generating indexing queue from config")
        queue = self.generate_indexing_queue(bucket_name)
        q_size = queue.qsize()
        LOGGER.info(f"Indexing {q_size} {bucket_name} tiles")
        self.index_from_s3(bucket_name, queue)  # TODO: multithread if necessary
        LOGGER.info(f"Finished indexing {q_size} {bucket_name} tiles")

    def generate_indexing_queue(self, bucket_name: str) -> Queue:
        """ Generates and returns a queue of S3 keys to index """
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        queue = Queue()
        prefixes = self.generate_s3_prefixes()
        for prefix in prefixes:
            for obj in bucket.objects.filter(Prefix=prefix, RequestPayer="requester"):
                if obj.key.endswith("metadata.xml"):
                    queue.put(obj.key)

        queue.put(GUARDIAN)
        return queue

    @staticmethod
    def generate_s3_prefixes() -> List[str]:
        """ Generates a list of S3 bucket prefixes based on config """
        prefixes = []
        for grid in config.index.s2_index.grids:
            a = grid[:2]
            b = grid[2:3]
            c = grid[3:]
            for year in config.index.s2_index.years:
                prefixes += [f"tiles/{a}/{b}/{c}/{year}/{month}"
                             for month in config.index.s2_index.months]
        return prefixes

    def index_from_s3(self, bucket_name: str, queue):
        """ Indexes S2 tiles from S3 bucket from a queue of keys """
        while True:
            try:
                key = queue.get(timeout=60)
                if key == GUARDIAN:
                    break
                obj = self.get_object_from_s3(bucket_name, key, RequestPayer="requester")
                data = self.s3obj_to_etree(obj)
                uri = self.generate_s3_uri(bucket_name, key)
                dataset_doc = self.generate_eo3_dataset_doc(bucket_name, uri, data)
                self.add_dataset(dataset_doc, uri)
                queue.task_done()
            except Empty:
                break
            except EOFError:
                break

    def generate_eo3_dataset_doc(self, bucket_name: str, uri: str, data: ElementTree) -> dict:
        """ Generates an eo3 metadata document for ODC indexing """
        tile_metadata = self.read_s2_tile_metadata(data)
        grids = self.read_s2_grid_metadata(data)

        eo3 = {
            "id": md5(uri.encode("utf-8")).hexdigest(),
            "$schema": "https://schemas.opendatacube.org/dataset",
            "product": {
                "name": S2_PRODUCT_NAMES[bucket_name],
            },
            "crs": tile_metadata.crs_code,
            "grids": {
                "default": {  # 10m
                    "shape": [grids["10"]["nrows"], grids["10"]["ncols"]],
                    "transform": grids["10"]["trans"],
                },
                "20m": {
                    "shape": [grids["20"]["nrows"], grids["20"]["ncols"]],
                    "transform": grids["20"]["trans"],
                },
                "60m": {
                    "shape": [grids["60"]["nrows"], grids["60"]["ncols"]],
                    "transform": grids["60"]["trans"],
                },
            },
            "measurements": self.generate_measurements(bucket_name),
            "location": uri,
            "properties": {
                "tile_id": tile_metadata.tile_id,
                "eo:instrument": "MSI",
                "eo:platform": "SENTINEL-2A",  # TODO: read A or B from metadata
                "odc:file_format": "JPEG2000",
                "datetime": tile_metadata.sensing_time,
                "odc:region_code": "".join(Path(uri).parts[3:6]),
                "mean_sun_zenith": tile_metadata.sun_zenith,
                "mean_sun_azimuth": tile_metadata.sun_azimuth,
                "cloudy_pixel_percentage": tile_metadata.cloudy_pixel_percentage,
                "s3_key": "/".join(Path(uri).parts[2:]),
            },
            "lineage": {},
        }

        return self.relative_s3_keys_to_absolute(eo3, uri)

    def add_dataset(self, doc: Dict, uri: str, **kwargs) -> (ODCDataset, Union[Exception, None]):
        """ Adds dataset to dcIndex """
        LOGGER.info("Indexing %s", uri)
        index = self.dc.index
        resolver = Doc2Dataset(index, **kwargs)
        dataset, err = resolver(doc, uri)
        if err is not None:
            LOGGER.error(err)
            return dataset, err
        try:
            index.datasets.add(dataset)  # Source policy to be checked in sentinel 2 dataset types
        except changes.DocumentMismatchError:
            index.datasets.update(dataset, {tuple(): changes.allow_any})
        except Exception as err:
            LOGGER.error(f"Unhandled exception when indexing: {err}")

        return dataset, err

    @staticmethod
    def generate_measurements(bucket_name: str) -> Dict:
        """ Generates a measurement dict for eo3 document """
        res = {}
        for measurement in S2_MEASUREMENTS[bucket_name]:
            band, resolution = measurement.split("_")
            if bucket_name == L2A_BUCKET:
                file_name = f"R{resolution}/{band}"
            else:
                measurement = file_name = band
            res[measurement] = {"path": f"{file_name}.jp2"}
            if resolution == "10m":
                grid = "default"
            else:
                grid = resolution
            res[measurement]["grid"] = grid
        return res


if __name__ == "__main__":
    LOGGER.info("Starting S2 indexer")
    S2Indexer().index()