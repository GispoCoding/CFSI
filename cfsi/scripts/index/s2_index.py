from queue import Queue
from pathlib import Path
from queue import Empty
from typing import Dict, List

from xml.etree import ElementTree
from hashlib import md5

import cfsi
from cfsi.scripts.index import ODCIndexer
from cfsi.utils.logger import create_logger

from cfsi.constants import (GUARDIAN, L2A_BUCKET, S2_MEASUREMENTS, S2_PRODUCT_NAMES)

config = cfsi.config()
LOGGER = create_logger("s2_index")


class S2Indexer(ODCIndexer):
    """ Index Sentinel 2 scenes from AWS S3 """

    def __init__(self, name: str = "S2Indexer"):
        super().__init__(name)

    def add_to_index(self):
        """ Index S2 datasets from S3 to ODC """
        LOGGER.info("Indexing Sentinel 2 datasets from S3")
        for bucket_name in config.index.s2_index.s3_buckets:
            LOGGER.info(f"Indexing bucket {bucket_name}")
            self.__index_s3_bucket(bucket_name)
            LOGGER.info(f"Bucket {bucket_name} indexed")

    def __index_s3_bucket(self, bucket_name: str):
        """ Indexes the contents of a single S3 bucket to ODC """
        LOGGER.info("Generating indexing queue from config")
        queue = self.__generate_s3_indexing_queue(bucket_name)
        q_size = queue.qsize()

        LOGGER.info(f"Indexing {q_size} {bucket_name} tiles")
        self.__index_from_s3(bucket_name, queue)  # TODO: multithread if necessary
        LOGGER.info(f"Finished indexing {q_size} {bucket_name} tiles")

    def __generate_s3_indexing_queue(self, bucket_name: str) -> Queue:
        """ Generates and returns a queue of S3 keys to index """
        s3 = self.session.resource('s3')
        bucket = s3.Bucket(bucket_name)
        queue = Queue()
        prefixes = self.__generate_s3_prefixes()
        for prefix in prefixes:
            LOGGER.info(f"Fetching metadata for s3://{bucket_name}/{prefix}/*")
            for obj in bucket.objects.filter(Prefix=prefix, RequestPayer="requester"):
                if obj.key.endswith("metadata.xml"):
                    queue.put(obj.key)

        queue.put(GUARDIAN)
        return queue

    @staticmethod
    def __generate_s3_prefixes() -> List[str]:
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

    def __index_from_s3(self, bucket_name: str, queue):
        """ Indexes S2 tiles from S3 bucket from a queue of keys """
        while True:
            try:
                key = queue.get(timeout=60)
                if key == GUARDIAN:
                    break
                obj = self.get_object_from_s3(bucket_name, key, RequestPayer="requester")
                data = self.s3obj_to_etree(obj)
                uri = self.generate_s3_uri(bucket_name, key)
                id_: str = md5(uri.encode("utf-8")).hexdigest()

                if self.dataset_id_exists(id_):
                    LOGGER.info(f"Dataset {key} with id {id_} already indexed, skipping")
                    queue.task_done()
                else:
                    dataset_doc = self.__generate_eo3_dataset_doc(bucket_name, uri, data)
                    self.add_dataset(dataset_doc, uri=uri)
                    queue.task_done()
            except Empty:
                break
            except EOFError:
                break

    def __generate_eo3_dataset_doc(self, bucket_name: str, uri: str, data: ElementTree) -> dict:
        """ Generates an eo3 metadata document for ODC indexing """
        tile_metadata = self.read_s2_tile_metadata(data)
        grids = self.read_s2_grid_metadata(data)

        eo3 = {
            "id": md5(str(uri).encode("utf-8")).hexdigest(),
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
            "measurements": self.__generate_s2_measurements(bucket_name),
            "location": uri,
            "properties": {
                "tile_id": tile_metadata.tile_id,
                "eo:instrument": "MSI",
                "eo:platform": "SENTINEL-2",
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

    @staticmethod
    def __generate_s2_measurements(bucket_name: str) -> Dict:
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
    S2Indexer().add_to_index()
