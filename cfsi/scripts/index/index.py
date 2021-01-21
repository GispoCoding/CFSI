import os
from pathlib import Path
from typing import Dict, Union
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import UUID
from xml.etree import ElementTree

from boto3 import Session
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.model import Dataset as ODCDataset
from datacube.utils import changes
from datacube.utils.changes import DocumentMismatchError

from cfsi.utils.logger import create_logger
from cfsi.utils.utils import swap_s2_bucket_names

LOGGER = create_logger("ODCIndexer")


class ODCIndexer:
    """ Index data to CFSI ODC - base class """

    def __init__(self, name: str = "ODCIndexer"):
        """ Sets up the indexer """
        self.dc: Datacube = Datacube(app=name)
        self.session: Session = Session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name='eu-central-1')

    def add_dataset(self, doc: Dict, uri: str = "", **kwargs) -> (ODCDataset, Union[Exception, None]):
        """ Adds dataset to dcIndex """
        if not uri:
            uri = doc["uri"]
        LOGGER.debug(f"Indexing {uri}")
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

    def dataset_exists(self, id_: str) -> bool:
        """ Check if dataset URI is already indexed """
        index = self.dc.index
        if not index.datasets.get(id_):
            return False
        return True

    @staticmethod
    def generate_s3_uri(bucket_name: str, key: str) -> str:
        """ Gets a URI based on S3 bucket name and key """
        key_path = Path(key)
        uri = "s3://" + str(Path(bucket_name / key_path.parent))
        return uri

    @staticmethod
    def relative_s3_keys_to_absolute(doc: Dict, uri: str) -> Dict:
        """ Convert S3 object paths in eo3 doc from relative to key values """
        measurements = doc["measurements"]
        for measurement in measurements:
            measurement_key = uri + "/" + measurements[measurement]["path"]
            measurements[measurement]["path"] = measurement_key
        return doc

    def get_object_from_s3_uri(self, uri: str, **kwargs) -> ElementTree:
        """ Fetches object from S3 URI """
        parsed_url = urlparse(uri)
        bucket_name = parsed_url.netloc
        key = parsed_url.path[1:]  # skip leading /
        return self.get_object_from_s3(bucket_name, key, **kwargs)

    def get_object_from_s3(self, bucket_name: str, key: str, **kwargs):
        """ Gets an object from S3 bucket with key """
        s3 = self.session.resource("s3")
        return s3.Object(bucket_name, key).get(**kwargs)

    @staticmethod
    def s3obj_to_etree(obj) -> ElementTree:
        """ Reads an S3 object to a ElementTree. Used for reading metadata.xml """
        return ElementTree.fromstring(str(obj["Body"].read(), "utf-8"))

    @staticmethod
    def read_s2_tile_metadata(data: ElementTree) -> SimpleNamespace:
        """ Reads necessary metadata from an metadata.xml ElementTree """
        return SimpleNamespace(
            tile_id=data.find("./*/TILE_ID").text,
            sensing_time=data.find("./*/SENSING_TIME").text,
            crs_code=data.find("./*/Tile_Geocoding/HORIZONTAL_CS_CODE").text.upper(),
            sun_zenith=float(data.find("./*/Tile_Angles/Mean_Sun_Angle/ZENITH_ANGLE").text),
            sun_azimuth=float(data.find("./*/Tile_Angles/Mean_Sun_Angle/AZIMUTH_ANGLE").text),
            cloudy_pixel_percentage=float(data.find("./*/Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE").text),
        )

    @staticmethod
    def read_s2_grid_metadata(data: ElementTree) -> Dict:
        """ Reads grid metadata from metadata.xml ElementTree. Returns a dict of 10, 20, 60m grids """
        grids = {
            "10": {},
            "20": {},
            "60": {},
        }
        for resolution in grids:
            grids[resolution]["nrows"] = int(data.findall(
                f"./*/Tile_Geocoding/Size[@resolution='{resolution}']/NROWS")[0].text)
            grids[resolution]["ncols"] = int(data.findall(
                f"./*/Tile_Geocoding/Size[@resolution='{resolution}']/NCOLS")[0].text)
            grids[resolution]["ulx"] = float(data.findall(
                f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULX")[0].text)
            grids[resolution]["uly"] = float(data.findall(
                f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/ULY")[0].text)
            grids[resolution]["xdim"] = float(data.findall(
                f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/XDIM")[0].text)
            grids[resolution]["ydim"] = float(data.findall(
                f"./*/Tile_Geocoding/Geoposition[@resolution='{resolution}']/YDIM")[0].text)
            grids[resolution]["trans"] = [grids[resolution]["xdim"], 0.0, grids[resolution]["ulx"], 0.0,
                                          grids[resolution]["ydim"], grids[resolution]["uly"],
                                          0.0, 0.0, 1.0]
        return grids

    def l2a_dataset_from_l1c(self, l1c_dataset: ODCDataset):
        """ Gets the S2 L2A dataset ODCDataset that corresponds to l1c_dataset """
        l1c_uri = l1c_dataset.uris[0]
        l2a_uri = swap_s2_bucket_names(l1c_uri)
        l2a_dataset_id = self.odcdataset_id_from_uri(l2a_uri, "s2a_sen2cor_granule")
        l2a_odcdataset: ODCDataset = self.dc.index.datasets.get(l2a_dataset_id)
        return l2a_odcdataset

    def odcdataset_id_from_uri(self, uri: str, product: str = None) -> UUID:
        """ Returns the id of a ODCDataset that matches the given URI """
        query = dict(product=product, uri=uri, limit=1)
        try:
            dataset: ODCDataset = [odc_ds for odc_ds in self.dc.index.datasets.search(**query)][0]
        except IndexError:
            LOGGER.warning(f"Couldn't find ODC Dataset for product {product} matching URI {uri}")
            raise  # TODO: custom exception
        return dataset.id
