import os
from queue import Queue
from pathlib import Path
from queue import Empty
from typing import Dict, Union
from types import SimpleNamespace

import boto3
from boto3.session import Session
from xml.etree import ElementTree
from hashlib import md5

import datacube
from datacube.model import Dataset as ODCDataset
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

from ...utils.logger import create_logger

LOGGER = create_logger("s2-index")
GUARDIAN = "GUARDIAN_QUEUE_EMPTY"

L1C_BUCKET = "sentinel-s2-l1c"
L2A_BUCKET = "sentinel-s2-l2a"
INDEX_BUCKETS = [L1C_BUCKET, L2A_BUCKET]

L1C_MEASUREMENTS = [
    "B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m",
    "B06_20m", "B07_20m", "B08_10m", "B09_60m", "B8A_20m",
    "B10_60m", "B11_20m", "B12_20m"
]
L2A_MEASUREMENTS = [
    "B02_20m", "B02_60m", "B03_20m", "B03_60m", "B04_20m",
    "B04_60m", "B05_60m", "B06_60m", "B07_60m", "B08_20m",
    "B08_60m", "B8A_60m", "B11_60m", "B12_60m", "SCL_20m"
] + L1C_MEASUREMENTS
L2A_MEASUREMENTS.remove("B10_60m")

MEASUREMENTS = {
    L1C_BUCKET: L1C_MEASUREMENTS,
    L2A_BUCKET: L2A_MEASUREMENTS,
}
PRODUCT_NAMES = {
    L1C_BUCKET: "s2a_level1c_granule",
    L2A_BUCKET: "s2a_sen2cor_granule",
}


def get_s3_uri(bucket_name: str, key: str) -> str:
    """ Gets a URI based on S3 bucket name and key """
    key_path = Path(key)
    uri = "s3://" + str(Path(bucket_name / key_path.parent))
    return uri


def absolutify_s3_paths(doc: Dict, uri: str) -> Dict:
    measurements = doc["measurements"]
    for measurement in measurements:
        measurement_key = uri + "/" + measurements[measurement]["path"]
        measurements[measurement]["path"] = measurement_key
    return doc


def add_dataset(doc: Dict,
                uri: str,
                index: datacube.index.index.Index,
                **kwargs) -> (ODCDataset, Union[Exception, None]):
    """ Adds dataset to ODC index """
    LOGGER.info("Indexing %s", uri)
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
        LOGGER.error(f"Unhandled exception {err}")

    return dataset, err


def generate_eo3_dataset_doc(bucket_name: str, uri: str, data: ElementTree) -> dict:
    """ Ref: https://datacube-core.readthedocs.io/en/latest/ops/dataset_documents.html """
    tile_metadata = read_tile_metadata(data)
    grids = read_grid_metadata(data)

    eo3 = {
        "id": md5(uri.encode("utf-8")).hexdigest(),
        "$schema": "https://schemas.opendatacube.org/dataset",
        "product": {
            "name": PRODUCT_NAMES[bucket_name],
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
        "measurements": generate_measurements(bucket_name),
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

    return absolutify_s3_paths(eo3, uri)


def generate_measurements(bucket_name: str) -> Dict:
    """ Generates a measurement dict for eo3 document """
    res = {}
    for measurement in MEASUREMENTS[bucket_name]:
        band, resolution = measurement.split("_")
        if bucket_name == L2A_BUCKET:
            file_name = measurement
        elif bucket_name == L1C_BUCKET:
            measurement = file_name = band
        res[measurement] = {"path": f"{file_name}.jp2"}
        if resolution == "10m":
            grid = "default"
        else:
            grid = resolution
        res[measurement]["grid"] = grid
    return res


def read_tile_metadata(data: ElementTree) -> SimpleNamespace:
    """ Reads tile metadata from metadata.xml """
    return SimpleNamespace(
        tile_id=data.find("./*/TILE_ID").text,
        sensing_time=data.find("./*/SENSING_TIME").text,
        crs_code=data.find("./*/Tile_Geocoding/HORIZONTAL_CS_CODE").text.lower(),
        sun_zenith=float(data.find("./*/Tile_Angles/Mean_Sun_Angle/ZENITH_ANGLE").text),
        sun_azimuth=float(data.find("./*/Tile_Angles/Mean_Sun_Angle/AZIMUTH_ANGLE").text),
        cloudy_pixel_percentage=float(data.find("./*/Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE").text),
    )


def read_grid_metadata(data: ElementTree) -> Dict:
    """ Reads grid metadata from metadata.xml. Returns a dict of 10, 20, 60m grids """
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


def index_from_s3(session: Session, bucket_name: str, queue):
    """ Indexes S2 tiles from S3 bucket from a queue of keys """
    dc = datacube.Datacube(app="s2-indexer")
    index = dc.index
    s3 = session.resource("s3")

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break
            obj = s3.Object(bucket_name, key).get(
                ResponseCacheControl="no-cache", RequestPayer="requester")
            uri = get_s3_uri(bucket_name, key)
            data = ElementTree.fromstring(str(obj["Body"].read(), "utf-8"))
            dataset_doc = generate_eo3_dataset_doc(bucket_name, uri, data)
            add_dataset(dataset_doc, uri, index)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break


def main():
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='eu-central-1',
    )
    s3 = session.resource('s3')
    queue = Queue()
    for bucket_name in INDEX_BUCKETS:
        bucket = s3.Bucket(bucket_name)
        # TODO: index other areas
        for obj in bucket.objects.filter(Prefix='tiles/35/P/PM/2020/10/', RequestPayer='requester'):
            if obj.key.endswith('metadata.xml'):
                queue.put(obj.key)

        q_size = queue.qsize()
        LOGGER.info(f"Indexing {q_size} {bucket_name} tiles")
        queue.put(GUARDIAN)
        index_from_s3(session, bucket_name, queue)  # TODO: multithread if necessary
        LOGGER.info(f"Finished indexing {q_size} {bucket_name} tiles")


if __name__ == "__main__":
    main()
