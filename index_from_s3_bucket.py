# coding: utf-8
import logging
import re
import uuid
from multiprocessing import Process, current_process, Manager, cpu_count
from pathlib import Path
from queue import Empty

import boto3
import click
from osgeo import osr
from ruamel.yaml import YAML
from xml.etree import ElementTree
import xmltodict
from hashlib import md5

import datacube
from datacube.index.hl import Doc2Dataset, load_rules_from_types
from datacube.utils import changes

# Need to check if we're on new gdal for coordinate order
import osgeo.gdal
from packaging import version

LON_LAT_ORDER = version.parse(osgeo.gdal.__version__) < version.parse("3.0.0")

GUARDIAN = "GUARDIAN_QUEUE_EMPTY"
AWS_PDS_TXT_SUFFIX = "MTL.txt"

MTL_PAIRS_RE = re.compile(r'(\w+)\s=\s(.*)')


def _parse_value(s):
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def _parse_group(lines):
    tree = {}
    for line in lines:
        match = MTL_PAIRS_RE.findall(line)
        if match:
            key, value = match[0]
            if key == 'GROUP':
                tree[value] = _parse_group(lines)
            elif key == 'END_GROUP':
                break
            else:
                tree[key] = _parse_value(value)
    return tree


def get_geo_ref_points(info):
    return {
        'ul': {'x': info['CORNER_UL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UL_PROJECTION_Y_PRODUCT']},
        'ur': {'x': info['CORNER_UR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UR_PROJECTION_Y_PRODUCT']},
        'll': {'x': info['CORNER_LL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LL_PROJECTION_Y_PRODUCT']},
        'lr': {'x': info['CORNER_LR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LR_PROJECTION_Y_PRODUCT']},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        if LON_LAT_ORDER:
            # GDAL 2.0 order
            lon, lat, z = t.TransformPoint(p['x'], p['y'])
        else:
            # GDAL 3.0 order
            lat, lon, z = t.TransformPoint(p['x'], p['y'])
            
        return {'lon': lon, 'lat': lat}
        
    return {key: transform(p) for key, p in geo_ref_points.items()}


def format_obj_key(obj_key):
    obj_key = '/'.join(obj_key.split("/")[:-1])
    return obj_key


def get_s3_url(bucket_name, obj_key):
    return 'http://{bucket_name}.s3.amazonaws.com/{obj_key}'.format(
        bucket_name=bucket_name, obj_key=obj_key)


def absolutify_paths(doc, bucket_name, obj_key):
    objt_key = format_obj_key(obj_key)
    measurements = doc["measurements"]
    for measurement in measurements:
        measurements[measurement]["path"] = get_s3_url(bucket_name, objt_key + '/' + measurements[measurement]["path"])
    return doc


def archive_document(doc, uri, index, sources_policy):
    def get_ids(dataset):
        ds = index.datasets.get(dataset.id, include_sources=True)
        for source in ds.sources.values():
            yield source.id
        yield dataset.id

    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)
    index.datasets.archive(get_ids(dataset))
    logging.info("Archiving %s and all sources of %s", dataset.id, dataset.id)


def add_dataset(doc, uri, index: datacube.index.index.Index, **kwargs):
    logging.info("Indexing %s", uri)
    resolver = Doc2Dataset(index, **kwargs)
    dataset, err = resolver(doc, uri)
    if err is not None:
        logging.error("%s", err)
        return dataset, err
    try:
        index.datasets.add(dataset)  # Source policy to be checked in sentinel 2 datase types
    except changes.DocumentMismatchError:
        index.datasets.update(dataset, {tuple(): changes.allow_any})
    except Exception as e:
        err = e
        logging.error("Unhandled exception %s", e)

    return dataset, err


def generate_eo3_dataset_doc(key: str, data: ElementTree) -> dict:
    """ Ref: https://datacube-core.readthedocs.io/en/latest/ops/dataset_documents.html """
    keypath = Path(key)
    keyparts = list(keypath.parts)
    regioncode = "".join(keyparts[1:4])

    sensing_time = data.findall("./*/SENSING_TIME")[0].text
    crs_code = data.findall("./*/Tile_Geocoding/HORIZONTAL_CS_CODE")[0].text.lower()

    nrows_10 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='10']/NROWS")[0].text)
    ncols_10 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='10']/NCOLS")[0].text)

    ulx_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/ULX")[0].text)
    uly_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/ULY")[0].text)

    xdim_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/XDIM")[0].text)
    ydim_10 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='10']/YDIM")[0].text)

    trans_10 = [xdim_10, 0.0, ulx_10, 0.0, ydim_10, uly_10, 0.0, 0.0, 1.0]

    nrows_20 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='20']/NROWS")[0].text)
    ncols_20 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='20']/NCOLS")[0].text)

    ulx_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/ULX")[0].text)
    uly_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/ULY")[0].text)

    xdim_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/XDIM")[0].text)
    ydim_20 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='20']/YDIM")[0].text)

    trans_20 = [xdim_20, 0.0, ulx_20, 0.0, ydim_20, uly_20, 0.0, 0.0, 1.0]

    nrows_60 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='60']/NROWS")[0].text)
    ncols_60 = int(data.findall("./*/Tile_Geocoding/Size[@resolution='60']/NCOLS")[0].text)

    ulx_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/ULX")[0].text)
    uly_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/ULY")[0].text)

    xdim_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/XDIM")[0].text)
    ydim_60 = float(data.findall("./*/Tile_Geocoding/Geoposition[@resolution='60']/YDIM")[0].text)

    trans_60 = [xdim_60, 0.0, ulx_60, 0.0, ydim_60, uly_60, 0.0, 0.0, 1.0]

    ten_list = ['B02_10m', 'B03_10m', 'B04_10m', 'B08_10m']
    twenty_list = ['B05_20m', 'B06_20m', 'B07_20m', 'B11_20m', 'B12_20m', 'B8A_20m',
                   'B02_20m', 'B03_20m', 'B04_20m']
    sixty_list = ['B01_60m', 'B02_60m', 'B03_60m', 'B04_60m', 'B8A_60m', 'B09_60m',
                  'B05_60m', 'B06_60m', 'B07_60m', 'B11_60m', 'B12_60m']

    eo3 = {
        "id": md5(key.encode("utf-8")).hexdigest(),
        "$schema": "https://schemas.opendatacube.org/dataset",
        "product": {
            "name": "s2a_sen2cor_granule",
        },
        "crs": crs_code,
        "grids": {
            "default": {
                "shape": [nrows_10, ncols_10],
                "transform": trans_10,
            },
            "20m": {
                "shape": [nrows_20, ncols_20],
                "transform": trans_20,
            },
            "60m": {
                "shape": [nrows_60, ncols_60],
                "transform": trans_60,
            },
        },
        "measurements": {},
        "location": f"http://sentinel-s2-l2a.s3.amazonaws.com/{keypath.parent}",
        "properties": {
            "eo:instrument": "MSI",
            "eo:platform": "SENTINEL-2A",
            "datetime": sensing_time,
            "odc:file_format": "JPEG2000",  # TODO: check validity
            "odc:region_code": regioncode,
        },
        "lineage": {},
    }
    for measurement in ten_list:
        band, res = measurement.split("_")
        eo3["measurements"][measurement] = {"path": f"R{res}/{band}.jp2"}

    for measurement in twenty_list:
        band, res = measurement.split("_")
        eo3["measurements"][measurement] = {
            "path": f"R{res}/{band}.jp2",
            "grid": "20m",
        }

    for measurement in sixty_list:
        band, res = measurement.split("_")
        eo3["measurements"][measurement] = {
            "path": f"R{res}/{band}.jp2",
            "grid": "60m",
        }

    eo3["measurements"]["SCL_20m"] = {
        "path": "R20m/SCL.jp2",
        "grid": "20m",
    }

    return absolutify_paths(eo3, "sentinel-s2-l2a", key)


def worker(config, bucket_name, func, unsafe, sources_policy, queue):
    dc = datacube.Datacube(config=config)
    index = dc.index
    s3 = boto3.resource("s3")
    safety = 'safe' if not unsafe else 'unsafe'

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break
            logging.info("Processing %s %s", key, current_process())
            obj = s3.Object(bucket_name, key).get(ResponseCacheControl="no-cache", RequestPayer="requester")
            raw = obj["Body"].read()
            content = str(raw, 'utf-8')
            data = ElementTree.fromstring(content)
            dataset_doc = generate_eo3_dataset_doc(key, data)
            uri = get_s3_url(bucket_name, key)
            logging.info("calling %s", func)
            func(dataset_doc, uri, index)
            queue.task_done()
        except Empty:
            break
        except EOFError:
            break


def iterate_datasets(bucket_name, config, func, unsafe, sources_policy):
    manager = Manager()
    queue = manager.Queue()

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    worker_count = 2
    # worker_count = cpu_count() * 2

    processes = []
    for i in range(worker_count):
        proc = Process(target=worker, args=(config, bucket_name, func, unsafe, sources_policy, queue))
        processes.append(proc)
        proc.start()

    for obj in bucket.objects.filter(Prefix='tiles/35/P/PM/2020/10/', RequestPayer='requester'):
        if obj.key.endswith('metadata.xml'):
            queue.put(obj.key)

    for i in range(worker_count):
        queue.put(GUARDIAN)

    for proc in processes:
        proc.join()
    logging.info("Processing done")


@click.command(help="Enter Bucket name. Optional to enter configuration file to access a different database")
@click.argument('bucket_name')
@click.option('--config', '-c', help=" Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.option('--archive', is_flag=True,
              help="If true, datasets found in the specified bucket and prefix will be archived")
@click.option('--unsafe', is_flag=True,
              help="If true, YAML will be parsed unsafely. Only use on trusted datasets. Only valid if suffix is yaml")
@click.option('--sources_policy', default="verify", help="verify, ensure, skip")
def main(bucket_name, config, archive, unsafe, sources_policy):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    action = archive_document if archive else add_dataset
    iterate_datasets(bucket_name, config, action, unsafe, sources_policy)


if __name__ == "__main__":
    main()
