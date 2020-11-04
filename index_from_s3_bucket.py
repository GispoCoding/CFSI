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

import datacube
from datacube.index.hl import Doc2Dataset
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
    for measurement in doc["measurements"]:
        measurement["path"] = get_s3_url(bucket_name, objt_key + '/' + measurement["path"])
    # for band in doc['image']['bands'].values():
    #     band['path'] = get_s3_url(bucket_name, objt_key + '/' + band['path'])
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


def add_dataset(doc, uri, index, **kwargs):
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

    size_10 = int(data.findall("./*/Tile_Geocoding/Size")[0].text)  # TODO: check resolution == 10
    geopos_10 = data.findall("./*/Tile_Geocoding/Geoposition")[0]  # TODO: check resolution == 10
    ulx_10 = int(geopos_10[0].text)
    uly_10 = int(geopos_10[1].text)
    xdim_10 = int(geopos_10[2].text)
    ydim_10 = int(geopos_10[3].text)

    # level = 'L2A'
    # product_type = data.findall('./*/Product_Info/PRODUCT_TYPE')[0].text
    # ct_time = data.findall('./*/Archiving_Info/ARCHIVING_TIME')[0].text
    # station = data.findall('./*/Archiving_Info/ARCHIVING_CENTRE')[0].text

    eo3 = {
        "id": str(uuid.uuid4()),
        "$schema": "https://schemas.opendatacube.org/dataset",
        # "processing_level": level,
        # "product_type": product_type,
        # "creation_dt": ct_time,
        "product": {
            "name": "s2a_sen2cor_granule",
        },
        # "platform": {"code": "SENTINEL_2A"},
        # "instrument": {"name": "MSI"},
        # "acquisition": {"groundstation": {"code": station}},
        "crs": crs_code,
        # Optional GeoJSON object
        # "geometry": {
        #     "type": "polygon",
        #     "coordinates": [[]],  # TODO: read from data dict
        # },
        "grids": {
            "default": {
                # shape is basically height, width tuple and transform captures a linear mapping
                # from pixel space to projected space encoded in a row - major order:
                # transform [a0, a1, a2, a3, a4, a5, 0, 0, 1]
                # [X][a0, a1, a2][Pixel]
                # [Y] = [a3, a4, a5][Line]
                # [1][0, 0, 1][1]
                "shape": [7811, 7691],  # TODO: read from data dict
                "transform": [30, 0, 618285, 0, -30, -1642485, 0, 0, 1],  # TODO: read from data dict
            }
        },
        "measurements": {
            "B01_60m": {
                "path": "R60m/B01.jp2",
            },
            "B02_10m": {
                "path": "R10m/B02.jp2",
            },
        },
        "location": f"http://sentinel-s2-l2a.s3.amazonaws.com/{keypath.parent}",
        "properties": {
            "eo:platform": "SENTINEL_2A",
            "datetime": sensing_time,
            "odc:file_format": "JPEG2000",  # TODO: check validity
            "odc:region_code": regioncode,
            "dea:dataset_maturity": "final",
            "odc:product_family": "ard",
        },
        "lineage": {"source_datasets": {}},
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
            product_def = Path('.' / 's2_granules.yaml')
            logging.info("calling %s", func)
            func(dataset_doc, uri, index, sources_policy)
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
    # logging.info("Bucket : %s prefix: %s ", bucket_name, str(prefix))
    # safety = 'safe' if not unsafe else 'unsafe'
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
