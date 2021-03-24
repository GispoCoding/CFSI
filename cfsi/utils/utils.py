import os
from typing import List
from pathlib import Path

from datacube.model import Dataset as ODCDataset
import rasterio as rio
from rasterio.crs import CRS
from rasterio.transform import Affine

L1C_BUCKET = "sentinel-s2-l1c"
L2A_BUCKET = "sentinel-s2-l2a"


def get_s2_tile_ids(dataset: ODCDataset) -> (str, str):
    """ Returns tile_id and s3_key from dataset metadata doc """
    tile_props = dataset.metadata_doc["properties"]
    tile_id = tile_props["tile_id"]
    s3_key = tile_props["s3_key"]
    return tile_id, s3_key


def check_existing_mask_directory(dataset: ODCDataset, mask_product_name: str) -> bool:
    """ Checks if a cloud mask directory for given dataset already exists """
    # TODO: check if mask exists in index
    mask_output_directory = generate_s2_tif_path(dataset, mask_product_name).parent
    if mask_output_directory.exists():
        return True
    return False


def generate_s2_tif_path(dataset: ODCDataset,
                         product_name: str = "",
                         band_name: str = "") -> Path:
    """ Generates a output path for writing a ODCDataset to a .tif file.
     :param dataset: ODCDataset being written
     :param product_name: product name being written. each product goes to its own sub-directory, optional
     :param band_name: name of band being written. band name is appended to filename, optional """
    base_output_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"])  # TODO: write to S3
    tile_id, s3_key = get_s2_tile_ids(dataset)
    if band_name:
        tile_id += f"_{band_name}"
    tile_id += ".tif"
    output_dir = Path(base_output_path / s3_key).joinpath(product_name, tile_id)
    return output_dir


def read_transform_from_file(file_path: Path) -> (Affine, CRS):
    """ Reads and returns transformation and CRS from file """
    with rio.open(file_path) as f:
        transform = f.transform
        crs = f.crs
    return transform, crs


def container_path_to_global_path(*file_paths: Path) -> List[Path]:
    """ Translates container paths to global paths based on
    CFSI_OUTPUT_CONTAINER and CFSI_OUTPUT_HOST env variables.
    e.g. /output/tiles/... -> /home/ubuntu/cfsi_output/tiles/... """
    res: List[Path] = []
    container_output_path = os.environ["CFSI_OUTPUT_CONTAINER"]
    external_output_path = os.environ["CFSI_OUTPUT_HOST"]
    for file_path in file_paths:
        file_string = str(file_path)
        protocol = ""
        if file_string.startswith("file://"):  # TODO: more protocols
            protocol = "file://"
            file_string = file_string[len(protocol):]
        if file_string.startswith(container_output_path):
            res.append(Path(protocol + file_string.replace(
                container_output_path, external_output_path)))
        else:
            res.append(file_path)

    return res


def swap_s2_bucket_names(uri: str) -> str:
    """ Swaps L1C <-> L2A bucket names in given uri string """
    if L1C_BUCKET in uri:
        return uri.replace(L1C_BUCKET, L2A_BUCKET)
    elif L2A_BUCKET in uri:
        return uri.replace(L2A_BUCKET, L1C_BUCKET)
    raise ValueError  # TODO: add custom exception
