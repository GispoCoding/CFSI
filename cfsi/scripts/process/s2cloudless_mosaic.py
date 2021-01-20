import os
from pathlib import Path
from typing import List, Dict

import numpy as np
from osgeo import gdal
import datacube
from datacube.model import Dataset as ODCDataset
import datacube.storage._read  # TODO: Remove hack to avoid circular import ImportError

from cfsi import config
from cfsi.scripts.masks.s2cloudless_masks import process_dataset
from cfsi.utils.logger import create_logger
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer
from cfsi.scripts.mosaic.mosaic import mosaic_from_mask_datasets
from cfsi.utils.write_utils import (array_to_geotiff, generate_s2_file_output_path,
                                    odcdataset_to_tif, write_l1c_dataset_rgb,
                                    generate_mosaic_output_path, gdal_params_for_xadataset)


LOGGER = create_logger("s2cloudless_mosaic")
OUTPUT_PATH = Path(os.environ["CFSI_CONTAINER_OUTPUT"])  # TODO: write to S3
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
WRITE_RGB = False


def main():
    """ Create s2cloudless masks for indexed L1C datasets """
    LOGGER.info("Starting S2cloudless mosaic creator")
    l1c_datasets = get_l1c_datasets()

    max_iterations = config.masks.s2cloudless_masks.max_iterations
    if len(l1c_datasets) < max_iterations:
        max_iterations = len(l1c_datasets)

    i = 1
    indexed_masks: List[ODCDataset] = []
    for dataset in l1c_datasets:
        if check_existing_masks(dataset, "s2cloudless"):
            LOGGER.info(f"S2Cloudless masks for dataset {dataset} already exist")
            continue

        LOGGER.info(f"Processing {dataset}, iteration {i}/{max_iterations}")
        mask_arrays = process_dataset(dataset)

        LOGGER.info("Writing masks to file")
        output_masks = write_mask_arrays(dataset, mask_arrays)

        if config.masks.s2cloudless_masks.write_rgb:
            write_l1c_dataset_rgb(dataset)

        LOGGER.info(f"Finished writing {dataset}, indexing output")
        indexed_masks += S2CloudlessIndexer().index({dataset: output_masks})

        i += 1
        if i > max_iterations:
            LOGGER.warning(f"Reached maximum iterations count {max_iterations}")
            break

    if len(indexed_masks) == 0:
        LOGGER.warning("No new masks generated")
        return

    mosaic_from_s2cloudless_datasets(indexed_masks)
    exit(0)


def get_l1c_datasets() -> List[ODCDataset]:
    """ Gets all L1C datasets from ODC Index """
    dc = datacube.Datacube(app="s2cloudless_mosaic")
    l1c_datasets = dc.find_datasets(product="s2a_level1c_granule")
    return l1c_datasets


def check_existing_masks(dataset: ODCDataset, product_name: str) -> bool:
    """ Checks if a S2Cloudless mask for given dataset already exists """
    output_directory = generate_s2_file_output_path(dataset, product_name).parent
    if output_directory.exists():
        return True
    return False


def write_mask_arrays(dataset: ODCDataset,
                      mask_arrays: (np.ndarray, np.ndarray)) -> Dict[str, Path]:
    """ Writes cloud and shadow masks to files """
    masks = {"clouds": mask_arrays[0],
             "shadows": mask_arrays[1]}
    output_mask_files = odcdataset_to_tif(dataset, masks, "s2cloudless", gdal.GDT_Byte)
    output_masks = {
        "cloud_mask": output_mask_files[0],
        "shadow_mask": output_mask_files[1]}
    return output_masks


def mosaic_from_s2cloudless_datasets(indexed_masks: List[ODCDataset]):
    """ Creates a new mosaic from a list of S2Cloudless mask ODC Datasets """
    LOGGER.info(f"Creating mosaic dataset from {len(indexed_masks)} masks")
    mosaic_ds = mosaic_from_mask_datasets(indexed_masks)
    mosaic_filepath = generate_mosaic_output_path("s2cloudless")

    LOGGER.info("Constructing mosaic array")
    mosaic_data: List[np.ndarray] = [np.squeeze(mosaic_ds[band].values)
                                     for band in mosaic_ds.data_vars]
    geo_transform, projection = gdal_params_for_xadataset(mosaic_ds)

    LOGGER.info(f"Writing mosaic to {mosaic_filepath}")
    array_to_geotiff(mosaic_filepath, mosaic_data, geo_transform, projection)
    LOGGER.info(f"Generated mosaic {mosaic_filepath}")


if __name__ == "__main__":
    main()
