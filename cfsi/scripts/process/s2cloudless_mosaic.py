from pathlib import Path
from typing import List, Dict

import numpy as np
from osgeo import gdal
import datacube
from datacube.model import Dataset as ODCDataset
import datacube.storage._read  # TODO: Remove hack to avoid circular import ImportError

from cfsi import config
from cfsi.scripts.index.mosaic_index import MosaicIndexer
from cfsi.scripts.masks.s2cloudless_masks import process_dataset
from cfsi.utils.logger import create_logger
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer
from cfsi.scripts.mosaic.mosaic import MosaicCreator
from cfsi.utils.write_utils import (generate_s2_file_output_path, odcdataset_to_tif,
                                    write_l1c_dataset_rgb)


LOGGER = create_logger("s2cloudless_mosaic")
WRITE_RGB = False


def main():
    """ Create s2cloudless masks for indexed L1C datasets """
    LOGGER.info("Starting S2cloudless mosaic creator")
    l1c_datasets = get_l1c_datasets()

    max_iterations = config.masks.s2cloudless_masks.max_iterations
    if len(l1c_datasets) < max_iterations:
        max_iterations = len(l1c_datasets)

    # TODO: read products to generate masks for from config
    i = 1
    indexed_masks: List[ODCDataset] = []
    for dataset in l1c_datasets:
        if check_existing_masks(dataset, "s2a_level1c_s2cloudless"):
            LOGGER.info(f"S2Cloudless masks for dataset {dataset} already exist")
            continue

        if max_iterations:
            LOGGER.info(f"Iteration {i}/{max_iterations}: {dataset}")
        mask_arrays = process_dataset(dataset)

        LOGGER.info("Writing masks to file")
        output_masks = write_mask_arrays(dataset, mask_arrays)

        if config.masks.s2cloudless_masks.write_rgb:
            write_l1c_dataset_rgb(dataset)

        LOGGER.info(f"Finished writing {dataset}, indexing output")
        indexed_masks += S2CloudlessIndexer().index({dataset: output_masks})

        if max_iterations and i > max_iterations:
            LOGGER.warning(f"Reached maximum iterations count {max_iterations}")
            break
        i += 1

    if len(indexed_masks) == 0:
        LOGGER.warning("No new masks generated")

    # TODO: read product names from config
    dates = config.mosaic.dates
    days = config.mosaic.range
    products = config.mosaic.products
    for product in products:
        for date_ in dates:
            mosaic_creator = MosaicCreator(product, date_, days)
            mosaic_ds = mosaic_creator.create_mosaic_dataset()
            output_mosaic_path = mosaic_creator.write_mosaic_to_file(mosaic_ds)
            LOGGER.info("Indexing output mosaic")
            MosaicIndexer().index(mosaic_ds, output_mosaic_path)

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


def get_mask_datasets() -> List[ODCDataset]:
    """ Gets all S2Cloudless datasets from ODC Index """
    dc = datacube.Datacube(app="s2cloudless_mosaic")
    s2cloudless_datasets = dc.find_datasets(product="s2a_level1c_s2cloudless")
    return s2cloudless_datasets


if __name__ == "__main__":
    main()
