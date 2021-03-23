import os
from logging import DEBUG
from pathlib import Path
from typing import List, Optional

from datacube import Datacube
from datacube.model import Dataset as ODCDataset
from sentinelhub import AwsTile, AwsTileRequest, DataCollection

import cfsi
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import check_existing_mask_directory, write_l1c_dataset

config = cfsi.config()
LOGGER = create_logger("fmask", level=DEBUG)


class CloudMaskGenerator:
    l1c_product_name = "s2_level1c_granule"

    def __init__(self):
        self.i = 1
        self.indexed_masks: List[ODCDataset] = []
        self.mask_product_name: Optional[str] = None
        self.max_iterations: Optional[int] = None

    def create_masks(self):
        l1c_datasets = self.get_l1c_datasets()
        if len(l1c_datasets) < self.max_iterations or not self.max_iterations:
            self.max_iterations = len(l1c_datasets)

        for l1c_dataset in l1c_datasets:
            should_continue = self._create_mask(l1c_dataset)
            if not should_continue:
                break

        if len(self.indexed_masks) == 0:
            LOGGER.warning("No new masks generated")

        return self.indexed_masks

    def get_l1c_datasets(self) -> List[ODCDataset]:
        """ Gets all L1C datasets from ODC Index """
        dc = Datacube(app="cloud_mask_generator")
        l1c_datasets = dc.find_datasets(product=self.l1c_product_name)
        return l1c_datasets

    @staticmethod
    def fetch_s2_to_safe(tile_id: str) -> Path:
        """ Fetches S2 granule by tile ID from AWS S3 to .SAFE format.
        Does not overwrite or re-download existing data.
        :param tile_id: S2 granule tile ID
        :returns Path of fetched data """
        tile_name, time, aws_index = AwsTile.tile_id_to_tile(tile_id)
        base_output_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"]).joinpath("cache/safe")
        request = AwsTileRequest(tile=tile_name,
                                 time=time,
                                 aws_index=aws_index,
                                 data_folder=base_output_path,
                                 data_collection=DataCollection.SENTINEL2_L1C,
                                 safe_format=True)
        LOGGER.info(f"Fetching data to .SAFE format for granule {tile_id}")
        request.save_data()
        tile_output_directory = Path(request.get_filename_list()[0]).parts[0]
        return base_output_path.joinpath(tile_output_directory)

    @staticmethod
    def _create_mask(l1c_dataset: ODCDataset) -> bool:
        """ Overridden in subclasses """
        pass

    def _should_process(self, dataset: ODCDataset) -> bool:
        """ Checks if masks should be generated for given ODCDataset """
        if check_existing_mask_directory(dataset, self.mask_product_name):
            LOGGER.info(f"{self.mask_product_name} files for dataset {dataset} already exist, skipping")
            return False
        metadata_cloud_percentage, in_threshold = self._check_clouds_in_threshold(dataset)
        if not in_threshold:
            return False
        return True

    @staticmethod
    def _check_clouds_in_threshold(dataset: ODCDataset) -> (float, bool):
        """ Checks if metadata cloud percentage of given dataset is within threshold """
        tile_metadata = dataset.metadata_doc["properties"]
        cloud_percent = tile_metadata["cloudy_pixel_percentage"]
        max_cloud = config.masks.max_cloud_threshold
        min_cloud = config.masks.min_cloud_threshold
        in_threshold = True

        if cloud_percent > max_cloud:
            LOGGER.info("Metadata cloud percentage greater than max threshold value: " +
                        f"{max_cloud} < {cloud_percent}")
            in_threshold = False

        elif cloud_percent < min_cloud:
            LOGGER.info("Metadata cloud percentage lower than min threshold value: " +
                        f"{min_cloud} > {cloud_percent}")
            in_threshold = False

        return cloud_percent, in_threshold

    def _continue_iteration(self) -> bool:
        """ Checks whether to continue mask generation iteration, increases iteration count by 1 """
        if self.i > self.max_iterations:
            LOGGER.warning(f"Reached maximum iterations count {self.max_iterations}")
            return False

        self.i += 1
        return True

    @staticmethod
    def write_l1c_reference(dataset: ODCDataset):
        if config.masks.write_rgb:
            write_l1c_dataset(dataset)
        if config.masks.write_l1c:
            write_l1c_dataset(dataset, rgb=False)
