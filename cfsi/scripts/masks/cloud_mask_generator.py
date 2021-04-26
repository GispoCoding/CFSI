import glob
import os
from logging import DEBUG
from pathlib import Path
from typing import List, Optional

from datacube import Datacube
from datacube.model import Dataset as ODCDataset
import numpy as np
import rasterio as rio
from rasterio.warp import reproject, Resampling
from sentinelhub import AwsTile, AwsTileRequest, DataCollection

import cfsi
from cfsi.utils import check_existing_mask_directory, get_s2_tile_ids
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import write_l1c_dataset

config = cfsi.config()
LOGGER = create_logger("cloud_mask_generator", level=DEBUG)


class CloudMaskGenerator:
    l1c_product_name = "s2_level1c_granule"

    def __init__(self):
        self.i = 1
        self.indexed_masks: List[ODCDataset] = []
        self.mask_product_name: Optional[str] = None
        self.max_iterations: Optional[int] = None
        self.total_iterations: Optional[int] = None

    def create_masks(self):
        l1c_datasets = self.get_l1c_datasets()
        if len(l1c_datasets) < self.max_iterations or not self.max_iterations:
            self.max_iterations = len(l1c_datasets)
        self.total_iterations = self.max_iterations

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

    def fetch_s2_jp2_files(self, dataset: ODCDataset) -> List[Path]:
        """ Fetches S2 data from S3 in .SAFE format, returns list of fetched JP2 files """
        tile_id, s3_key = get_s2_tile_ids(dataset)
        image_path = self.fetch_s2_to_safe(tile_id).joinpath("IMG_DATA")
        jp2_files = glob.glob(str(image_path) + "/*B??.jp2")
        jp2_files.sort()  # B1, B2, B3, ... B11, B12, B8A
        jp2_files.insert(8, jp2_files[-1])  # ... B7, B8, B8A, B9, ...
        jp2_files = jp2_files[:-1]
        if len(jp2_files) != 13:
            LOGGER.warning(f"Unexpected number of images in {image_path}: {len(jp2_files)}")
        jp2_files = [Path(jp2_file) for jp2_file in jp2_files]
        return jp2_files

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
        LOGGER.info(f"Fetching .SAFE for {tile_id}")
        request.save_data()
        tile_output_directory = Path(request.get_filename_list()[0]).parts[0]
        return base_output_path.joinpath(tile_output_directory)

    @staticmethod
    def array_from_jp2_files(jp2_files: List[Path]) -> np.ndarray:
        """ Constructs a np.ndarray from a list of JP2 files """
        LOGGER.info("Reading and reprojecting data to arrays from JP2 files")
        arrays = []

        with rio.open(jp2_files[1], nodata=0) as f:  # Read 10m transform from B02
            data = f.read()
            dest_transform = f.transform
            dest_shape = data.shape
            dest_datatype = data.dtype

        for jp2_file in jp2_files:
            with rio.open(jp2_file) as f:
                data = f.read()
                dest_array = np.zeros(dest_shape, dest_datatype)
                reproject(data, destination=dest_array,
                          src_transform=f.transform, dst_transform=dest_transform,
                          src_crs=f.crs, dst_crs=f.crs,
                          src_nodata=0, dst_nodata=0,
                          resampling=Resampling.nearest)
                arrays.append(dest_array)

        LOGGER.info("Constructing final array")
        final_array = np.array(arrays, dtype="float64") / 10000
        final_array = np.moveaxis(final_array, 0, -1)
        return final_array

    @staticmethod
    def _create_mask(l1c_dataset: ODCDataset) -> bool:
        """ Overridden in subclasses """
        pass

    def _should_process(self, dataset: ODCDataset) -> bool:
        """ Checks if masks should be generated for given ODCDataset """
        if check_existing_mask_directory(dataset, self.mask_product_name):
            LOGGER.info(f"Existing {self.mask_product_name} files for "
                        f"{dataset.uris[0]}, skipping")
            self.total_iterations -= 1
            return False
        metadata_cloud_percentage, in_threshold = self._check_clouds_in_threshold(dataset)
        if not in_threshold:
            self.total_iterations -= 1
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
    def write_l1c_reference(l1c_dataset: ODCDataset):
        if config.masks.write_rgb:
            write_l1c_dataset(l1c_dataset)
        if config.masks.write_l1c:
            write_l1c_dataset(l1c_dataset, rgb=False)
