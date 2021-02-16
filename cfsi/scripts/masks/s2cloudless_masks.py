from pathlib import Path
from typing import Dict, List
from logging import DEBUG
from datacube import Datacube
from datacube.model import Dataset as ODCDataset
import numpy as np
from s2cloudless import S2PixelCloudDetector
from osgeo import gdal

from cfsi import config
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer
from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import generate_s2_file_output_path, odcdataset_to_tif, write_l1c_dataset

LOGGER = create_logger("s2cloudless", level=DEBUG)


class S2CloudlessGenerator:

    def __init__(self):
        """ Constructor method """
        self.max_iterations = config.masks.s2cloudless_masks.max_iterations
        self.i = 1
        self.indexed_masks: List[ODCDataset] = []
        self.base_product_name = "s2a_level1c_granule"
        self.mask_product_name = "s2a_level1c_s2cloudless"

    def create_masks(self) -> List[ODCDataset]:
        """ Creates masks based on config """
        l1c_datasets = self.__get_l1c_datasets()
        if len(l1c_datasets) < self.max_iterations or not self.max_iterations:
            self.max_iterations = len(l1c_datasets)

        for dataset in l1c_datasets:
            if not self.__should_process(dataset):
                continue

            mask_arrays = self._process_dataset(dataset)
            output_masks = self._write_mask_arrays(dataset, mask_arrays)
            self.indexed_masks += S2CloudlessIndexer().index({dataset: output_masks})

            if self.i > self.max_iterations:
                LOGGER.warning(f"Reached maximum iterations count {self.max_iterations}")
                break
            self.i += 1

        if len(self.indexed_masks) == 0:
            LOGGER.warning("No new masks generated")
        return self.indexed_masks

    def __get_l1c_datasets(self) -> List[ODCDataset]:
        """ Gets all L1C datasets from ODC Index """
        dc = Datacube(app="s2cloudless_mosaic")
        l1c_datasets = dc.find_datasets(product=self.base_product_name)
        return l1c_datasets

    def __should_process(self, dataset: ODCDataset) -> bool:
        """ Checks if masks should be generated for given ODCDataset """
        if self.__check_existing_masks(dataset):
            LOGGER.info(f"S2Cloudless masks for dataset {dataset} already exist")
            return False
        metadata_cloud_percentage, in_threshold = self.__check_clouds_in_threshold(dataset)
        if not in_threshold:
            return False
        return True

    def __check_existing_masks(self, dataset: ODCDataset) -> bool:
        """ Checks if a S2Cloudless mask for given dataset already exists """
        # TODO: check if mask exists in index
        output_directory = generate_s2_file_output_path(dataset, self.mask_product_name).parent
        if output_directory.exists():
            return True
        return False

    @staticmethod
    def __check_clouds_in_threshold(dataset: ODCDataset) -> (float, bool):
        """ Checks if metadata cloud percentage of given dataset is within threshold """
        tile_metadata = dataset.metadata_doc["properties"]
        cloud_percent = tile_metadata["cloudy_pixel_percentage"]
        max_cloud = config.masks.s2cloudless_masks.max_cloud_threshold
        min_cloud = config.masks.s2cloudless_masks.min_cloud_threshold
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

    def _process_dataset(self, dataset: ODCDataset) -> (np.ndarray, np.ndarray):
        """ Generate cloud and cloud shadow masks for a single datacube dataset """
        LOGGER.info(f"Iteration {self.i}/{self.max_iterations}: {dataset}")
        tile_props = dataset.metadata_doc["properties"]
        s3_key = tile_props["s3_key"]

        ds = dataset_from_odcdataset("s2a_level1c_granule", dataset)

        LOGGER.info("Fetching data to array")
        array = np.moveaxis(ds.to_array().values.astype("float64") / 10000, 0, -1)
        LOGGER.info(f"Generating cloud masks for {s3_key}")
        cloud_masks = self.__generate_cloud_masks(array)
        LOGGER.info(f"Generating shadow masks for {s3_key}")
        shadow_masks = self.__generate_cloud_shadow_masks(
            array[:, :, :, 7], cloud_masks, tile_props["mean_sun_azimuth"])
        return cloud_masks, shadow_masks

    @staticmethod
    def __generate_cloud_masks(array: np.ndarray) -> np.ndarray:
        """ Generate binary cloud masks with s2cloudless """
        cloud_threshold = config.masks.s2cloudless_masks.cloud_threshold
        cloud_detector = S2PixelCloudDetector(threshold=cloud_threshold, all_bands=True)
        return np.squeeze(cloud_detector.get_cloud_masks(array)).astype("byte")

    @staticmethod
    def __generate_cloud_shadow_masks(nir_array: np.ndarray,
                                      cloud_mask_array: np.ndarray,
                                      mean_sun_azimuth: float) -> np.ndarray:
        """ Generate binary cloud shadow masks """
        az = np.deg2rad(mean_sun_azimuth)
        rows, cols = cloud_mask_array.shape
        # calculate how many rows/cols to shift cloud shadow masks
        x = np.math.cos(az)
        y = np.math.sin(az)
        cloud_projection_distance = config.masks.s2cloudless_masks.cloud_projection_distance
        x *= cloud_projection_distance
        y *= cloud_projection_distance

        new_rows = np.zeros((abs(int(y)), cols))
        new_cols = np.zeros((rows, abs(int(x))))
        # TODO: fix issues with projection direction
        # DEBUG - Mean sun azimuth: 133.129531680158. Shifting shadow masks by 21 rows, -20 cols
        # should shift towards top-left, i.e. -21 rows, -20 cols
        LOGGER.debug(f"Mean sun azimuth: {mean_sun_azimuth}. " +
                     f"Shifting shadow masks by {int(y)} rows, {int(x)} cols")
        new_rows[:] = 2
        new_cols[:] = 2

        if y > 0:
            shadow_mask_array = np.append(cloud_mask_array, new_rows, axis=0)[int(y):, :]
        else:
            shadow_mask_array = np.append(new_rows, cloud_mask_array, axis=0)[:-int(y), :]
        if x > 0:
            shadow_mask_array = np.append(new_cols, shadow_mask_array, axis=1)[:, :-int(x)]
        else:
            shadow_mask_array = np.append(shadow_mask_array, new_cols, axis=1)[:, -int(x):]

        dark_pixel_threshold = config.masks.s2cloudless_masks.dark_pixel_threshold
        dark_pixels = np.squeeze(np.where(nir_array <= dark_pixel_threshold, 1, 0))
        return np.where((cloud_mask_array == 0) & (shadow_mask_array == 1) & (dark_pixels == 1), 1, 0)

    @staticmethod
    def _write_mask_arrays(dataset: ODCDataset,
                           mask_arrays: (np.ndarray, np.ndarray)) -> Dict[str, Path]:
        """ Writes cloud and shadow masks to files """
        masks = {"clouds": mask_arrays[0],
                 "shadows": mask_arrays[1]}
        output_mask_files = odcdataset_to_tif(dataset, masks, "s2cloudless", gdal.GDT_Byte)
        output_masks = {
            "cloud_mask": output_mask_files[0],
            "shadow_mask": output_mask_files[1]}

        if config.masks.s2cloudless_masks.write_rgb:
            write_l1c_dataset(dataset)
        if config.masks.s2cloudless_masks.write_l1c:
            write_l1c_dataset(dataset, rgb=False)

        return output_masks
