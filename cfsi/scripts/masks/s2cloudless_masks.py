import glob
from pathlib import Path
from typing import Dict, List
from logging import DEBUG
from datacube.model import Dataset as ODCDataset
import numpy as np
from s2cloudless import S2PixelCloudDetector
from osgeo.gdal import GDT_Byte as Byte
import rasterio as rio
from rasterio.warp import reproject, Resampling

import cfsi
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer
from cfsi.scripts.masks.cloud_mask_generator import CloudMaskGenerator
from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import get_s2_tile_ids, odcdataset_to_multiple_tif

LOGGER = create_logger("s2cloudless", level=DEBUG)

config = cfsi.config()


class S2CloudlessGenerator(CloudMaskGenerator):

    def __init__(self):
        """ Constructor method """
        super().__init__()
        self.max_iterations = config.masks.s2cloudless_masks.max_iterations
        self.mask_product_name = "s2_level1c_s2cloudless"

    def _create_mask(self, l1c_dataset: ODCDataset) -> bool:
        """ Creates a single mask, returns bool indicating whether to continue iteration """
        if not config.masks.s2cloudless_masks.generate:
            LOGGER.info("Skipping s2cloudless mask generation due to config")
            return False
        if not self._should_process(l1c_dataset):
            return True

        LOGGER.info(f"Iteration {self.i}/{self.max_iterations}: {l1c_dataset}")

        mask_arrays = self.__process_dataset(l1c_dataset)
        output_masks = self.__write_masks(l1c_dataset, mask_arrays)
        self.indexed_masks.append(S2CloudlessIndexer().index_masks(l1c_dataset, output_masks))

        return self._continue_iteration()

    def __process_dataset(self, dataset: ODCDataset) -> (np.ndarray, np.ndarray):
        """ Generate cloud and cloud shadow masks for a single datacube dataset """
        _, s3_key = get_s2_tile_ids(dataset)
        mean_sun_azimuth = dataset.metadata_doc["properties"]["mean_sun_azimuth"]

        ds = dataset_from_odcdataset(dataset)

        LOGGER.info("Fetching data to array")
        array = self.__construct_s2_array(dataset)
        LOGGER.info(f"Generating s2cloudless masks for {s3_key}")
        cloud_masks = self.__generate_cloud_masks(array)
        LOGGER.info(f"Generating shadow masks for {s3_key}")
        shadow_masks = self.__generate_cloud_shadow_masks(
            array[:, :, :, 7], cloud_masks, mean_sun_azimuth)
        return cloud_masks, shadow_masks

    def __construct_s2_array(self, dataset: ODCDataset) -> np.ndarray:
        """ Constructs a np.ndarray from S2 L1C dataset """
        if not config.masks.s2cloudless_masks.cache:
            ds = dataset_from_odcdataset(dataset)
            return np.moveaxis(ds.to_array().values.astype("float64") / 10000, 0, -1)

        jp2_files = self.__fetch_s2_data(dataset)
        return self.__array_from_jp2_files(dataset, jp2_files)

    def __fetch_s2_data(self, dataset: ODCDataset) -> List[str]:
        """ Fetches S2 data from S3 in .SAFE format, returns list of fetched JP2 files """
        tile_id, s3_key = get_s2_tile_ids(dataset)
        image_path = self.fetch_s2_to_safe(tile_id).joinpath("IMG_DATA")
        jp2_files = glob.glob(str(image_path) + "/*B??.jp2")
        jp2_files.sort()  # B1, B2, B3, ... B11, B12, B8A
        jp2_files.insert(8, jp2_files[-1])  # ... B7, B8, B8A, B9, ...
        jp2_files = jp2_files[:-1]
        if len(jp2_files) != 13:
            LOGGER.warning(f"Unexpected number of images in {image_path}: {len(jp2_files)}")
        return jp2_files

    @staticmethod
    def __array_from_jp2_files(dataset: ODCDataset, jp2_files: List[str]) -> np.ndarray:
        """ Constructs a np.ndarray from a list of JP2 files """
        LOGGER.info("Reading and reprojecting data to arrays from JP2 files")
        arrays = []
        with rio.open(jp2_files[1], nodata=0) as f:  # Read B02 first as it has full resolution
            data = f.read()
            tmp_arr = np.empty_like(data)
            transform = f.transform

        for jp2_file in jp2_files:
            with rio.open(jp2_file) as f:
                data = f.read()
                reproject(data, destination=tmp_arr,
                          src_transform=f.transform, dst_transform=transform,
                          src_crs=f.crs, dst_crs=f.crs,
                          src_nodata=0, dst_nodata=0,
                          resampling=Resampling.nearest)
                arrays.append(tmp_arr)

        LOGGER.info("Constructing final array")
        final_array = np.array(arrays, dtype="float64") / 10000
        final_array = np.moveaxis(final_array, 0, -1)
        return final_array

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

        x, y = int(x), int(y)
        if y == 0:
            shadow_mask_array = cloud_mask_array
        elif y > 0:
            shadow_mask_array = np.append(cloud_mask_array, new_rows, axis=0)[int(y):, :]
        else:
            shadow_mask_array = np.append(new_rows, cloud_mask_array, axis=0)[:-int(y), :]

        if x == 0:
            pass
        elif x > 0:
            shadow_mask_array = np.append(new_cols, shadow_mask_array, axis=1)[:, :-int(x)]
        else:
            shadow_mask_array = np.append(shadow_mask_array, new_cols, axis=1)[:, -int(x):]

        dark_pixel_threshold = config.masks.s2cloudless_masks.dark_pixel_threshold
        dark_pixels = np.squeeze(np.where(nir_array <= dark_pixel_threshold, 1, 0))
        return np.where((cloud_mask_array == 0) & (shadow_mask_array == 1) & (dark_pixels == 1), 1, 0)

    def __write_masks(self, dataset: ODCDataset,
                      mask_arrays: (np.ndarray, np.ndarray)) -> Dict[str, Path]:
        """ Writes cloud and shadow masks to files """
        masks = {"clouds": mask_arrays[0],
                 "shadows": mask_arrays[1]}
        output_mask_files = self.__write_mask_arrays(dataset, masks)
        output_masks = {
            "cloud_mask": output_mask_files[0],
            "shadow_mask": output_mask_files[1]}

        self.write_l1c_reference(dataset)

        return output_masks

    def __write_mask_arrays(self, dataset: ODCDataset, masks: Dict[str, np.ndarray]) -> List[Path]:
        """ Writes mask arrays to GeoTIFF files """
        transform = None
        if config.masks.s2cloudless_masks.cache:
            jp2_files = self.__fetch_s2_data(dataset)
            with rio.open(jp2_files[1], nodata=0) as f:  # Read B02 first as it has full resolution
                transform = (tuple(f.transform.to_gdal()), f.crs.to_wkt())

        return odcdataset_to_multiple_tif(dataset, masks,
                                          product_name=self.mask_product_name,
                                          data_type=Byte,
                                          transform=transform)


if __name__ == "__main__":
    S2CloudlessGenerator().create_masks()
