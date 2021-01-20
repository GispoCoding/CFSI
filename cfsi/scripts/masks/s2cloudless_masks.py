from typing import Dict, Union
from logging import DEBUG
from datacube.model import Dataset as ODCDataset
import numpy as np
from s2cloudless import S2PixelCloudDetector

from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import generate_s2_file_output_path

# TODO: define in a separate config file
CLOUD_THRESHOLD = 0.3           # cloud threshold value for s2cloudless
MAX_CLOUD_THRESHOLD = 100.0     # maximum cloudiness percentage in metadata
MIN_CLOUD_THRESHOLD = 0.0       # minimum cloudiness percentage in metadata
CLOUD_PROJECTION_DISTANCE = 30  # maximum distance to search for cloud shadows
DARK_PIXEL_THRESHOLD = 0.15     # max band 8 value for pixel to be considered dark
WRITE_RGB = False               # write L1C rgb for validating results

LOGGER = create_logger("s2cloudless", level=DEBUG)


def process_dataset(dataset: ODCDataset) -> (np.ndarray, np.ndarray):
    """ Generate cloud and cloud shadow masks for a single datacube dataset """
    tile_props = dataset.metadata_doc["properties"]
    s3_key = tile_props["s3_key"]
    metadata_cloud_percentage = cloud_percentage_inside_threshold(tile_props)
    if isinstance(metadata_cloud_percentage, bool):
        return

    LOGGER.info(f"Processing {s3_key}, {metadata_cloud_percentage}% cloudy")
    ds = dataset_from_odcdataset("s2a_level1c_granule", dataset)

    LOGGER.info("Fetching data to array")
    array = np.moveaxis(ds.to_array().values.astype("float64") / 10000, 0, -1)
    LOGGER.info(f"Generating cloud masks for {s3_key}")
    cloud_masks = generate_cloud_masks(array)
    LOGGER.info(f"Generating shadow masks for {s3_key}")
    shadow_masks = generate_cloud_shadow_masks(array[:, :, :, 7], cloud_masks, tile_props["mean_sun_azimuth"])
    LOGGER.info("Mask generation done")
    return cloud_masks, shadow_masks


def cloud_percentage_inside_threshold(tile_metadata: Dict) -> Union[float, bool]:
    """ Checks if metadata cloud percentage of given dataset is within threshold """
    metadata_cloud_percentage = tile_metadata["cloudy_pixel_percentage"]
    if metadata_cloud_percentage > MAX_CLOUD_THRESHOLD:
        LOGGER.info("Metadata cloud percentage greater than max threshold value: " +
                    f"{MAX_CLOUD_THRESHOLD} < {metadata_cloud_percentage}")
        return False
    if metadata_cloud_percentage < MIN_CLOUD_THRESHOLD:
        LOGGER.info("Metadata cloud percentage lower than min threshold value: " +
                    f"{MIN_CLOUD_THRESHOLD} > {metadata_cloud_percentage}")
        return False
    return metadata_cloud_percentage


def generate_cloud_masks(array: np.ndarray) -> np.ndarray:
    """ Generate binary cloud masks with s2cloudless """
    cloud_detector = S2PixelCloudDetector(threshold=CLOUD_THRESHOLD, all_bands=True)
    return np.squeeze(cloud_detector.get_cloud_masks(array)).astype("byte")


def generate_cloud_shadow_masks(nir_array: np.ndarray,
                                cloud_mask_array: np.ndarray,
                                mean_sun_azimuth: float) -> np.ndarray:
    """ Generate binary cloud shadow masks """
    az = np.deg2rad(mean_sun_azimuth)
    rows, cols = cloud_mask_array.shape
    # calculate how many rows/cols to shift cloud shadow masks
    x = np.math.cos(az)
    y = np.math.sin(az)
    x *= CLOUD_PROJECTION_DISTANCE
    y *= CLOUD_PROJECTION_DISTANCE

    new_rows = np.zeros((abs(int(y)), cols))
    new_cols = np.zeros((rows, abs(int(x))))
    # TODO: fix issues with projection direction
    # DEBUG - Mean sun azimuth: 133.129531680158. Shifting shadow masks by 21 rows, -20 cols
    # should shift towards top-left, i.e. -21 rows, -20 cols
    LOGGER.debug(f"Mean sun azimuth: {mean_sun_azimuth}. " +
                 f"Shifting shadow masks by {int(y)} rows, {int(x)} cols")
    new_rows[:] = 1
    new_cols[:] = 1

    if y > 0:
        shadow_mask_array = np.append(cloud_mask_array, new_rows, axis=0)[int(y):, :]
    else:
        shadow_mask_array = np.append(new_rows, cloud_mask_array, axis=0)[:int(y), :]
    if x < 0:
        shadow_mask_array = np.append(new_cols, shadow_mask_array, axis=1)[:, :int(x)]
    else:
        shadow_mask_array = np.append(shadow_mask_array, new_cols, axis=1)[:, int(x):]

    dark_pixels = np.squeeze(np.where(nir_array <= DARK_PIXEL_THRESHOLD, 1, 0))
    return np.where((cloud_mask_array == 0) & (shadow_mask_array == 1) & (dark_pixels == 1), 1, 0)


def check_existing_masks(dataset: ODCDataset, product_name: str) -> bool:
    """ Checks if a S2Cloudless mask for given dataset already exists """
    output_directory = generate_s2_file_output_path(dataset, product_name).parent
    if output_directory.exists():
        return True
    return False
