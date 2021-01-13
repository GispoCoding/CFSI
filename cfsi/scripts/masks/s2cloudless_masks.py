from os import environ
from typing import Union, List
from pathlib import Path
import logging
import datacube
from datacube.model import Dataset as ODCDataset
import datacube.storage._read  # TODO: Remove hack to avoid circular import ImportError
import numpy as np
from xarray import Dataset
from s2cloudless import S2PixelCloudDetector
from ...utils.array_to_geotiff import array_to_geotiff_multiband
from ...utils.logger import create_logger
from osgeo import gdal
gdal.UseExceptions()


CLOUD_THRESHOLD = 0.3  # cloud threshold value for s2cloudless
MAX_CLOUD_THRESHOLD = 94.0  # maximum cloudiness percentage in metadata
MIN_CLOUD_THRESHOLD = 1.0  # minimum cloudiness percentage in metadata
CLOUD_PROJECTION_DISTANCE = 30  # maximum distance to search for cloud shadows
DARK_PIXEL_THRESHOLD = 0.15
WRITE_RGB = True

try:
    OUTPUT_PATH = Path(environ["CFSI_OUTPUT_DIR"])
    if not OUTPUT_PATH.exists():
        raise Exception("Error in env. variable OUTPUT_PATH: directory does not exist")
except KeyError:
    OUTPUT_PATH = Path("/home/mikael/tmp/cfsi_output")

LOGGER = create_logger("s2cloudless", level=logging.DEBUG)


def process_dataset(dataset: ODCDataset) -> (np.ndarray, np.ndarray):
    """ Generate cloud and cloud shadow masks for a single datacube dataset """
    tile_props = dataset.metadata_doc["properties"]
    metadata_cloud_percentage = tile_props["cloudy_pixel_percentage"]
    if metadata_cloud_percentage > MAX_CLOUD_THRESHOLD:
        LOGGER.info("Metadata cloud percentage greater than max threshold value: " +
                    f"{MAX_CLOUD_THRESHOLD} < {metadata_cloud_percentage}")
        raise ValueError  # TODO: add custom exception and catch
    if metadata_cloud_percentage < MIN_CLOUD_THRESHOLD:
        LOGGER.info("Metadata cloud percentage lower than min threshold value: " +
                    f"{MIN_CLOUD_THRESHOLD} > {metadata_cloud_percentage}")
        raise ValueError

    s3_key = tile_props["s3_key"]
    LOGGER.info(f"Processing {s3_key}\t{metadata_cloud_percentage}% cloudy")
    ds = load_datasets([dataset], app_name=f"s2cloudless-processor_{s3_key}")

    LOGGER.info("Fetching data to array")
    array = np.moveaxis(ds.to_array().values.astype("float64") / 10000, 0, -1)
    LOGGER.info("Generating cloud masks")
    cloud_masks = generate_cloud_masks(array)  # TODO: evaluate performance
    LOGGER.info("Generating shadow masks")
    # TODO: evaluate performance
    shadow_masks = generate_cloud_shadow_masks(array[:, :, :, 7], cloud_masks, tile_props["mean_sun_azimuth"])

    LOGGER.info("Mask generation done")
    return cloud_masks, shadow_masks


def load_datasets(
        datasets: Union[List[ODCDataset], ODCDataset],
        measurements: List[str] = None,
        app_name: str = "s2cloudless") -> Dataset:
    """ Loads a xarray.Dataset datacube from an ODC dataset """
    if not isinstance(datasets, list):
        datasets = [datasets]
    dc = datacube.Datacube(app=app_name)
    ds = dc.load(product="s2a_level1c_granule",
                 dask_chunks={},
                 measurements=measurements,
                 output_crs="epsg:32635",  # TODO: read from dataset
                 resolution=(-10, 10),  # TODO: read from dataset
                 crs="epsg:32635",  # TODO: read from dataset
                 datasets=datasets)
    return ds


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
    LOGGER.debug(f"Mean sun azimuth: {mean_sun_azimuth}. " +
                 f"Shifting shadow masks by {int(new_rows)} rows, {int(new_cols)} cols")
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
    LOGGER.debug("shapes")
    LOGGER.debug("cloud mask\t\tshadow_mask\t\tdark_pixels")
    LOGGER.debug(f"{cloud_mask_array.shape}\t{shadow_mask_array.shape}\t\t{dark_pixels.shape}")
    return np.where((cloud_mask_array == 0) & (shadow_mask_array == 1) & (dark_pixels == 1), 1, 0)


def write_to_tif(
        dataset: ODCDataset,
        data: List[np.ndarray],
        data_type: int = gdal.GDT_Byte,
        file_suffix: str = "s2cloudless"):
    """ Write a set of ndarrays to .tif """
    tile_props = dataset.metadata_doc["properties"]
    tile_path = Path(tile_props["s3_key"])
    tile_id = tile_props["tile_id"]
    output_dir = Path(OUTPUT_PATH / tile_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    ds = load_datasets(dataset, app_name=f"s2cloudless-writer_{tile_path}")
    geo_transform = ds.geobox.transform.to_gdal()
    projection = ds.geobox.crs.wkt
    array_to_geotiff_multiband(
        str(Path(output_dir / f"{tile_id}_{file_suffix}.tif")),
        data,
        geo_transform,
        projection,
        data_type=data_type)


def write_dataset_rgb(dataset: ODCDataset):
    """ Writes a ODC dataset to a rgb .tif file """
    rgb_bands = ['B02', 'B03', 'B04']
    rgb_ds = load_datasets(dataset, measurements=rgb_bands, app_name=f"s2cloudless-writer_rgb")
    write_to_tif(
        dataset,
        [np.squeeze(rgb_ds[band].values / 10000) for band in rgb_ds.data_vars],
        data_type=gdal.GDT_Float32,
        file_suffix="rgb")


def main():
    LOGGER.info("Starting")
    dc = datacube.Datacube(app="s2cloudless-main")
    l1c_datasets = dc.find_datasets(product="s2a_level1c_granule")

    for dataset in l1c_datasets:
        try:
            masks = list(process_dataset(dataset))
        except ValueError:  # TODO: catch and handle custom exceptions
            continue
        LOGGER.info("Writing output")
        write_to_tif(dataset, masks)
        if WRITE_RGB:
            LOGGER.info(f"Writing rgb output")
            write_dataset_rgb(dataset)  # TODO: write corresponding L2A dataset
        LOGGER.info("Finished processing")
        break


if __name__ == "__main__":
    main()
