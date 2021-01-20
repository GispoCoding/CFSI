import os
from typing import Dict, List, Union
from pathlib import Path
from logging import DEBUG
import datacube
from datacube.model import Dataset as ODCDataset
import datacube.storage._read  # TODO: Remove hack to avoid circular import ImportError
import numpy as np
from s2cloudless import S2PixelCloudDetector
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer
from cfsi.scripts.mosaic.mosaic import mosaic_from_mask_datasets
from cfsi.utils.array_to_geotiff import array_to_geotiff_multiband
from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger
from osgeo import gdal
gdal.UseExceptions()

# TODO: define in a separate config file
CLOUD_THRESHOLD = 0.3           # cloud threshold value for s2cloudless
MAX_CLOUD_THRESHOLD = 100.0     # maximum cloudiness percentage in metadata
MIN_CLOUD_THRESHOLD = 0.0       # minimum cloudiness percentage in metadata
CLOUD_PROJECTION_DISTANCE = 30  # maximum distance to search for cloud shadows
DARK_PIXEL_THRESHOLD = 0.15     # max band 8 value for pixel to be considered dark
WRITE_RGB = False               # write L1C rgb for validating results

OUTPUT_PATH = Path(os.environ["CFSI_CONTAINER_OUTPUT"])  # TODO: write to S3
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

LOGGER = create_logger("s2cloudless", level=DEBUG)


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
    LOGGER.info(f"Processing {s3_key}, {metadata_cloud_percentage}% cloudy")
    ds = dataset_from_odcdataset("s2a_level1c_granule", dataset)

    LOGGER.info("Fetching data to array")
    array = np.moveaxis(ds.to_array().values.astype("float64") / 10000, 0, -1)
    LOGGER.debug(f"Loaded array shaped {array.shape} into memory, size {array.nbytes} bytes")
    LOGGER.info(f"Generating cloud masks for {s3_key}")
    cloud_masks = generate_cloud_masks(array)
    LOGGER.info(f"Generating shadow masks for {s3_key}")
    # TODO: evaluate performance
    shadow_masks = generate_cloud_shadow_masks(array[:, :, :, 7], cloud_masks, tile_props["mean_sun_azimuth"])

    LOGGER.info("Mask generation done")
    return cloud_masks, shadow_masks


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


def odcdataset_to_tif(
        dataset: ODCDataset,
        data: Union[List[np.ndarray], Dict[str, np.ndarray]],
        product_name: str = "",
        data_type: int = gdal.GDT_Float32,
        ) -> List[Path]:
    """ Write a ODCDataset to .tif file(s).
     :param dataset: ODC Dataset being written
     :param data: data to write in numpy ndarray,
            or output_name: ndarray dict if writing multiple single band files
     :param product_name: write .tif(s) to own subdirectory with product name suffix, optional
     :param data_type: GDAL data type to use when writing file, optional
     :return: list of Paths of written files """

    if isinstance(data, List):
        output_file = odcdataset_to_single_tif(dataset, data, product_name, data_type)
        return [output_file]
    else:
        output_files = odcdataset_to_multiple_tif(dataset, data, product_name, data_type)
        return output_files


def odcdataset_to_single_tif(dataset: ODCDataset,
                             data: List[np.ndarray],
                             product_name: str = "",
                             data_type: int = gdal.GDT_Float32) -> Path:
    """ Writes a list of ndarray to single .tif file.
     :param dataset: ODC dataset being written
     :param data: list of ndarray
     :param product_name: name of product being written, optional
     :param data_type: GDAL data type, optional """
    geo_transform, projection = gdal_params_for_odcdataset(dataset)
    output_dir = generate_s2_file_output_path(dataset, product_name)
    array_to_geotiff_multiband(str(output_dir), data, geo_transform, projection, data_type=data_type)
    return output_dir


def gdal_params_for_odcdataset(dataset: ODCDataset):
    """ Gets transformation and projection info for writing ODCDataset with GDAL """
    ds = dataset_from_odcdataset("s2a_level1c_granule", dataset)
    geo_transform = ds.geobox.transform.to_gdal()
    projection = ds.geobox.crs.wkt
    return geo_transform, projection


def generate_s2_file_output_path(dataset: ODCDataset, product_name: str = "", band_name: str = "") -> Path:
    """ Generates a output path for writing a ODCDataset to a .tif file.
     :param dataset: ODCDataset being written
     :param product_name: product name being written. each product goes to its own sub-directory, optional
     :param band_name: name of band being written. band name is appended to filename, optional """
    base_path = Path(os.environ["CFSI_CONTAINER_OUTPUT"])  # TODO: write to S3
    tile_id, s3_key = get_s2_tile_ids(dataset)
    file_name = f"{tile_id}"
    if band_name:
        file_name += f"_{band_name}"
    file_name += ".tif"
    output_dir = Path(base_path / s3_key).joinpath(product_name, file_name)
    LOGGER.debug(f"Generated output directory {output_dir} for dataset {dataset}")
    return output_dir


def get_s2_tile_ids(dataset: ODCDataset) -> (str, str):
    """ Returns tile_id and s3_key from dataset metadata doc """
    tile_props = dataset.metadata_doc["properties"]
    tile_id = tile_props["tile_id"]
    tile_path = tile_props["s3_key"]
    return tile_id, tile_path


def odcdataset_to_multiple_tif(dataset: ODCDataset,
                               data: Dict[str, np.ndarray],
                               product_name: str = "",
                               data_type: int = gdal.GDT_Float32) -> List[Path]:
    """ Writes output in dictionary to multiple single band .tif files.
     :param dataset: ODCDataset being written
     :param data: dict of band_name: np.ndarray, each band is written to a separate file
     :param product_name: name of product being written, optional
     :param data_type: GDAL datatype, optional
     :return: list of written files """
    output_paths = []
    geo_transform, projection = gdal_params_for_odcdataset(dataset)
    for band_name in data:
        output_path = generate_s2_file_output_path(dataset, product_name, band_name)
        array_to_geotiff_multiband(
            str(output_path),
            [data[band_name]],
            geo_transform,
            projection,
            data_type=data_type)
        output_paths.append(output_path)
    return output_paths


def write_dataset_rgb(dataset: ODCDataset):
    """ Writes a ODC dataset to a rgb .tif file """
    rgb_bands = ['B02', 'B03', 'B04']
    rgb_ds = dataset_from_odcdataset("s2a_l1c_granule", dataset, measurements=rgb_bands)
    data = [np.squeeze(rgb_ds[band].values / 10000) for band in rgb_ds.data_vars]
    odcdataset_to_tif(dataset, data, product_name="rgb")


def check_existing_masks(dataset: ODCDataset, product_name: str) -> bool:
    """ Checks if a S2Cloudless mask for given dataset already exists """
    output_directory = generate_s2_file_output_path(dataset, product_name).parent
    if output_directory.exists():
        return True
    return False


def main():
    """ Create s2cloudless masks for indexed L1C datasets """
    LOGGER.info("Starting")
    dc = datacube.Datacube(app="s2cloudless-main")
    l1c_datasets = dc.find_datasets(product="s2a_level1c_granule")
    indexed_masks: List[ODCDataset] = []

    i = 1
    max_iterations = 4
    if len(l1c_datasets) < max_iterations:
        max_iterations = len(l1c_datasets)

    for dataset in l1c_datasets:
        LOGGER.info(f"Processing {dataset}, iteration {i}/{max_iterations}")
        if check_existing_masks(dataset, "s2cloudless"):
            LOGGER.info(f"S2Cloudless masks for dataset {dataset} already exist")
            continue
        mask_arrays = process_dataset(dataset)
        masks = {"clouds": mask_arrays[0],
                 "shadows": mask_arrays[1]}

        output_mask_files = odcdataset_to_tif(dataset, masks, "s2cloudless", gdal.GDT_Byte)
        output_masks = {
            "cloud_mask": output_mask_files[0],
            "shadow_mask": output_mask_files[1]}
        if WRITE_RGB:
            LOGGER.info("Writing rgb output")
            write_dataset_rgb(dataset)  # TODO: write corresponding L2A dataset
        LOGGER.info(f"Finished processing {dataset}, indexing output")
        indexed_masks += S2CloudlessIndexer().index({dataset: output_masks})
        i += 1
        if i > max_iterations:
            LOGGER.warning(f"Reached maximum iterations count {max_iterations}")
            break

    LOGGER.info(f"Creating mosaic from {len(indexed_masks)} masks")
    mosaic_ds = mosaic_from_mask_datasets(indexed_masks)
    mosaic_filepath = Path(OUTPUT_PATH / "latest_mosaic.tif")
    LOGGER.info(f"Writing mosaic to {mosaic_filepath}")
    geo_transform = mosaic_ds.geobox.transform.to_gdal()
    projection = mosaic_ds.geobox.crs.wkt
    mosaic_data: List[np.ndarray] = [np.squeeze(mosaic_ds[band].values) for band in mosaic_ds.data_vars]
    array_to_geotiff_multiband(str(mosaic_filepath),
                               mosaic_data,
                               geo_transform,
                               projection)


if __name__ == "__main__":
    main()
