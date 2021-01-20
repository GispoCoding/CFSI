import os
from datetime import date
from pathlib import Path
from typing import List, Tuple, Union, Dict
import numpy as np
from osgeo import gdal
import xarray as xa

from cfsi.utils.logger import create_logger
from cfsi.utils.load_datasets import dataset_from_odcdataset
from datacube.model import Dataset as ODCDataset

gdal.UseExceptions()
LOGGER = create_logger("write_utils")


def odcdataset_to_tif(dataset: ODCDataset,
                      data: Union[List[np.ndarray], Dict[str, np.ndarray]],
                      product_name: str = "",
                      data_type: int = gdal.GDT_Float32) -> List[Path]:
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
    array_to_geotiff(output_dir, data, geo_transform, projection, data_type=data_type)
    return output_dir


def gdal_params_for_odcdataset(dataset: ODCDataset):
    """ Gets transformation and projection info for writing ODCDataset with GDAL """
    ds = dataset_from_odcdataset("s2a_level1c_granule", dataset)
    return gdal_params_for_xadataset(ds)


def gdal_params_for_xadataset(dataset: xa.Dataset):
    """ Gets transformation and projection info for writing Datacube with GDAL """
    geo_transform = dataset.geobox.transform.to_gdal()
    projection = dataset.geobox.crs.wkt
    return geo_transform, projection


def generate_s2_file_output_path(dataset: ODCDataset,
                                 product_name: str = "",
                                 band_name: str = "") -> Path:
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
        array_to_geotiff(output_path,
                         data[band_name],
                         geo_transform,
                         projection,
                         data_type=data_type)
        output_paths.append(output_path)
    return output_paths


def write_l1c_dataset_rgb(dataset: ODCDataset):
    """ Writes a ODC S2 L1C dataset to a rgb .tif file """
    LOGGER.info(f"Writing RGB output for dataset {dataset}")
    rgb_bands = ['B02', 'B03', 'B04']
    rgb_ds = dataset_from_odcdataset("s2a_l1c_granule", dataset, measurements=rgb_bands)
    data = [np.squeeze(rgb_ds[band].values / 10000) for band in rgb_ds.data_vars]
    odcdataset_to_tif(dataset, data, product_name="rgb")


def array_to_geotiff(file_path: Path,
                     data: Union[List[np.ndarray], np.ndarray],
                     geo_transform: Tuple,
                     projection: str,
                     nodata_val=0,
                     data_type=gdal.GDT_Float32):
    """ Create a single or multi band GeoTIFF file with data from an array.
    file_name : output geotiff file path including extension
    data : list of numpy arrays
    geo_transform : Geotransform for output raster; e.g.
    "(up_left_x, x_size, x_rotation, up_left_y, y_rotation, y_size)"
    projection : WKT projection for output raster
    nodata_val : Value to convert to nodata in the output raster; default 0
    data_type : gdal data_type object, optional
        Optionally set the data_type of the output raster; can be
        useful when exporting an array of float or integer values. """
    driver = gdal.GetDriverByName('GTiff')
    if not file_path.parent.exists():
        LOGGER.info(f"Creating output directory {file_path.parent}")
        file_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, np.ndarray):
        data = [data]
    rows, cols = data[0].shape  # Create raster of given size and projection
    dataset = driver.Create(str(file_path), cols, rows, len(data), data_type)
    dataset.SetGeoTransform(geo_transform)
    dataset.SetProjection(projection)
    for idx, d in enumerate(data):
        band = dataset.GetRasterBand(idx + 1)
        band.WriteArray(d)
        band.SetNoDataValue(nodata_val)

    # noinspection PyUnusedLocal
    dataset = None  # Close %%file


def generate_mosaic_output_path(mosaic_name: str) -> Path:
    """ Generates an output Path for a new mosaic """
    base_output_path = Path(os.environ["CFSI_CONTAINER_OUTPUT"])
    mosaic_dir = Path(base_output_path / "mosaics")
    i = 0
    file_path = Path(mosaic_dir / f"{date.today()}_{mosaic_name}_{i}.tif")
    while file_path.exists():
        i += 1
        file_path = Path(mosaic_dir / f"{date.today()}_{mosaic_name}_{i}.tif")
    return file_path
