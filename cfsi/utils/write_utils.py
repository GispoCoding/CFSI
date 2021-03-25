from pathlib import Path
from typing import List, Tuple, Union, Dict
import numpy as np
import xarray as xa
import rasterio as rio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.transform import Affine

from cfsi.utils import generate_s2_tif_path
from cfsi.utils.logger import create_logger
from cfsi.utils.load_datasets import xadataset_from_odcdataset
from datacube.model import Dataset as ODCDataset

LOGGER = create_logger("write_utils")


def write_l1c_dataset(dataset: ODCDataset, rgb: bool = True):
    """ Writes a ODC S2 L1C dataset to a rgb .tif file """
    measurements = None
    product_name = "l1c"
    if rgb:
        measurements = ['B02', 'B03', 'B04']
        product_name = "rgb"
    output_dir = generate_s2_tif_path(dataset, product_name).parent
    if output_dir.exists():
        LOGGER.info(f"Directory {product_name} for L1C output already exists, skipping")
        return

    LOGGER.info(f"Writing L1C output for dataset {dataset}")
    ds = xadataset_from_odcdataset(dataset, measurements=measurements)
    data = [np.squeeze(ds[band].values / 10000) for band in ds.data_vars]
    odcdataset_to_single_tif(dataset, data, product_name=product_name)


def odcdataset_to_single_tif(dataset: ODCDataset,
                             data: List[np.ndarray],
                             product_name: str = "",
                             data_type: int = rio.float32,
                             custom_transform: Tuple[Affine, CRS] = None) -> Path:
    """ Writes a list of ndarray to single .tif file.
     :param dataset: ODC dataset being written
     :param data: list of ndarray
     :param product_name: name of product being written, optional
     :param data_type: rasterio data type, optional
     :param custom_transform: provide custom transform and CRS, optional
     :return: Path of written file """
    if custom_transform:
        geo_transform, projection = custom_transform
    else:
        geo_transform, projection = rio_params_for_odcdataset(dataset)

    output_path = generate_s2_tif_path(dataset, product_name)
    array_to_geotiff(output_path, data, geo_transform=geo_transform, projection=projection, data_type=data_type)
    return output_path


def odcdataset_to_multiple_tif(dataset: ODCDataset,
                               data: Dict[str, np.ndarray],
                               product_name: str = "",
                               data_type: int = rio.float32,
                               custom_transform: Tuple[Affine, CRS] = None) -> List[Path]:
    """ Writes output in dictionary to multiple single band .tif files.
     :param dataset: ODCDataset being written
     :param data: dict of band_name: np.ndarray, each band is written to a separate file
     :param product_name: name of product being written, optional
     :param data_type: rasterio datatype, optional
     :param custom_transform: provide custom transform and CRS, optional
     :return: list of written files """
    if custom_transform:
        geo_transform, projection = custom_transform
    else:
        geo_transform, projection = rio_params_for_odcdataset(dataset)

    output_paths = []
    for band_name, band_data in data.items():
        output_path = generate_s2_tif_path(dataset, product_name, band_name)
        array_to_geotiff(output_path, band_data,
                         geo_transform=geo_transform, projection=projection,
                         data_type=data_type)
        output_paths.append(output_path)
    return output_paths


def rio_params_for_odcdataset(dataset: ODCDataset):
    """ Gets transformation and projection info for writing ODCDataset with rasterio """
    ds = xadataset_from_odcdataset(dataset)
    return rio_params_for_xadataset(ds)


def rio_params_for_xadataset(dataset: xa.Dataset):
    """ Gets transformation and projection info for writing xa.Dataset with rasterio """
    geo_transform = dataset.geobox.transform
    projection = dataset.geobox.crs
    return geo_transform, projection


def array_to_geotiff(file_path: Path,
                     data: Union[List[np.ndarray], np.ndarray],
                     geo_transform: Affine, projection: CRS,
                     compress: str = "lzw", data_type=rio.float32):
    """ Write a single or multi band GeoTIFF
    :param file_path: output geotiff file path including extension
    :param data: (list of) numpy array(s), all written to single file
    :param geo_transform: Geotransform for output raster in rasterio format
    :param projection: projection for output raster in rasterio format
    :param compress: output compression method, use "none" for uncompressed, optional
    :param data_type: rasterio data type, optional """
    if not file_path.parent.exists():
        LOGGER.info(f"Creating output directory {file_path.parent}")
        file_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, np.ndarray):
        data = [data]

    rows, cols = data[0].shape  # Create raster of given size and projection
    with rio.open(file_path, "w",
                  driver="GTiff", compress=compress,
                  height=rows, width=cols,
                  transform=geo_transform, crs=projection,
                  count=(len(data)), nodata=0,
                  dtype=data_type) as dest:

        for idx, d in enumerate(data):
            dest.write(d.astype(data_type), idx + 1)


def create_overviews(file_path: Path):
    """ Create internal overviews to GeoTIFF
    :param file_path: Path to GeoTIFF file """
    with rio.open(file_path, "r+") as f:
        f.build_overviews([2, 4, 8, 16, 32], Resampling.nearest)
        f.update_tags(ns="rio_overview", resampling="nearest")
