import os
from datetime import date
from logging import DEBUG
from pathlib import Path
from typing import List

from datacube import Datacube
from datacube.model import Dataset as ODCDataset
import numpy as np
from osgeo import gdal
import xarray as xa

from cfsi import config
from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import array_to_geotiff, gdal_params_for_xadataset

LOGGER = create_logger("s2cloudless_mosaic", level=DEBUG)

# TODO: option to write latest image as reference
# TODO: write output to correct path


class MosaicCreator:

    def __init__(self, mask_product_name: str):
        """ Constructor method """
        self.__product_name = mask_product_name
        self.__mask_datasets = self.__get_mask_datasets()

    def __get_mask_datasets(self) -> List[ODCDataset]:
        """ Finds mask datasets based on config """
        # TODO: use config, now returns all datasets for product
        dc = Datacube(app="mosaic_creator")
        return dc.find_datasets(product=self.__product_name)

    def create_mosaic_dataset(self) -> xa.Dataset:
        """ Creates a cloudless mosaic from cloud/shadow mask ODCDatasets """
        LOGGER.info(f"Creating mosaic dataset from {len(self.__mask_datasets)} masks")
        ds = self.__setup_mask_datacube()
        ds = ds.where((ds.cloud_mask == 0) & (ds.shadow_mask == 0), 0)

        ds_out: xa.Dataset = ds.copy(deep=True).isel(time=-1)
        recentness: int = config.mosaic.recentness
        output_bands = config.mosaic.output_bands
        i = 1

        for band in output_bands:
            LOGGER.info(f"Creating mosaic for band {band}, {i}/{len(output_bands)}")
            mosaic_da = self.__mosaic_from_data_array(ds[band], recentness=recentness)
            ds_out[band].values = mosaic_da[band].values
            if recentness:
                if recentness == 1:
                    ds_out["recentness"] = mosaic_da[f"{band}_recentness"]
                    LOGGER.info("Generated recentness array once")
                    recentness = 0
                else:
                    ds_out[f"{band}_recentness"] = mosaic_da[f"{band}_recentness"]
                    LOGGER.info(f"Generated recentness array for band {band}")
            i += 1

        LOGGER.info("Mosaic creation finished")
        ds_out = ds_out.drop_vars(key for key in ds_out.data_vars.keys()
                                  if key not in output_bands and "recentness" not in key)
        return ds_out

    def __setup_mask_datacube(self) -> xa.Dataset:
        """ Creates a datacube with L2A S2 bands and masks from given list """
        mask_dict = {}
        for dataset in self.__mask_datasets:
            LOGGER.debug(f"{type(dataset)}: {dataset}")
            mask_dict[dataset.id] = dataset.metadata_doc["properties"]["l2a_dataset_id"]

        mask_dataset_ids = list(mask_dict.keys())
        l2a_dataset_ids = list(mask_dict.values())

        ds_l2a = dataset_from_odcdataset("s2a_sen2cor_granule", ids=l2a_dataset_ids)
        ds_mask = dataset_from_odcdataset(self.__product_name, ids=mask_dataset_ids)
        return ds_l2a.merge(ds_mask)

    @staticmethod
    def __mosaic_from_data_array(da_in: xa.DataArray, recentness: int = 0) -> xa.Dataset:
        """ Creates a most-recent-to-oldest mosaic of the input dataset.
            da_in: A xa.DataArray retrieved from the Data Cube; should contain:
            coordinates: time, latitude, longitude """
        da_in = da_in.copy(deep=True)
        da_out = da_in.isel(time=-1).copy(deep=True)  # TODO: check if .drop("time" is needed
        out_arr = da_out.values
        recentness_arr = None
        cols, rows = da_out.sizes['x'], da_out.sizes['y']
        if recentness:
            latest_time = da_in.time[-1].values.astype('datetime64[D]').astype('uint16')
            recentness_arr = np.empty((rows, cols), dtype=np.uint16)
            recentness_arr[:] = latest_time

        for index in range(len(da_in.time) - 2, -1, -1):
            da_slice = da_in.isel(time=index)  # TODO: check if .drop("time") needed
            if recentness:
                da_slice_time = da_in.time[index].values.astype('datetime64[D]').astype('uint16')
                recentness_arr[out_arr == 0] = da_slice_time
            out_arr[out_arr == 0] = da_slice.values[out_arr == 0]

        da_out.values = out_arr
        if recentness:
            recentness_data = da_out.copy(deep=True).rename(f"{da_out.name}_recentness")
            recentness_data.values = recentness_arr
            return xa.merge([da_out, recentness_data])

        return da_out.to_dataset()

    def write_mosaic_to_file(self, mosaic_ds: xa.Dataset) -> Path:
        """ Creates a new mosaic from a list of S2Cloudless mask ODC Datasets """
        filepath = self.__generate_mosaic_output_path()

        LOGGER.info("Constructing mosaic array")
        data: List[np.ndarray] = [np.squeeze(mosaic_ds[band].values)
                                  for band in mosaic_ds.data_vars]
        geo_transform, projection = gdal_params_for_xadataset(mosaic_ds)

        LOGGER.info(f"Writing mosaic to {filepath}")
        array_to_geotiff(filepath, data, geo_transform, projection, data_type=gdal.GDT_UInt16)
        LOGGER.info(f"Generated mosaic {filepath}")
        return filepath

    def __generate_mosaic_output_path(self) -> Path:
        """ Generates an output Path for a new mosaic """
        # TODO: merge with write_utils.generate_s2_file_output_path?
        base_output_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"])
        mosaic_dir = Path(base_output_path / "mosaics")
        i = 0
        file_path = Path(mosaic_dir / f"{date.today()}_{self.__product_name}_{i}.tif")
        while file_path.exists():
            i += 1
            file_path = Path(mosaic_dir / f"{date.today()}_{self.__product_name}_{i}.tif")
        return file_path
