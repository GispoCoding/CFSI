import os
from datetime import datetime, date, timedelta
from logging import DEBUG
from pathlib import Path
from typing import List, Dict
from uuid import UUID

from datacube import Datacube
from datacube.model import Dataset as ODCDataset
import numpy as np
import rasterio as rio
import xarray as xa

import cfsi
from cfsi.exceptions import ProductNotFoundException
from cfsi.utils.load_datasets import xadataset_from_odcdataset, odcdataset_from_uri
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import array_to_geotiff, rio_params_for_xadataset, create_overviews

LOGGER = create_logger("mosaic", level=DEBUG)

# TODO: option to write latest image as reference
config = cfsi.config()


class MosaicCreator:

    def __init__(self,
                 mask_product_name: str,
                 date_: str = "today",
                 days: int = 30):
        """ Constructor method """
        self.__product_name = mask_product_name
        if date_ == "today":
            self.__end_date = date.today()
        else:
            self.__end_date = datetime.strptime(date_, "%Y-%m-%d").date()
        self.__start_date = self.__end_date - timedelta(days=days)
        self.__mask_datasets = self.__get_mask_datasets()

    def __get_mask_datasets(self) -> List[ODCDataset]:
        """ Finds mask datasets based on config """
        dc = Datacube(app="mosaic_creator")
        time_range = (str(self.__start_date), str(self.__end_date))
        datasets = dc.find_datasets(product=self.__product_name, time=time_range)
        if not datasets:
            LOGGER.warning("No mask datasets found for"
                           f"product={self.__product_name}, time={time_range}")
            raise ValueError("No datasets found")  # TODO: custom exception
        return datasets

    def create_mosaic_dataset(self) -> xa.Dataset:
        """ Creates a cloudless mosaic """
        LOGGER.info(f"Creating {self.__product_name} mosaic dataset "
                    f"from {len(self.__mask_datasets)} masks "
                    f"from {self.__start_date} to {self.__end_date}")
        ds = self.__setup_mask_datacube()
        ds = self.__apply_mask(ds)

        ds_out: xa.Dataset = ds.copy(deep=True).isel(time=-1)
        recentness: int = config.mosaic.recentness
        output_bands = config.mosaic.output_bands
        i = 1

        for band in output_bands:
            LOGGER.info(f"Creating {self.__product_name} mosaic for band {band}, {i}/{len(output_bands)}")
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
        """ Creates a datacube with L2A S2 bands and cloud masks """
        mask_dict = self.__generate_mask_dict()
        mask_dataset_ids = list(mask_dict.keys())
        l2a_dataset_ids = list(mask_dict.values())

        ds_l2a = xadataset_from_odcdataset(ids=l2a_dataset_ids)
        ds_mask = xadataset_from_odcdataset(ids=mask_dataset_ids)
        return ds_l2a.merge(ds_mask)

    def __generate_mask_dict(self) -> Dict[UUID, UUID]:
        """ Generates a dict of mask_dataset.id: l2a_dataset.id """
        mask_dict = {}
        for mask_dataset in self.__mask_datasets:
            l2a_dataset_id = mask_dataset.metadata_doc["properties"]["l2a_dataset_id"]
            if not l2a_dataset_id:
                try:
                    LOGGER.info("L2A dataset id not provided, searching using URI")
                    l2a_uri = mask_dataset.metadata_doc["properties"]["l2a_uri"]
                    l2a_dataset_id = odcdataset_from_uri(l2a_uri, "s2_sen2cor_granule").id
                except ProductNotFoundException:
                    LOGGER.warning(f"L2A dataset not in index, skipping mask {mask_dataset}")
                    continue
            mask_dict[mask_dataset.id] = l2a_dataset_id
        return mask_dict

    def __apply_mask(self, ds: xa.Dataset) -> xa.Dataset:
        """ """
        if self.__product_name == "s2_level1c_s2cloudless":
            return ds.where((ds.cloud_mask == 0) & (ds.shadow_mask == 0), 0)
        elif self.__product_name == "s2_level1c_fmask":
            return ds.where((ds.fmask == 1) | (ds.fmask == 4) | (ds.fmask == 5), 0)
        raise ValueError("Invalid mask product name")  # TODO: custom exception

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
        LOGGER.info(f"Mosaic array shape (x, y): {cols, rows}")
        total_pixels = f"{round((cols * rows) / 1000000)}Mp"

        if recentness:
            latest_time = da_in.time[-1].values.astype('datetime64[D]').astype('uint16')
            recentness_arr = np.empty((rows, cols), dtype=np.uint16)
            recentness_arr[:] = latest_time

        for index in range(len(da_in.time) - 2, -1, -1):
            nodata_pixels = np.count_nonzero(out_arr == 0)
            if nodata_pixels > 1000000:
                nodata_pixels = str(round(nodata_pixels/1000000)) + 'Mp'
            else:
                nodata_pixels = str(nodata_pixels) + 'p'
            LOGGER.info(f"Index {index}/{len(da_in.time) - 2}; "
                        "Nodata pixels remaining: "
                        f"{nodata_pixels}/{total_pixels}")

            da_slice = da_in.isel(time=index)
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
        file_path = self.__generate_mosaic_output_path()

        LOGGER.info("Constructing mosaic array")
        data: List[np.ndarray] = [np.squeeze(mosaic_ds[band].values)
                                  for band in mosaic_ds.data_vars]
        geo_transform, projection = rio_params_for_xadataset(mosaic_ds)

        LOGGER.info(f"Writing mosaic to {file_path}")
        array_to_geotiff(file_path, data, geo_transform, projection,
                         compress="none", data_type=rio.uint16)
        create_overviews(file_path)
        LOGGER.info(f"Generated mosaic {file_path}")
        return file_path

    def __generate_mosaic_output_path(self) -> Path:
        """ Generates an output Path for a new mosaic """
        base_output_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"])
        mosaic_dir = Path(base_output_path / "mosaics")
        i = 0
        file_path = Path(mosaic_dir / f"{self.__end_date}_{self.__product_name}_{i}.tif")
        while file_path.exists():
            i += 1
            file_path = Path(mosaic_dir / f"{self.__end_date}_{self.__product_name}_{i}.tif")
        return file_path
