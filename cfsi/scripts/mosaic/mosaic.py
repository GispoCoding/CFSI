from logging import DEBUG
from typing import List
from uuid import UUID

from datacube.model import Dataset as ODCDataset
import numpy as np
import xarray as xa

from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger

LOGGER = create_logger("s2cloudless_mosaic", level=DEBUG)

OUTPUT_BANDS = ["B02_10m", "B03_10m", "B04_10m"]
RECENTNESS = True


def mosaic_from_data_array(da_in: xa.DataArray, recentness: int = 0) -> xa.Dataset:
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


def mosaic_from_mask_datasets(mask_datasets: List[ODCDataset]) -> xa.Dataset:
    """ Creates a cloudless mosaic from cloud/shadow mask ODCDatasets
    :param mask_datasets: List of ODCDatasets with cloud_mask, shadow_mask measurements """
    mask_dict = {}
    for dataset in mask_datasets:
        mask_dict[dataset.id] = UUID(dataset.metadata_doc["properties"]["l2a_dataset_id"])

    mask_dataset_ids = list(mask_dict.keys())
    l2a_dataset_ids = list(mask_dict.values())

    ds_l2a = dataset_from_odcdataset("s2a_sen2cor_granule", ids=l2a_dataset_ids)
    mask_product_name = mask_datasets[0].type.name
    ds_mask = dataset_from_odcdataset(mask_product_name, ids=mask_dataset_ids)
    ds_merged = ds_l2a.merge(ds_mask)
    ds_merged = ds_merged.where((ds_merged.cloud_mask == 0) & (ds_merged.shadow_mask == 0), 0)

    recentness: int = 1  # 0: don't check, 1: check once, 2: check for every band

    ds_out: xa.Dataset = ds_merged.copy(deep=True).isel(time=-1)
    for band in OUTPUT_BANDS:
        mosaic_da = mosaic_from_data_array(ds_merged[band], recentness=recentness)
        ds_out[band].values = mosaic_da[band].values
        if recentness:
            if recentness == 1:
                ds_out["recentness"] = mosaic_da[f"{band}_recentness"]
                recentness = 0
            else:
                ds_out[f"{band}_recentness"] = mosaic_da[f"{band}_recentness"]

    ds_out = ds_out.drop_vars(key for key in ds_out.data_vars.keys()
                              if key not in OUTPUT_BANDS and "recentness" not in key)
    return ds_out
