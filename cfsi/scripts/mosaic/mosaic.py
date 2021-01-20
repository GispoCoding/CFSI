from logging import DEBUG
from typing import List

from datacube.model import Dataset as ODCDataset
import numpy as np
import xarray as xa

from cfsi import config
from cfsi.utils.load_datasets import dataset_from_odcdataset
from cfsi.utils.logger import create_logger

LOGGER = create_logger("s2cloudless_mosaic", level=DEBUG)

# TODO: option to write latest image as reference
# TODO: write output to correct path


def mosaic_from_mask_datasets(mask_datasets: List[ODCDataset]) -> xa.Dataset:
    """ Creates a cloudless mosaic from cloud/shadow mask ODCDatasets
    :param mask_datasets: List of ODCDatasets with cloud_mask, shadow_mask measurements """
    LOGGER.info(f"Creating mosaic dataset from {len(mask_datasets)} masks")
    ds = setup_mask_datacube(mask_datasets)
    ds = ds.where((ds.cloud_mask == 0) & (ds.shadow_mask == 0), 0)

    ds_out: xa.Dataset = ds.copy(deep=True).isel(time=-1)
    recentness: int = config.mosaic.recentness
    output_bands = config.mosaic.output_bands
    i = 1

    for band in output_bands:
        LOGGER.info(f"Creating mosaic for band {band}, {i}/{len(output_bands)}")
        mosaic_da = mosaic_from_data_array(ds[band], recentness=recentness)
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


def setup_mask_datacube(mask_datasets: List[ODCDataset]) -> xa.Dataset:
    """ Creates a datacube with L2A S2 bands and masks from given list """
    mask_dict = {}
    for dataset in mask_datasets:
        LOGGER.debug(f"{type(dataset)}: {dataset}")
        mask_dict[dataset.id] = dataset.metadata_doc["properties"]["l2a_dataset_id"]

    mask_dataset_ids = list(mask_dict.keys())
    l2a_dataset_ids = list(mask_dict.values())

    ds_l2a = dataset_from_odcdataset("s2a_sen2cor_granule", ids=l2a_dataset_ids)
    mask_product_name = mask_datasets[0].type.name
    ds_mask = dataset_from_odcdataset(mask_product_name, ids=mask_dataset_ids)
    return ds_l2a.merge(ds_mask)


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
