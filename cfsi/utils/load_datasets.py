from typing import List, Union

from uuid import UUID
from datacube import Datacube
from datacube.model import Dataset as ODCDataset
import xarray as xa


def dataset_from_odcdataset(
        datasets: Union[List[ODCDataset], ODCDataset] = None,
        ids: Union[List[UUID], UUID] = None,
        measurements: List[str] = None) -> xa.Dataset:
    """ Loads a xaDataset from ODCDatasets or ODCDataset ids
     :param datasets: ODCDataset(s), optional
     :param ids: ODCDataset id(s), optional
     :param measurements: list of measurements/bands to load, optional
     :return: xa.Dataset containing given ODCDatasets or IDs """

    dc = Datacube(app="dataset_from_ODCDataset")

    if not datasets:
        if not isinstance(ids, list):
            ids = [ids]
        datasets = [dc.index.datasets.get(id_) for id_ in ids]

    if not isinstance(datasets, list):
        datasets = [datasets]

    product_name = datasets[0].metadata_doc["product"]["name"]
    crs = datasets[0].crs
    try:
        xform = datasets[0].metadata_doc["grids"]["default"]["transform"]
        res = (xform[0], xform[4])
    except KeyError:
        res = (10, -10)

    ds = dc.load(product=product_name,
                   dask_chunks={},
                   measurements=measurements,
                   output_crs=str(crs),
                   resolution=res,
                   datasets=datasets)
    return ds
