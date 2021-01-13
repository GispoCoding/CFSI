from typing import List, Tuple
import numpy as np
from osgeo import gdal
gdal.UseExceptions()


def array_to_geotiff_multiband(file_name: str,
                               data: List[np.ndarray],
                               geo_transform: Tuple,
                               projection: str,
                               nodata_val=0,
                               data_type=gdal.GDT_Float32):
    """ Create a multiband GeoTIFF file with data from an array.
    file_name : output geotiff file path including extension
    data : list of numpy arrays
    geo_transform : Geotransform for output raster; e.g.
    "(upleft_x, x_size, x_rotation, upleft_y, y_rotation, y_size)"
    projection : WKT projection for output raster
    nodata_val : Value to convert to nodata in the output raster; default 0
    data_type : gdal data_type object, optional
        Optionally set the data_type of the output raster; can be
        useful when exporting an array of float or integer values. """
    driver = gdal.GetDriverByName('GTiff')
    rows, cols = data[0].shape  # Create raster of given size and projection
    dataset = driver.Create(file_name, cols, rows, len(data), data_type)
    dataset.SetGeoTransform(geo_transform)
    dataset.SetProjection(projection)
    for idx, d in enumerate(data):
        band = dataset.GetRasterBand(idx + 1)
        band.WriteArray(d)
        band.SetNoDataValue(nodata_val)
    dataset = None  # Close %%file
