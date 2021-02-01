from pathlib import Path
import numpy as np

from cfsi.scripts.index.mosaic_index import MosaicIndexer

time = np.datetime64('2020-09-10T08:47:27.8891464000')

file_path = Path('/output/mosaics/2021-01-21_s2cloudless_0.tif')
eo3_doc = {
    'id': '53c7ad87e0b11920924f3b7d3274ec45',
    '$schema': 'https://schemas.opendatacube.org/dataset',
    'product': {
        'name': 'cloudless_mosaic'
    },
    'crs': 'EPSG:32635',
    'grids': {
        'default': {  # from rio info
            'shape': (10980, 10980),
            'transform': [10.0, 0.0, 499980.0, 0.0, -10.0, 1200000.0, 0.0, 0.0, 1.0]
        }
    },
    'measurements': {
        'B01': {
            'path': 'file:///output/mosaics/2021-01-20_s2cloudless_0.tif',
            'band': 1
        },
        'B02': {
            'path': 'file:///output/mosaics/2021-01-20_s2cloudless_0.tif',
            'band': 2
        },
        'B03': {
            'path': 'file:///output/mosaics/2021-01-20_s2cloudless_0.tif',
            'band': 3
        },
        'B04': {
            'path': 'file:///output/mosaics/2021-01-20_s2cloudless_0.tif',
            'band': 4
        }
    },
    'uri': 'file:///home/mikael/files/cfsi_container_out/mosaics/2021-01-21_s2cloudless_0.tif',
    'properties': {
        'tile_id': '2021-01-21_s2cloudless_0.tif',
        'eo:instrument': 'MSI',
        'eo:platform': 'SENTINEL-2',
        'odc:file_format': 'GeoTIFF',
        'datetime': str(time),
    }
}

MosaicIndexer().add_dataset(eo3_doc)
