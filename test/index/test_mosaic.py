from datacube import Datacube

from cfsi.scripts.mosaic.mosaic import mosaic_from_mask_datasets


dc = Datacube(app="test_mosaic")
query = dict(product="s2a_level1c_s2cloudless")
mask_datasets = [dataset for dataset in dc.index.datasets.search(**query)]

mosaic_from_mask_datasets(mask_datasets)
