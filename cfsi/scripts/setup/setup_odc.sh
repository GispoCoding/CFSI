#!/bin/sh

# init datacube database
datacube -v system init

# add products
datacube product add cfsi/products/s2_granules.yaml
datacube product add cfsi/products/s2cloudless_masks.yaml

# index s2 scenes
python3 -m cfsi.scripts.index.s2_index
