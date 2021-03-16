#!/bin/sh

# init datacube database
datacube -v system init

# add products
datacube product add cfsi/products/s2_granules.yaml
datacube product add cfsi/products/s2cloudless_masks.yaml
datacube product add cfsi/products/fmask_masks.yaml
datacube product add cfsi/products/cloudless_mosaic.yaml
