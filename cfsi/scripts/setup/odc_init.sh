#!/bin/sh

# init datacube database
datacube -v system init

# add products
for product in cfsi/products/*.yaml; do
  datacube product add "$product"
done
