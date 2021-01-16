#!/bin/sh

until psql "$DATACUBE_DB_URL" -c "SELECT version()" > /dev/null 2>&1
do
  sleep 1
done

datacube -v system init
datacube product add cfsi/products/s2_granules.yaml
datacube product add cfsi/products/s2cloudless_masks.yaml
