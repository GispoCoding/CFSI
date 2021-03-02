from cfsi import config
from cfsi.scripts.index.mosaic_index import MosaicIndexer
from cfsi.scripts.mosaic import MosaicCreator


def create_mosaics():
    """ Creates cloudless mosaics from masks """
    dates = config.mosaic.dates
    days = config.mosaic.range
    products = config.mosaic.products

    for product in products:
        for date_ in dates:
            mosaic_creator = MosaicCreator(product, date_, days)
            mosaic_ds = mosaic_creator.create_mosaic_dataset()
            output_mosaic_path = mosaic_creator.write_mosaic_to_file(mosaic_ds)
            MosaicIndexer().index(mosaic_ds, output_mosaic_path)


if __name__ == "__main__":
    create_mosaics()
