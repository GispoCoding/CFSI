from cfsi import config
from cfsi.scripts.index.mosaic_index import MosaicIndexer
from cfsi.scripts.masks.s2cloudless_masks import S2CloudlessGenerator
from cfsi.utils.logger import create_logger
from cfsi.scripts.mosaic.mosaic import MosaicCreator


LOGGER = create_logger("s2cloudless_mosaic")
WRITE_RGB = False


def main():
    """ Generate s2cloudless masks and create cloudless mosaics """
    generate_masks()
    create_mosaics()
    exit(0)


def generate_masks():
    """ Generate s2cloudless masks """
    # TODO: read product names from config
    S2CloudlessGenerator().create_masks()


def create_mosaics():
    """ Creates cloudless mosaics from s2cloudless masks """
    dates = config.mosaic.dates
    days = config.mosaic.range
    products = config.mosaic.products

    for product in products:
        for date_ in dates:
            mosaic_creator = MosaicCreator(product, date_, days)
            mosaic_ds = mosaic_creator.create_mosaic_dataset()
            output_mosaic_path = mosaic_creator.write_mosaic_to_file(mosaic_ds)
            LOGGER.info("Indexing output mosaic")
            MosaicIndexer().index(mosaic_ds, output_mosaic_path)


if __name__ == "__main__":
    main()
