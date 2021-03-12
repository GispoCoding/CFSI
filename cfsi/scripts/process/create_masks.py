from cfsi.scripts.masks.fmask_masks import FmaskGenerator
from cfsi.scripts.masks.s2cloudless_masks import S2CloudlessGenerator


def create_masks():
    """ Generate s2cloudless masks """
    # TODO: read product names from config
    FmaskGenerator().create_masks()
    S2CloudlessGenerator().create_masks()


if __name__ == "__main__":
    create_masks()
