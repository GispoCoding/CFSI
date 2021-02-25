from cfsi.scripts.process.create_mosaics import create_mosaics
from cfsi.scripts.process.create_masks import create_masks


def main():
    """ Generate s2cloudless masks and create cloudless mosaics """
    create_masks()
    create_mosaics()
    exit(0)


if __name__ == "__main__":
    main()
