import os
from logging import DEBUG
from pathlib import Path

from datacube.model import Dataset as ODCDataset
from fmask.cmdline import sentinel2Stacked
from sentinelhub import AwsTile, DataCollection, AwsTileRequest

import cfsi
from cfsi.scripts.index.fmask_index import FmaskIndexer
from cfsi.scripts.masks.cloud_mask_generator import CloudMaskGenerator
from cfsi.utils.logger import create_logger
from cfsi.utils.write_utils import get_s2_tile_ids, generate_s2_file_output_path

config = cfsi.config()
LOGGER = create_logger("fmask", level=DEBUG)


class FmaskGenerator(CloudMaskGenerator):

    def __init__(self):
        super().__init__()
        self.max_iterations = config.masks.fmask_masks.max_iterations
        self.mask_product_name = "s2_level1c_fmask"

    def _create_mask(self, l1c_dataset: ODCDataset) -> bool:
        if not config.masks.fmask_masks.generate:
            LOGGER.info("Skipping Fmask mask generation due to config")
            return False
        if not self._should_process(l1c_dataset):
            return True

        LOGGER.info(f"Iteration {self.i}/{self.max_iterations}: {l1c_dataset}")
        output_mask = {"fmask": self.__create_fmask_file(l1c_dataset)}
        self.indexed_masks.append(FmaskIndexer().index_masks(l1c_dataset, output_mask))

        return self._continue_iteration()

    def __create_fmask_file(self, dataset: ODCDataset) -> Path:
        """ Generate Fmask masks for a single datacube dataset """
        tile_id, s3_key = get_s2_tile_ids(dataset)
        safe_tile_path = self.fetch_s2_to_safe(tile_id)

        mask_output_path = generate_s2_file_output_path(dataset, self.mask_product_name)
        fmask_args = ["--granuledir", str(safe_tile_path), "-o", str(mask_output_path), "-v"]

        if not mask_output_path.parent.exists():
            mask_output_path.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.info(f"Generating fmask masks for {s3_key}")
        sentinel2Stacked.mainRoutine(fmask_args)

        self.write_l1c_reference(dataset)

        return mask_output_path

    @staticmethod
    def fetch_s2_to_safe(tile_id: str) -> Path:
        """ Fetches S2 granule by tile ID from AWS S3 to .SAFE format, returns Path of fetched data """
        tile_name, time, aws_index = AwsTile.tile_id_to_tile(tile_id)
        base_output_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"]).joinpath("cache/safe")
        request = AwsTileRequest(tile=tile_name,
                                 time=time,
                                 aws_index=aws_index,
                                 data_folder=base_output_path,
                                 data_collection=DataCollection.SENTINEL2_L1C,
                                 safe_format=True)
        LOGGER.info("Fetching data to .SAFE format")
        request.save_data()
        tile_output_directory = Path(request.get_filename_list()[0]).parts[0]
        return base_output_path.joinpath(tile_output_directory)


if __name__ == "__main__":
    FmaskGenerator().create_masks()
