from logging import DEBUG
from pathlib import Path

from datacube.model import Dataset as ODCDataset
from fmask.cmdline import sentinel2Stacked

import cfsi
from cfsi.scripts.index.fmask_index import FmaskIndexer
from cfsi.scripts.masks.cloud_mask_generator import CloudMaskGenerator
from cfsi.utils.logger import create_logger
from cfsi.utils import get_s2_tile_ids, generate_s2_tif_path

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

    def __create_fmask_file(self, l1c_dataset: ODCDataset) -> Path:
        """ Generate Fmask masks for a single datacube dataset """
        tile_id, s3_key = get_s2_tile_ids(l1c_dataset)
        safe_tile_path = self.fetch_s2_to_safe(tile_id)

        mask_output_path = generate_s2_tif_path(l1c_dataset, self.mask_product_name)
        fmask_args = ["--granuledir", str(safe_tile_path), "-o", str(mask_output_path), "-v"]

        if not mask_output_path.parent.exists():
            mask_output_path.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.info(f"Generating fmask masks for {s3_key}")
        sentinel2Stacked.mainRoutine(fmask_args)

        self.write_l1c_reference(l1c_dataset)

        return mask_output_path


if __name__ == "__main__":
    FmaskGenerator().create_masks()
