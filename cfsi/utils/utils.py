import os
from typing import List
from pathlib import Path


def container_path_to_global_path(*file_paths: Path) -> List[Path]:
    """ Translates container paths to global paths based on
    CFSI_CONTAINER_OUTPUT and CFSI_OUTPUT_DIR env variables.
    e.g. /output/tiles/... -> /home/ubuntu/cfsi_output/tiles/... """
    res: List[Path] = []
    container_output_path = os.environ["CFSI_CONTAINER_OUTPUT"]
    external_output_path = os.environ["CFSI_OUTPUT_DIR"]
    for file_path in file_paths:
        if str(file_path).startswith(container_output_path):
            res.append(Path(str(file_path).replace(container_output_path, external_output_path)))
        else:
            res.append(file_path)

    return res
