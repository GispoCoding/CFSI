import os
from typing import List
from pathlib import Path

L1C_BUCKET = "sentinel-s2-l1c"
L2A_BUCKET = "sentinel-s2-l2a"


def container_path_to_global_path(*file_paths: Path) -> List[Path]:
    """ Translates container paths to global paths based on
    CFSI_CONTAINER_OUTPUT and CFSI_OUTPUT_DIR env variables.
    e.g. /output/tiles/... -> /home/ubuntu/cfsi_output/tiles/... """
    res: List[Path] = []
    container_output_path = os.environ["CFSI_CONTAINER_OUTPUT"]
    external_output_path = os.environ["CFSI_OUTPUT_DIR"]
    for file_path in file_paths:
        file_string = str(file_path)
        protocol = ""
        if file_string.startswith("file://"):  # TODO: more protocols
            protocol = "file://"
            file_string = file_string[len(protocol):]
        if file_string.startswith(container_output_path):
            res.append(Path(protocol + file_string.replace(
                container_output_path, external_output_path)))
        else:
            res.append(file_path)

    return res


def swap_s2_bucket_names(uri: str) -> str:
    """ Swaps L1C <-> L2A bucket names in given uri string """
    if L1C_BUCKET in uri:
        return uri.replace(L1C_BUCKET, L2A_BUCKET)
    elif L2A_BUCKET in uri:
        return uri.replace(L2A_BUCKET, L1C_BUCKET)
    raise ValueError  # TODO: add custom exception
