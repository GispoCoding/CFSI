from pathlib import Path
from datacube import Datacube

from cfsi.scripts.index.index import ODCIndexer
from cfsi.scripts.index.s2cloudless_index import S2CloudlessIndexer


dc = Datacube(app="test_s2cloudless_index")

dataset_id = ODCIndexer().odcdataset_id_from_uri("s3://sentinel-s2-l1c/tiles/35/P/PM/2020/10/12/0",
                                                 product="s2a_level1c_granule")
dataset = dc.index.datasets.get(dataset_id)
print(dataset.uris[0])
base_path = Path("/home/mikael/files/cfsi_out/tiles/35/P/PM/2020/10/12/0/s2cloudless")
fake_input = {
    dataset: {
        "cloud_mask": base_path.joinpath("S2A_OPER_MSI_L1C_TL_EPAE_20201012T095618_A027717_T35PPM_N02.09_clouds.tif"),
        "shadow_mask": base_path.joinpath("S2A_OPER_MSI_L1C_TL_EPAE_20201012T095618_A027717_T35PPM_N02.09_shadows.tif"),
    }
}
indexer = S2CloudlessIndexer()
indexer.index(fake_input)
print("done")
