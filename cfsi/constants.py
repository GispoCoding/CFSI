""" Stores constant values used in various modules """

GUARDIAN = "GUARDIAN_QUEUE_EMPTY"

L1C_BUCKET = "sentinel-s2-l1c"
L2A_BUCKET = "sentinel-s2-l2a"

L1C_MEASUREMENTS = [
    "B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m",
    "B06_20m", "B07_20m", "B08_10m", "B09_60m", "B8A_20m",
    "B10_60m", "B11_20m", "B12_20m"
]
L2A_MEASUREMENTS = [measurement for measurement
                    in filter(lambda x: x != "B10_60m", ([
                       "B02_20m", "B02_60m", "B03_20m", "B03_60m", "B04_20m",
                       "B04_60m", "B05_60m", "B06_60m", "B07_60m", "B08_20m",
                       "B08_60m", "B8A_60m", "B11_60m", "B12_60m", "SCL_20m"
                       ] + L1C_MEASUREMENTS))]

S2_MEASUREMENTS = {
    L1C_BUCKET: L1C_MEASUREMENTS,
    L2A_BUCKET: L2A_MEASUREMENTS,
}
S2_PRODUCT_NAMES = {
    L1C_BUCKET: "s2_level1c_granule",
    L2A_BUCKET: "s2_sen2cor_granule",
}
