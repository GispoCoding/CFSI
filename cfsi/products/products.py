from typing import List, Optional, Union


class Product:
    """ Base CFSI products class """

    def __init__(self):
        """ Setup products """
        self.__name: str = "cfsi-product"
        self.__odc_product_name = "s2a_level1c_granule"
        self.__created_from: Optional[Product] = None

    def get_name(self) -> str:
        return self.__name

    def get_odc_product_name(self) -> str:
        return self.__odc_product_name

    def get_created_from(self) -> Union["Product", None]:
        return self.__created_from


class S2L1C(Product):
    """ Sentinel 2 Level 1C imagery """

    def __init__(self):
        super().__init__()
        self.__product_name = "s2_l1c"
        self.__odc_product_name = "s2a_level1c_granule"


class S2L2A(Product):
    """ Sentinel 2 Level 2A imagery """

    def __init__(self):
        super().__init__()
        self.__product_name = "s2_l2a"
        self.__odc_product_name = "s2_sen2cor_granule"
        self.__created_from = S2L1C


class CFSIProductSet:
    """ Set of CFSI products """

    def __init__(self):
        self.products: List[Product] = []


class S2BaseProducts(CFSIProductSet):
    """ Sentinel 2 L1C and L2A images """

    def __init__(self):
        super().__init__()
        self.products = [S2L1C(), S2L2A()]
