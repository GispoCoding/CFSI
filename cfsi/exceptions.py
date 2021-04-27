
class CFSIException(Exception):
    """ Base CFSI exception class """
    pass


class ProductNotFoundException(CFSIException):
    """ Raised when product not found in index """
    pass
