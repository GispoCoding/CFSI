def create_cfmask_clean_mask(cfmask):
    """
    Description:
      Create a clean mask for clear land/water pixels,
      i.e. mask out shadow, snow, cloud, and no data
    -----
    Input:
      cfmask (xarray) - cf_mask from the ledaps products
    Output:
      clean_mask (boolean numpy array) - clear land/water mask
    """

    #########################
    # cfmask values:        #
    #   0 - clear           #
    #   1 - water           #
    #   2 - cloud shadow    #
    #   3 - snow            #
    #   4 - cloud           #
    #   255 - fill          #
    #########################

    clean_mask = (cfmask == 0) | (cfmask == 1)
    return clean_mask.values

def create_default_clean_mask(dataset_in):
    """
    Description:
        Creates a data mask that masks nothing.
    -----
    Inputs:
        dataset_in (xarray.Dataset) - dataset retrieved from the Data Cube.
    Throws:
        ValueError - if dataset_in is an empty xarray.Dataset.
    """
    data_vars = dataset_in.data_vars
    if len(data_vars) != 0:
        first_data_var = next(iter(data_vars))
        clean_mask = np.ones(dataset_in[first_data_var].shape).astype(np.bool)
        return clean_mask
    else:
        raise ValueError('`dataset_in` has no data!')

