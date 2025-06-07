# lisflood_temporal_aggregator/data_loader.py

import xarray as xr
import os
from glob import glob
import logging
from aggregator.config import VAR_TO_REMOVE, LAT_DIM_NAMES, LON_DIM_NAMES, VARS_TO_IGNORE
from aggregator.utils import ncname2lisname # Import from your utils module
import numpy as np

# Setup logger for this module
logger = logging.getLogger(__name__)

def load_lisflood_variable_data(file_path):
    """
    Loads a single LISFLOOD NetCDF file (or a pattern of files) into an xarray DataArray.
    It attempts to guess the main data variable and removes common metadata variables.
    Standardizes 'lat' and 'lon' dimension names.

    Args:
        file_path (str): Full path to the NetCDF file.

    Returns:
        xarray.DataArray or None: The loaded data array, or None if it fails.
    """
    actual_path = file_path
    if isinstance(file_path, str) and not file_path.endswith('.nc') and '*' not in file_path:
        actual_path = file_path + '.nc'

    try:
        ds = xr.open_mfdataset(actual_path, decode_times=True, chunks='auto')
    except Exception as e:
        logger.error(f"Error opening NetCDF file(s) {actual_path}: {e}")
        return None

    data_vars = [var for var in ds.data_vars if var not in VAR_TO_REMOVE]

    if not data_vars:
        logger.warning(f"No suitable data variable found in {actual_path} after filtering. Found: {list(ds.data_vars)}")
        return None

    da = ds[data_vars[0]]

    current_lat_dim = next((dim for dim in LAT_DIM_NAMES if dim in da.dims), None)
    current_lon_dim = next((dim for dim in LON_DIM_NAMES if dim in da.dims), None)

    if current_lat_dim and current_lat_dim != 'lat':
        da = da.rename({current_lat_dim: 'lat'})
    if current_lon_dim and current_lon_dim != 'lon':
        da = da.rename({current_lon_dim: 'lon'})

    if 'time' in da.coords and isinstance(da['time'].values[0], np.datetime64):
        if hasattr(da['time'].dt, 'tz') and da['time'].dt.tz is not None:
             da['time'] = da['time'].dt.tz_convert(None)

    return da

def get_lisflood_output_files_and_vars(output_dir, binding_lisflood, **kwargs):
    """
    Discovers NetCDF output files and maps them to their LISFLOOD variable names.
    The logic to skip already processed files is now handled downstream in the
    main processing function for more granular resume capabilities.

    Args:
        output_dir (str): Path to the directory containing LISFLOOD .nc files.
        binding_lisflood (dict): The `binding` dictionary from a LisSettings object.
        **kwargs: Catches unused arguments like 'skip_if_output_exists_in' for compatibility.

    Returns:
        dict: A dictionary of all processable LISFLOOD variables and their file paths.
    """
    if 'skip_if_output_exists_in' in kwargs and kwargs['skip_if_output_exists_in'] is not None:
        logger.debug("'skip_if_output_exists_in' is handled by the main worker process, not in file discovery.")

    nc_file_paths_found = glob(os.path.join(output_dir, '*.nc'))
    lisf_var_paths_map = {}

    for fpath in nc_file_paths_found:
        base_name_no_ext = os.path.basename(fpath)[:-3]
        lisf_var = ncname2lisname(base_name_no_ext, binding_lisflood)
        
        # This function now only filters based on the VARS_TO_IGNORE list.
        # The decision to skip or resume is made later.
        if lisf_var and lisf_var not in VARS_TO_IGNORE:
            lisf_var_paths_map[lisf_var] = fpath
        elif lisf_var:
            logger.debug(f"Ignoring variable '{lisf_var}' as it is in VARS_TO_IGNORE list.")

    return lisf_var_paths_map
