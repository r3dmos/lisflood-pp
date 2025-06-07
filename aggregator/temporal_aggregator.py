# aggregator/temporal_aggregator.py

import xarray as xr
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def _perform_spatial_aggregation(da, spatial_agg_method='mean'):
    """
    Internal helper function to perform spatial aggregation on an xarray DataArray.
    """
    if 'lat' not in da.dims or 'lon' not in da.dims:
        raise ValueError(f"DataArray must have 'lat' and 'lon' dimensions for spatial aggregation. Found: {da.dims}")

    normalized_spatial_agg_method = spatial_agg_method.strip().lower()

    if normalized_spatial_agg_method == 'mean':
        return da.mean(dim=['lat', 'lon'])
    elif normalized_spatial_agg_method == 'sum':
        return da.sum(dim=['lat', 'lon'])
    elif normalized_spatial_agg_method == 'max':
        return da.max(dim=['lat', 'lon'])
    elif normalized_spatial_agg_method == 'min':
        return da.min(dim=['lat', 'lon'])
    elif normalized_spatial_agg_method == 'median':
        return da.median(dim=['lat', 'lon'])
    else:
        raise ValueError(f"Unsupported spatial aggregation method: '{spatial_agg_method}'.")

def aggregate_data_to_timeseries(da, target_freq, spatial_agg_methods, temporal_agg_method):
    """
    Aggregates a spatially distributed xarray DataArray into one or more time series.
    """
    if da is None:
        logger.warning("Input DataArray is None for time series aggregation. Skipping.")
        return {}

    aggregated_tss = {}
    original_var_name = da.name if da.name else "unknown_var"
    
    input_freq_str = pd.infer_freq(da['time'].values)
    resampling_needed = True

    # If the input data's frequency already matches the target, no need to resample.
    if input_freq_str and input_freq_str.upper().startswith(target_freq.upper()):
        logger.debug(f"Input data frequency ({input_freq_str}) matches target ({target_freq}). Skipping temporal resampling.")
        resampling_needed = False
        
    for spatial_method in spatial_agg_methods:
        try:
            ts_da = _perform_spatial_aggregation(da, spatial_method)
            resampled_ts_df = None
            
            # --- MODIFIED: Use a consistent key and column name format ---
            key_for_dict = f"{original_var_name}_{spatial_method}"

            if not resampling_needed:
                # If no temporal change is needed, just convert the spatially aggregated DataArray.
                resampled_ts_df = ts_da.to_dataframe()
                
                # For 6H native output, adjust timestamp to represent the start of the interval.
                if target_freq.upper() == '6H':
                    logger.debug(f"Adjusting 6H timestamp for '{original_var_name}' to represent interval start.")
                    resampled_ts_df.index = resampled_ts_df.index - pd.Timedelta(hours=6)
            else:
                # Perform temporal resampling
                if target_freq.upper() in ['M', 'Y', 'A', 'AS', 'Q', 'QS']:
                    resampler = ts_da.resample(time=target_freq.upper(), label="right", closed="right")
                else:
                    resampler = ts_da.resample(time=target_freq.upper(), label="left", closed="left")

                normalized_temporal_agg_method = temporal_agg_method.strip().lower()
                
                if normalized_temporal_agg_method == 'sum':
                    resampled_ts_df = resampler.sum().to_dataframe()
                elif normalized_temporal_agg_method == 'mean':
                    resampled_ts_df = resampler.mean().to_dataframe()
                elif normalized_temporal_agg_method == 'max':
                    resampled_ts_df = resampler.max().to_dataframe()
                elif normalized_temporal_agg_method == 'min':
                    resampled_ts_df = resampler.min().to_dataframe()
                elif normalized_temporal_agg_method == 'median':
                    resampled_ts_df = resampler.median().to_dataframe()
                else:
                    raise ValueError(f"Unsupported temporal aggregation method: '{temporal_agg_method}'.")

            # Set the column name to match the key
            if resampled_ts_df is not None:
                resampled_ts_df.columns = [key_for_dict]
            
            if resampled_ts_df is not None and not resampled_ts_df.empty:
                aggregated_tss[key_for_dict] = resampled_ts_df

        except Exception as e:
            logger.error(f"Error during aggregation for '{original_var_name}' (spatial: {spatial_method}, target: {target_freq}): {e}", exc_info=True)
            continue
    return aggregated_tss

def aggregate_data_to_netcdf(da, target_freq, temporal_agg_method):
    """
    Aggregates an xarray DataArray to a new temporal frequency, preserving spatial dimensions.
    """
    if da is None:
        logger.warning("Input DataArray is None for NetCDF aggregation. Skipping.")
        return None

    normalized_temporal_agg_method = temporal_agg_method.strip().lower()
    if normalized_temporal_agg_method not in ['sum', 'mean', 'max', 'min', 'median']:
        raise ValueError(f"Unsupported temporal aggregation method: '{temporal_agg_method}'.")

    try:
        if target_freq.upper() in ['M', 'Y', 'A', 'AS', 'Q', 'QS']:
            resampler = da.resample(time=target_freq.upper(), label="right", closed="right")
        else:
            resampler = da.resample(time=target_freq.upper(), label="left", closed="left")
        
        if normalized_temporal_agg_method == 'sum':
            return resampler.sum()
        elif normalized_temporal_agg_method == 'mean':
            return resampler.mean()
        elif normalized_temporal_agg_method == 'max':
            return resampler.max()
        elif normalized_temporal_agg_method == 'min':
            return resampler.min()
        elif normalized_temporal_agg_method == 'median':
            return resampler.median()
            
    except Exception as e:
        logger.error(f"Error during temporal aggregation to NetCDF for '{da.name}' at {target_freq}: {e}", exc_info=True)
        return None
