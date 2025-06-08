# aggregator/temporal_aggregator.py

import os
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

    original_var_name = da.name if da.name else "unknown_var"
    
    # Determine if temporal resampling is needed
    input_freq_str = pd.infer_freq(da['time'].values)
    resampling_needed = True
    if input_freq_str and input_freq_str.upper().startswith(target_freq.upper()):
        logger.debug(f"Input data frequency ({input_freq_str}) matches target ({target_freq}). Skipping temporal resampling.")
        resampling_needed = False
        
    aggregated_da = da
    if resampling_needed:
        logger.debug(f"Temporally resampling '{original_var_name}' to target frequency '{target_freq}'.")
        if target_freq.upper() in ['M', 'Y', 'A', 'AS', 'Q', 'QS']:
            resampler = da.resample(time=target_freq.upper(), label="right", closed="right")
        else:
            resampler = da.resample(time=target_freq.upper(), label="left", closed="left")

        normalized_temporal_agg_method = temporal_agg_method.strip().lower()
        
        if normalized_temporal_agg_method == 'sum':
            aggregated_da = resampler.sum()
        elif normalized_temporal_agg_method == 'mean':
            aggregated_da = resampler.mean()
        elif normalized_temporal_agg_method == 'max':
            aggregated_da = resampler.max()
        elif normalized_temporal_agg_method == 'min':
            aggregated_da = resampler.min()
        elif normalized_temporal_agg_method == 'median':
            aggregated_da = resampler.median()
        else:
            raise ValueError(f"Unsupported temporal aggregation method: '{temporal_agg_method}'.")

    # --- Perform all spatial aggregations ---
    aggregated_tss = {}
    for spatial_method in spatial_agg_methods:
        try:
            # Spatially aggregate the (already temporally aggregated) data
            ts_da = _perform_spatial_aggregation(aggregated_da, spatial_method)
            
            # Convert to DataFrame
            df = ts_da.to_dataframe()

            # Adjust timestamp for 6H native output if no resampling occurred
            if target_freq.upper() == '6H' and not resampling_needed:
                logger.debug(f"Adjusting 6H timestamp for '{original_var_name}' to represent interval start.")
                df.index = df.index - pd.Timedelta(hours=6)

            key = f"{original_var_name}_{spatial_method}"
            df.columns = [key]
            
            if not df.empty:
                aggregated_tss[key] = df
        except Exception as e:
            logger.error(f"Failed spatial aggregation for method '{spatial_method}' on '{original_var_name}': {e}", exc_info=True)
            
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
