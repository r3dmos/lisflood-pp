# tests/test_temporal_aggregator.py

import pytest
import xarray as xr
import numpy as np
import pandas as pd

# Import the functions from your temporal_aggregator module
from aggregator.temporal_aggregator import (
    _perform_spatial_aggregation,
    aggregate_data_to_timeseries,
    aggregate_data_to_netcdf
)

### --- Fixtures for Sample Data --- ###

@pytest.fixture(scope="module")
def sample_6h_data_array():
    """
    Creates a sample 6-hourly xarray DataArray with known values for testing.
    Time: 2 days (8x6h steps)
    Spatial: 2x2 grid
    Values are simple to allow easy manual calculation for aggregation checks.
    """
    times = pd.date_range("2023-01-01T00:00:00", periods=8, freq="6H") # 00:00, 06:00, 12:00, 18:00
    lats = np.array([45.0, 45.5])
    lons = np.array([8.0, 8.5])

    # Create data with some pattern for easier sum/mean verification
    # e.g., increasing values over time
    data = np.arange(1, 33).reshape(len(times), len(lats), len(lons))
    # Reshape:
    # Day 1:  [[1, 2], [3, 4]] -> Sum = 10, Mean = 2.5
    #         [[5, 6], [7, 8]]
    # Day 2:  [[9, 10], [11, 12]]
    #         [[13, 14], [15, 16]]
    # ... and so on
    # Monthly sum of spatial mean for Jan: 2 days * (sum_of_daily_spatial_mean)
    # Day 1: (1+2+3+4)/4 + (5+6+7+8)/4 = 2.5 + 6.5 = 9
    # Day 2: (9+10+11+12)/4 + (13+14+15+16)/4 = 10.5 + 14.5 = 25
    # Total for 2 days (Jan): 9 + 25 = 34
    
    da = xr.DataArray(
        data,
        coords={"time": times, "lat": lats, "lon": lons},
        dims=["time", "lat", "lon"],
        name="test_flux" # Assume a flux variable for sum aggregation
    )
    return da

@pytest.fixture(scope="module")
def sample_daily_data_array():
    """
    Creates a sample daily xarray DataArray with known values for testing.
    Time: 5 days
    Spatial: 2x2 grid
    """
    times = pd.date_range("2023-01-01", periods=5, freq="D")
    lats = np.array([40.0, 40.5])
    lons = np.array([10.0, 10.5])

    data = np.arange(1, 21).reshape(len(times), len(lats), len(lons))
    # Day 1: [[1,2],[3,4]] -> Sum = 10, Mean = 2.5
    # Day 2: [[5,6],[7,8]] -> Sum = 26, Mean = 6.5
    # ...

    da = xr.DataArray(
        data,
        coords={"time": times, "lat": lats, "lon": lons},
        dims=["time", "lat", "lon"],
        name="test_state" # Assume a state variable for mean aggregation
    )
    return da

### --- Tests for _perform_spatial_aggregation (Internal Helper) --- ###

def test_spatial_aggregation_mean(sample_6h_data_array):
    """Test spatial mean aggregation."""
    spatially_agg_da = _perform_spatial_aggregation(sample_6h_data_array, 'mean')
    assert 'lat' not in spatially_agg_da.dims and 'lon' not in spatially_agg_da.dims
    assert 'time' in spatially_agg_da.dims
    # Expected mean for the first time step: (1+2+3+4)/4 = 2.5
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T00:00:00').item(), 2.5)
    # Expected mean for the second time step: (5+6+7+8)/4 = 6.5
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T06:00:00').item(), 6.5)

def test_spatial_aggregation_sum(sample_6h_data_array):
    """Test spatial sum aggregation."""
    spatially_agg_da = _perform_spatial_aggregation(sample_6h_data_array, 'sum')
    # Expected sum for the first time step: 1+2+3+4 = 10
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T00:00:00').item(), 10.0)
    # Expected sum for the second time step: 5+6+7+8 = 26
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T06:00:00').item(), 26.0)

def test_spatial_aggregation_max(sample_6h_data_array):
    """Test spatial max aggregation."""
    spatially_agg_da = _perform_spatial_aggregation(sample_6h_data_array, 'max')
    # Expected max for the first time step: 4
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T00:00:00').item(), 4.0)

def test_spatial_aggregation_min(sample_6h_data_array):
    """Test spatial min aggregation."""
    spatially_agg_da = _perform_spatial_aggregation(sample_6h_data_array, 'min')
    # Expected min for the first time step: 1
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T00:00:00').item(), 1.0)

def test_spatial_aggregation_median(sample_6h_data_array):
    """Test spatial median aggregation."""
    spatially_agg_da = _perform_spatial_aggregation(sample_6h_data_array, 'median')
    # Expected median for the first time step: (2+3)/2 = 2.5 (values: 1,2,3,4)
    assert np.isclose(spatially_agg_da.sel(time='2023-01-01T00:00:00').item(), 2.5)

def test_spatial_aggregation_invalid_method(sample_6h_data_array):
    """Test error handling for invalid spatial aggregation method."""
    with pytest.raises(ValueError, match="Unsupported spatial aggregation method"):
        _perform_spatial_aggregation(sample_6h_data_array, 'unsupported_method')

### --- Tests for aggregate_data_to_timeseries --- ###

def test_6h_to_daily_timeseries_sum(sample_6h_data_array):
    """
    Test aggregation from 6-hourly to daily time series with sum for fluxes.
    Input `sample_6h_data_array` has 8 steps (2 days).
    Daily sum of spatial means for Day 1 (Jan 01): (2.5 + 6.5 + 10.5 + 14.5) = 34
    Daily sum of spatial means for Day 2 (Jan 02): (18.5 + 22.5 + 26.5 + 30.5) = 98
    """
    spatial_agg_methods = ['mean']
    temporal_agg_method = 'sum'
    
    result_dict = aggregate_data_to_timeseries(
        sample_6h_data_array, 'D', spatial_agg_methods, temporal_agg_method
    )
    
    assert len(result_dict) == 1
    key = "test_flux_mean_D_sum"
    assert key in result_dict
    
    ts_df = result_dict[key]
    assert isinstance(ts_df, pd.DataFrame)
    assert len(ts_df) == 2 # 2 days
    assert ts_df.index.freq == 'D' # Check frequency
    
    # Check values
    assert np.isclose(ts_df.loc['2023-01-01'].item(), 34.0)
    assert np.isclose(ts_df.loc['2023-01-02'].item(), 98.0)
    assert ts_df.columns[0] == key # Check column name

def test_6h_to_monthly_timeseries_mean(sample_6h_data_array):
    """
    Test aggregation from 6-hourly to monthly time series with mean for states.
    Input `sample_6h_data_array` has 8 steps (2 days) in Jan 2023.
    Monthly mean of spatial means for Jan 2023: (34 + 98) / 8 = 132 / 8 = 16.5
    """
    spatial_agg_methods = ['mean']
    temporal_agg_method = 'mean'
    
    result_dict = aggregate_data_to_timeseries(
        sample_6h_data_array, 'M', spatial_agg_methods, temporal_agg_method
    )
    
    assert len(result_dict) == 1
    key = "test_flux_mean_M_mean" # Note: fixture is 'test_flux', not 'test_state'
    assert key in result_dict
    
    ts_df = result_dict[key]
    assert isinstance(ts_df, pd.DataFrame)
    assert len(ts_df) == 1 # 1 month
    assert ts_df.index.freq == 'M' # Check frequency
    
    # Check value
    assert np.isclose(ts_df.loc['2023-01-31'].item(), 16.5) # Monthly mean is average of all 8 values
    assert ts_df.columns[0] == key

def test_daily_to_yearly_timeseries_sum(sample_daily_data_array):
    """
    Test aggregation from daily to yearly time series with sum for fluxes.
    Input `sample_daily_data_array` has 5 days in Jan 2023.
    Yearly sum of spatial means for 2023: (2.5 + 6.5 + 10.5 + 14.5 + 18.5) = 52.5
    """
    sample_daily_data_array.name = "test_flux_daily" # Rename to act as flux for this test
    spatial_agg_methods = ['mean']
    temporal_agg_method = 'sum'
    
    result_dict = aggregate_data_to_timeseries(
        sample_daily_data_array, 'Y', spatial_agg_methods, temporal_agg_method
    )
    
    assert len(result_dict) == 1
    key = "test_flux_daily_mean_Y_sum"
    assert key in result_dict
    
    ts_df = result_dict[key]
    assert isinstance(ts_df, pd.DataFrame)
    assert len(ts_df) == 1 # 1 year
    assert ts_df.index.freq == 'A-DEC' # Check frequency for annual end of year
    
    # Check value
    assert np.isclose(ts_df.loc['2023-12-31'].item(), 52.5)
    assert ts_df.columns[0] == key

def test_6h_to_6h_timeseries_mean(sample_6h_data_array):
    """
    Test aggregation from 6-hourly input to 6-hourly time series (only spatial aggregation).
    """
    spatial_agg_methods = ['mean']
    temporal_agg_method = 'mean' # Or 'sum', it won't matter much for same-frequency output
    
    result_dict = aggregate_data_to_timeseries(
        sample_6h_data_array, '6H', spatial_agg_methods, temporal_agg_method
    )
    
    assert len(result_dict) == 1
    key = "test_flux_mean" # For '6H' to '6H', the column name is simplified
    assert key in result_dict.keys()
    
    ts_df = result_dict[key]
    assert isinstance(ts_df, pd.DataFrame)
    assert len(ts_df) == 8 # Original 8 steps
    assert ts_df.index.freq == '6H' # Frequency should be preserved

    # Check values for first two 6h steps
    assert np.isclose(ts_df.loc['2023-01-01T00:00:00'].item(), 2.5) # Mean of 1,2,3,4
    assert np.isclose(ts_df.loc['2023-01-01T06:00:00'].item(), 6.5) # Mean of 5,6,7,8

def test_aggregate_timeseries_multiple_spatial_methods(sample_daily_data_array):
    """
    Test outputting multiple spatial aggregation methods in one call.
    """
    original_name = sample_daily_data_array.name # Store original if needed elsewhere
    sample_daily_data_array.name = "test_state" # Ensure fixture name is as expected for this test
 
    spatial_agg_methods = ['mean', 'max', 'min']
    temporal_agg_method = 'mean'
    
    result_dict = aggregate_data_to_timeseries(
        sample_daily_data_array, 'M', spatial_agg_methods, temporal_agg_method
    )
    
    assert len(result_dict) == 3 # For mean, max, min
    assert "test_state_mean_M_mean" in result_dict
    assert "test_state_max_M_mean" in result_dict
    assert "test_state_min_M_mean" in result_dict

    # Check monthly mean of max for Jan 2023 (max of each day, then mean of those maxes)
    # Day 1 max: 4, Day 2 max: 8, Day 3 max: 12, Day 4 max: 16, Day 5 max: 20
    # Monthly mean of maxes: (4+8+12+16+20)/5 = 60/5 = 12
    assert np.isclose(result_dict["test_state_max_M_mean"].loc['2023-01-31'].item(), 12.0)


def test_aggregate_timeseries_none_input():
    """Test behavior with None as input DataArray."""
    result = aggregate_data_to_timeseries(None, 'D', ['mean'], 'mean')
    assert result == {}

### --- Tests for aggregate_data_to_netcdf --- ###

def test_daily_to_monthly_netcdf_mean(sample_daily_data_array):
    """
    Test aggregation from daily to monthly NetCDF (spatial maps preserved).
    """
    monthly_da = aggregate_data_to_netcdf(sample_daily_data_array, 'M', 'mean')
    
    assert monthly_da is not None
    assert isinstance(monthly_da, xr.DataArray)
    assert 'time' in monthly_da.dims and 'lat' in monthly_da.dims and 'lon' in monthly_da.dims
    assert len(monthly_da['time']) == 1 # One month
    assert monthly_da['time'].dt.month.item() == 1 # January
    assert monthly_da['time'].dt.year.item() == 2023

    # Check a specific aggregated value (mean of first 5 days for a specific pixel)
    # Pixel (lat=40.0, lon=10.0) values: 1, 5, 9, 13, 17
    # Mean: (1+5+9+13+17)/5 = 45/5 = 9.0
    assert np.isclose(monthly_da.sel(time='2023-01-31', lat=40.0, lon=10.0).item(), 9.0)


def test_6h_to_daily_netcdf_sum(sample_6h_data_array):
    """
    Test aggregation from 6-hourly to daily NetCDF (spatial maps preserved), sum for fluxes.
    """
    daily_da = aggregate_data_to_netcdf(sample_6h_data_array, 'D', 'sum')

    assert daily_da is not None
    assert isinstance(daily_da, xr.DataArray)
    assert 'time' in daily_da.dims and 'lat' in daily_da.dims and 'lon' in daily_da.dims
    assert len(daily_da['time']) == 2 # Two days
    assert daily_da['time'].dt.day.values[0] == 1
    assert daily_da['time'].dt.day.values[1] == 2

    # Check a specific aggregated value (sum of 6-hourly values for Day 1, pixel (45.0, 8.0))
    # Values for pixel (45.0, 8.0) on Day 1 (00h, 06h, 12h, 18h): 1, 5, 9, 13
    # Sum: 1+5+9+13 = 28
    assert np.isclose(daily_da.sel(time='2023-01-01', lat=45.0, lon=8.0).item(), 28.0)

def test_aggregate_netcdf_none_input():
    """Test behavior with None as input DataArray."""
    result = aggregate_data_to_netcdf(None, 'D', 'mean')
    assert result is None

def test_aggregate_netcdf_invalid_method(sample_daily_data_array):
    """Test error handling for invalid temporal aggregation method."""
    with pytest.raises(ValueError, match="Unsupported temporal aggregation method"):
        aggregate_data_to_netcdf(sample_daily_data_array, 'M', 'unsupported_method')