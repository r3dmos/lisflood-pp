# tests/test_data_loader.py

import pytest
import xarray as xr
import os
import numpy as np
import pandas as pd # Needed for date_range
from pathlib import Path # Use pathlib for robust path handling

# Import functions from your package
from aggregator.data_loader import load_lisflood_variable_data, get_lisflood_output_files_and_vars
from aggregator.utils import ncname2lisname

# --- Setup for mock data (these can also be defined in tests/data/) ---

# Mock LisSettings binding for testing purposes
# This simulates what LisSettings.binding would return for your test files
MOCK_BINDING = {
    'DischargeMaps': '/path/to/output/test_discharge.nc',
    'TotalRunoffMaps': '/path/to/output/test_runoff.nc',
    'WaterDepthState': '/path/to/output/test_water_depth.nc',
    'SomeOtherVarEnd': '/path/to/output/someothervar.end.nc' # Should be filtered out
}

@pytest.fixture(scope="module") # Use module scope if the data is static across tests
def sample_lisflood_nc_data(tmp_path_factory):
    """
    Creates a temporary directory and writes a sample NetCDF file for testing.
    This fixture ensures a clean file for each test run.
    """
    # Create a temporary test data directory within the pytest tmp_path
    test_data_dir = tmp_path_factory.mktemp("test_lisflood_data")
    file_path = test_data_dir / "test_discharge.nc" # Simulate a LISFLOOD output file

    time = pd.date_range("2020-01-01", periods=10, freq="6H") # 6-hourly data
    lat = np.linspace(45, 46, 2)
    lon = np.linspace(8, 9, 2)
    # Simulate some discharge data (e.g., in m3/s or mm)
    data = np.random.rand(len(time), len(lat), len(lon)) * 100 + 10 # Add base to avoid zeros

    ds = xr.Dataset(
        {"Q": (("time", "lat", "lon"), data)}, # Your variable might be named 'Q'
        coords={"time": time, "lat": lat, "lon": lon}
    )
    # Add a 'crs' variable which should be removed by data_loader
    crs_var = xr.DataArray(0, name='crs', attrs={'grid_mapping_name': 'latitude_longitude'})
    ds['crs'] = crs_var

    ds.to_netcdf(file_path)
    return file_path

@pytest.fixture(scope="module")
def sample_lisflood_nc_data_alt_dims(tmp_path_factory):
    """Creates a temporary NetCDF file with alternative dimension names."""
    test_data_dir = tmp_path_factory.mktemp("test_alt_dims_data")
    file_path = test_data_dir / "test_alt_dims_output.nc"
    time = pd.date_range("2020-01-01", periods=5, freq="D")
    y = np.linspace(10, 12, 2)
    x = np.linspace(20, 22, 2)
    data = np.random.rand(len(time), len(y), len(x)) * 50

    ds = xr.Dataset(
        {"some_variable": (("time", "y", "x"), data)},
        coords={"time": time, "y": y, "x": x}
    )
    ds.to_netcdf(file_path)
    return file_path


# --- Test Cases ---

def test_load_lisflood_variable_data_basic(sample_lisflood_nc_data):
    """Test loading a basic NetCDF file with expected dimensions."""
    da = load_lisflood_variable_data(str(sample_lisflood_nc_data))
    assert da is not None
    assert isinstance(da, xr.DataArray)
    assert 'Q' in da.name # Assuming 'Q' is the primary variable name
    assert 'crs' not in da.coords # 'crs' should have been removed
    assert 'lat' in da.dims and 'lon' in da.dims # Dimensions should be standardized
    assert da.shape == (10, 2, 2) # Check expected shape

def test_load_lisflood_variable_data_alt_dims(sample_lisflood_nc_data_alt_dims):
    """Test loading a NetCDF file with alternative lat/lon dimension names."""
    da = load_lisflood_variable_data(str(sample_lisflood_nc_data_alt_dims))
    assert da is not None
    assert 'some_variable' in da.name
    assert 'lat' in da.dims and 'lon' in da.dims # Should have been renamed from 'y', 'x'
    assert 'x' not in da.dims and 'y' not in da.dims # Original dims should be gone

def test_load_lisflood_variable_data_nonexistent_file():
    """Test handling of non-existent file paths."""
    da = load_lisflood_variable_data("nonexistent_file_path.nc")
    assert da is None

def test_get_lisflood_output_files_and_vars_basic(tmp_path):
    """Test discovery and mapping of LISFLOOD output files."""
    # Create mock LISFLOOD output files in a temporary directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # Create dummy files that match the MOCK_BINDING (their base names)
    (output_dir / "test_discharge.nc").touch()
    (output_dir / "test_runoff.nc").touch()
    (output_dir / "test_water_depth.nc").touch()
    (output_dir / "someothervar.end.nc").touch() # Should be filtered out by "End" suffix
    (output_dir / "not_in_binding.nc").touch() # Not in MOCK_BINDING

    lisf_var_paths = get_lisflood_output_files_and_vars(str(output_dir), MOCK_BINDING)
    
    assert 'DischargeMaps' in lisf_var_paths
    assert 'TotalRunoffMaps' in lisf_var_paths
    assert 'WaterDepthState' in lisf_var_paths
    assert 'SomeOtherVarEnd' not in lisf_var_paths # Should be filtered out
    assert 'not_in_binding' not in lisf_var_paths # Should be filtered out

    # Verify the paths are correct
    assert os.path.basename(lisf_var_paths['DischargeMaps']) == 'test_discharge.nc'
    assert len(lisf_var_paths) == 3 # Only the three expected variables should be mapped