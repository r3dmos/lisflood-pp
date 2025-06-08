# aggregator/__main__.py

import os
import sys
import yaml
from os import path
import argparse
import logging
import multiprocessing
import datetime
from glob import glob
import subprocess
import xarray as xr

# --- Setup Logger ---
logger = logging.getLogger(__name__)

# Import modules from your package
from aggregator.config import (
    DEFAULT_AGGREGATION_SETTINGS, POSTPROCESS_SETTINGS_YAML,
    TIMESTEP_SECONDS_TO_HOURS
)
from aggregator.utils import get_lisflood_settings, create_output_dirs
from aggregator.data_loader import load_lisflood_variable_data, get_lisflood_output_files_and_vars
from aggregator.temporal_aggregator import aggregate_data_to_timeseries, aggregate_data_to_netcdf
from aggregator.output_writer import save_timeseries_to_csv, save_netcdf_data

def process_variable(task_args):
    """
    Worker function to process a single LISFLOOD variable file.
    This is the core logic that will be executed for each file, potentially in parallel.
    It includes logic to resume from partially completed aggregations.
    
    Args:
        task_args (tuple): A tuple containing all necessary arguments:
            (lisf_var, nc_file_path, config)
    """
    # Unpack all arguments passed to the worker process
    lisf_var, nc_file_path, config = task_args
    worker_logger = logging.getLogger(f"worker.{lisf_var}")

    worker_logger.info(f"--- Starting processing for variable: {lisf_var} ---")
            
    da_original = load_lisflood_variable_data(nc_file_path)
    if da_original is None:
        worker_logger.warning(f"Could not load data for {nc_file_path}. Skipping variable.")
        return
    
    internal_var_name = da_original.name
    if not internal_var_name:
        worker_logger.warning(f"Could not determine internal variable name from {nc_file_path}. Skipping.")
        return

    # Unpack the configuration dictionary for easier access
    fluxes_vars = config['fluxes_vars']
    fluxes_agg_type = config['fluxes_agg_type']
    states_vars = config['states_vars']
    states_agg_type = config['states_agg_type']
    discharge_vars = config['discharge_vars']
    area_map = config.get('area_map')
    input_timestep_hours = config['input_timestep_hours']
    spatial_agg_methods = config['spatial_agg_methods_for_timeseries']
    save_agg_nc_maps = config['save_agg_nc_maps']
    ts_output_dirs = config['ts_output_dirs']
    nc_output_dirs = config['nc_output_dirs']
    overwrite = config['overwrite']

    # --- SPECIAL DISCHARGE HANDLING ---
    if lisf_var in discharge_vars:
        if area_map is None:
            worker_logger.warning(f"Skipping discharge variable '{lisf_var}' because area map is not available.")
            return

        worker_logger.info(f"Applying special discharge conversion for '{lisf_var}'")
        
        # PATH 1: Convert to depth in mm and save as _mm.nc
        logger.info("  PATH 1: Converting discharge to depth (mm)")
        if input_timestep_hours == 6:
            da_daily_rate_for_mm = da_original.resample(time='D', label='left', closed='left').mean()
        else:
            da_daily_rate_for_mm = da_original
        area_map_aligned, _ = xr.align(area_map, da_daily_rate_for_mm, join="right")
        da_daily_mm = (da_daily_rate_for_mm * 86400 * 1000) / area_map_aligned
        da_daily_mm.attrs['units'] = 'mm/day'; da_daily_mm.name = f"{internal_var_name}_mm"
        if save_agg_nc_maps: save_netcdf_data(da_daily_mm, nc_output_dirs['D'], filename=f"{internal_var_name}_mm.nc")

        da_monthly_mm = da_daily_mm.resample(time='M', label='right', closed='right').sum()
        da_monthly_mm.attrs['units'] = 'mm/month'; da_monthly_mm.name = f"{internal_var_name}_mm"
        if save_agg_nc_maps: save_netcdf_data(da_monthly_mm, nc_output_dirs['M'], filename=f"{internal_var_name}_mm.nc")

        da_yearly_mm = da_monthly_mm.resample(time='Y', label='right', closed='right').sum()
        da_yearly_mm.attrs['units'] = 'mm/year'; da_yearly_mm.name = f"{internal_var_name}_mm"
        if save_agg_nc_maps: save_netcdf_data(da_yearly_mm, nc_output_dirs['Y'], filename=f"{internal_var_name}_mm.nc")

        # PATH 2: Aggregate rate in m3/s and save as .nc
        logger.info("  PATH 2: Aggregating discharge rate (m3/s)")
        da_current_rate = da_original
        if input_timestep_hours == 6:
            da_daily_rate = da_current_rate.resample(time='D', label='left', closed='left').mean()
            if save_agg_nc_maps: save_netcdf_data(da_daily_rate, nc_output_dirs['D'], filename=f"{internal_var_name}.nc")
            da_current_rate = da_daily_rate
        da_monthly_rate = da_current_rate.resample(time='M', label='right', closed='right').mean()
        if save_agg_nc_maps: save_netcdf_data(da_monthly_rate, nc_output_dirs['M'], filename=f"{internal_var_name}.nc")
        da_yearly_rate = da_monthly_rate.resample(time='Y', label='right', closed='right').mean()
        if save_agg_nc_maps: save_netcdf_data(da_yearly_rate, nc_output_dirs['Y'], filename=f"{internal_var_name}.nc")

        worker_logger.info(f"Finished processing discharge variable '{lisf_var}'.")
        return

    # --- STANDARD PROCESSING FOR FLUX AND STATE VARIABLES ---
    temporal_agg_method = None
    if lisf_var in fluxes_vars:
        temporal_agg_method = fluxes_agg_type
    elif lisf_var in states_vars:
        temporal_agg_method = states_agg_type
    else:
        worker_logger.warning(f"Variable '{lisf_var}' not configured for standard processing. Skipping.")
        return

    worker_logger.info(f"Applying standard aggregation for '{lisf_var}'")
    
    da_current_agg = da_original
    native_freq_code = '6H' if input_timestep_hours == 6 else 'D'
    current_freq_code = native_freq_code
    is_resumed = False
    
    if not overwrite:
        yearly_nc_path = os.path.join(nc_output_dirs.get('Y', ''), f"{internal_var_name}.nc")
        monthly_nc_path = os.path.join(nc_output_dirs.get('M', ''), f"{internal_var_name}.nc")
        daily_nc_path = os.path.join(nc_output_dirs.get('D', ''), f"{internal_var_name}.nc")

        if os.path.exists(yearly_nc_path):
            worker_logger.info(f"Yearly output already exists. Task complete for '{lisf_var}'.")
            return
        elif os.path.exists(monthly_nc_path):
            worker_logger.info(f"Resuming from existing monthly data: {monthly_nc_path}")
            da_current_agg = load_lisflood_variable_data(monthly_nc_path)
            current_freq_code = 'M'
            is_resumed = True
        elif os.path.exists(daily_nc_path) and input_timestep_hours == 6:
            worker_logger.info(f"Resuming from existing daily data: {daily_nc_path}")
            da_current_agg = load_lisflood_variable_data(daily_nc_path)
            current_freq_code = 'D'
            is_resumed = True
    
    if da_current_agg is None:
        worker_logger.error(f"Failed to load data for {lisf_var} at {current_freq_code}. Cannot proceed.")
        return

    log_prefix = "Resumed" if is_resumed else "Native resolution"
    worker_logger.info(f"        Generating {current_freq_code} time series ({log_prefix})...")
    timeseries_dict = aggregate_data_to_timeseries(
        da_current_agg, target_freq=current_freq_code, 
        spatial_agg_methods=spatial_agg_methods,
        temporal_agg_method=temporal_agg_method 
    )
    save_timeseries_to_csv(timeseries_dict, ts_output_dirs[current_freq_code], lisf_var, current_freq_code)
    
    aggregation_chain = []
    if current_freq_code == '6H': aggregation_chain = [('D', 'Daily'), ('M', 'Monthly'), ('Y', 'Yearly')]
    elif current_freq_code == 'D': aggregation_chain = [('M', 'Monthly'), ('Y', 'Yearly')]
    elif current_freq_code == 'M': aggregation_chain = [('Y', 'Yearly')]
    
    for target_freq, freq_name in aggregation_chain:
        worker_logger.info(f"        Aggregating from {current_freq_code} to {freq_name}...")
        next_da = aggregate_data_to_netcdf(da_current_agg, target_freq, temporal_agg_method)
        if next_da is None:
            worker_logger.warning(f"        Failed to aggregate to {freq_name}. Stopping chain for this variable.")
            break
        da_current_agg = next_da
        current_freq_code = target_freq
        if save_agg_nc_maps:
            save_netcdf_data(da_current_agg, nc_output_dirs[target_freq])
        
        worker_logger.info(f"        Generating {freq_name} time series...")
        timeseries_dict = aggregate_data_to_timeseries(
            da_current_agg, target_freq=target_freq,
            spatial_agg_methods=spatial_agg_methods,
            temporal_agg_method=temporal_agg_method)
        save_timeseries_to_csv(timeseries_dict, ts_output_dirs[target_freq], lisf_var, target_freq)
    
    worker_logger.info(f"Finished processing variable: {lisf_var}")


def main():
    parser = argparse.ArgumentParser(description="Temporally aggregate LISFLOOD NetCDF output data.")
    parser.add_argument('--output_root', type=str, required=True, help="Directory containing LISFLOOD .nc files.")
    parser.add_argument('--lisflood_settings_xml', type=str, required=True, help="Full path to the LISFLOOD XML settings file.")
    parser.add_argument('--num_workers', type=int, default=1, help="Number of parallel workers. Use -1 for all CPUs.")
    parser.add_argument('--maps_path', type=str, default=None, help="Optional: New base path for input maps.")
    parser.add_argument('--vars_to_process', nargs='+', default=None, help="Optional: A space-separated list of variables to process.")
    parser.add_argument('--settings_yaml', type=str, default=POSTPROCESS_SETTINGS_YAML, help=f"Path to aggregation rules YAML.")
    parser.add_argument('--save_agg_nc_maps', action='store_true', help="Save aggregated NetCDF maps.")
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing processed files.")
    parser.add_argument('--convert_tss', action='store_true', help="Convert all .tss files to .csv.")
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Set the logging level.")
    
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel.upper()), format='%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    logger.info("--- Starting LISFLOOD Tool ---")
    
    LISFLOOD_DATA_DIR = args.output_root
    
    settings_lis = get_lisflood_settings(args.lisflood_settings_xml)
    if settings_lis is None: sys.exit(1)
    binding = settings_lis.binding

    if args.maps_path:
        logger.info(f"Overriding map paths with new base directory: {args.maps_path}")
        if not os.path.isdir(args.maps_path):
            logger.error(f"Provided --maps_path is not a valid directory: {args.maps_path}")
            sys.exit(1)
        updated_binding = {}
        for key, value in binding.items():
            if isinstance(value, str) and ('.nc' in value or '.map' in value) and not os.path.isabs(value):
                original_filename = os.path.basename(value)
                new_path = os.path.join(args.maps_path, original_filename)
                updated_binding[key] = new_path
            else:
                updated_binding[key] = value
        binding = updated_binding
        
    user_settings = {}
    try:
        if os.path.exists(args.settings_yaml):
            with open(args.settings_yaml, "r") as stream: user_settings = yaml.safe_load(stream)
            logger.info(f"Loaded aggregation settings from: {args.settings_yaml}")
        else:
            logger.warning(f"Aggregation settings file not found. Using package default.")
            user_settings = DEFAULT_AGGREGATION_SETTINGS
    except yaml.YAMLError as exc:
        logger.error(f"Error parsing YAML: {exc}. Falling back to defaults.")
        user_settings = DEFAULT_AGGREGATION_SETTINGS
        
    processing_config = DEFAULT_AGGREGATION_SETTINGS['time_processing'].copy()
    user_output_config = user_settings.get('time_processing', {}).get('output', {})
    processing_config['output']['fluxes'].update(user_output_config.get('fluxes', {}))
    processing_config['output']['states'].update(user_output_config.get('states', {}))
    if 'discharge' in user_output_config:
        processing_config['output']['discharge'] = user_output_config.get('discharge', {})
    
    fluxes_vars = processing_config['output']['fluxes']['var_name']
    states_vars = processing_config['output']['states']['var_name']
    discharge_vars = processing_config['output'].get('discharge', {}).get('var_name', [])
    fluxes_agg_type = processing_config['output']['fluxes']['aggregation_type']
    states_agg_type = processing_config['output']['states']['aggregation_type']
    
    timestep_sec_str = binding.get('DtSec', None)
    try:
        timestep_sec = int(timestep_sec_str)
    except (ValueError, TypeError):
        logger.error(f"'DtSec' value is invalid. Exiting.")
        sys.exit(1)
        
    input_timestep_hours = TIMESTEP_SECONDS_TO_HOURS.get(timestep_sec, None)
    if input_timestep_hours is None:
        logger.error(f"Unsupported input timestep. Exiting.")
        sys.exit(1)

    lisf_var_paths_map = get_lisflood_output_files_and_vars(LISFLOOD_DATA_DIR, binding)
    if args.vars_to_process:
        logger.info(f"Filtering to process only: {args.vars_to_process}")
        lisf_var_paths_map = {var: p for var, p in lisf_var_paths_map.items() if var in args.vars_to_process}
    if not lisf_var_paths_map:
        logger.info("No files to process.")
        sys.exit(0)

    pp_base_dir = os.path.join(LISFLOOD_DATA_DIR, 'pp')
    create_output_dirs(LISFLOOD_DATA_DIR, ['pp'])
    possible_frequencies = ['6H', 'D', 'M', 'Y']
    ts_output_dirs = {freq: os.path.join(pp_base_dir, f"{freq.lower()}hourly_TS" if freq == '6H' else f"{freq.lower()}ly_TS") for freq in possible_frequencies}
    nc_output_dirs = {freq: os.path.join(pp_base_dir, f"{freq.lower()}ly_NC") for freq in possible_frequencies if freq != '6H'}
    for dir_path in list(ts_output_dirs.values()) + list(nc_output_dirs.values()):
        os.makedirs(dir_path, exist_ok=True)
    
    area_map = None
    if any(v in discharge_vars for v in lisf_var_paths_map.keys()):
        pixel_area_path = binding.get('PixelAreaUser')
        if not pixel_area_path: logger.error("PixelAreaUser not found in settings.")
        else:
            path_to_load = pixel_area_path
            if pixel_area_path.endswith('.map'):
                path_to_load = os.path.splitext(pixel_area_path)[0] + '.nc'
            if os.path.exists(path_to_load):
                area_map = load_lisflood_variable_data(path_to_load)
            else:
                logger.error(f"PixelAreaUser file does not exist at '{path_to_load}'.")
                
    config_for_workers = {
        'fluxes_vars': fluxes_vars, 'fluxes_agg_type': fluxes_agg_type,
        'states_vars': states_vars, 'states_agg_type': states_agg_type,
        'discharge_vars': discharge_vars, 'area_map': area_map,
        'input_timestep_hours': input_timestep_hours,
        'spatial_agg_methods_for_timeseries': ['mean', 'max', 'min', 'median'],
        'save_agg_nc_maps': args.save_agg_nc_maps, 'overwrite': args.overwrite,
        'ts_output_dirs': ts_output_dirs, 'nc_output_dirs': nc_output_dirs
    }
    
    tasks = [(var, path, config_for_workers) for var, path in lisf_var_paths_map.items()]
    
    num_workers = args.num_workers
    if num_workers == -1: num_workers = os.cpu_count() or 1
    logger.info(f"Using {num_workers} parallel worker(s) for {len(tasks)} tasks.")

    if num_workers > 1 and len(tasks) > 1:
        with multiprocessing.Pool(processes=num_workers) as pool:
            pool.map(process_variable, tasks)
    else:
        logger.info("Starting aggregation sequentially...")
        for task in tasks:
            process_variable(task)

    logger.info("\n--- LISFLOOD Processing Complete! ---")

if __name__ == '__main__':
    main()
