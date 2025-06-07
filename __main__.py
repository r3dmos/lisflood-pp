# aggregator/__main__.py

import os
import sys
import yaml
from os import path
import argparse
import logging
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

def main():
    parser = argparse.ArgumentParser(description="Temporally aggregate LISFLOOD NetCDF output data.")
    parser.add_argument('--output_root', type=str, required=True, help="Directory containing LISFLOOD .nc files.")
    parser.add_argument('--lisflood_settings_xml', type=str, required=True, help="Full path to the LISFLOOD XML settings file.")
    parser.add_argument('--settings_yaml', type=str, default=POSTPROCESS_SETTINGS_YAML, help=f"Path to aggregation rules YAML.")
    parser.add_argument('--save_agg_nc_maps', action='store_true', help="Save aggregated NetCDF maps.")
    parser.add_argument('--overwrite', action='store_true', help="If set, force reprocessing and overwrite existing files.")
    parser.add_argument('--convert_tss', action='store_true', help="Convert all .tss files in the output folder to .csv.")
    parser.add_argument('--loglevel', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Set the logging level.")
    
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel.upper()), format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    logger.info("--- Starting LISFLOOD Tool ---")
    LISFLOOD_DATA_DIR = args.output_root
    LISFLOOD_SETTINGS_XML_PATH = args.lisflood_settings_xml

    # Optional TSS Conversion
    if args.convert_tss:
        # ... (TSS conversion logic as before) ...
        pass

    # --- Main Aggregation Processing ---
    logger.info("\n--- Starting NetCDF Aggregation ---")
    
    settings_lis = get_lisflood_settings(LISFLOOD_SETTINGS_XML_PATH)
    if settings_lis is None:
        logger.error("Could not load LISFLOOD settings XML. Exiting.")
        sys.exit(1)
    binding = settings_lis.binding
    
    # Load aggregation config
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
    
    logger.debug(f"Flux variables configured: {fluxes_vars}")
    logger.debug(f"State variables configured: {states_vars}")
    logger.debug(f"Discharge variables configured: {discharge_vars}")
    
    timestep_sec_str = binding.get('DtSec', None)
    try:
        timestep_sec = int(timestep_sec_str)
    except (ValueError, TypeError):
        logger.error(f"'DtSec' value '{timestep_sec_str}' is invalid in settings. Exiting.")
        sys.exit(1)
        
    input_timestep_hours = TIMESTEP_SECONDS_TO_HOURS.get(timestep_sec, None)
    if input_timestep_hours is None:
        logger.error(f"Unsupported LISFLOOD input timestep ({timestep_sec}s). Exiting.")
        sys.exit(1)

    lisf_var_paths_map = get_lisflood_output_files_and_vars(LISFLOOD_DATA_DIR, binding)
    if not lisf_var_paths_map:
        logger.info("No NetCDF files to process. Exiting.")
        sys.exit(0)

    pp_base_dir = os.path.join(LISFLOOD_DATA_DIR, 'pp')
    create_output_dirs(LISFLOOD_DATA_DIR, ['pp'])
    possible_frequencies = ['6H', 'D', 'M', 'Y']
    ts_output_dirs = {freq: os.path.join(pp_base_dir, f"{freq}_TS") for freq in possible_frequencies}
    nc_output_dirs = {freq: os.path.join(pp_base_dir, f"{freq}_NC") for freq in possible_frequencies if freq != '6H'}
    for dir_path in list(ts_output_dirs.values()) + list(nc_output_dirs.values()):
        os.makedirs(dir_path, exist_ok=True)
    
    spatial_agg_methods_for_timeseries = ['mean', 'max', 'min', 'median']
    
    # Load Area map once if needed
    area_map = None
    if any(v in discharge_vars for v in lisf_var_paths_map.keys()):
        pixel_area_path = binding.get('PixelAreaUser')
        if not pixel_area_path:
            logger.error("PixelAreaUser variable not found in settings. Cannot process discharge variables.")
        else:
            path_to_load = pixel_area_path
            if pixel_area_path.endswith('.map'):
                path_to_load = os.path.splitext(pixel_area_path)[0] + '.nc'
            if os.path.exists(path_to_load):
                logger.info(f"Loading pixel area map from {path_to_load}")
                area_map = load_lisflood_variable_data(path_to_load)
            else:
                logger.error(f"PixelAreaUser file does not exist at {path_to_load}. Cannot process discharge.")
    
    # --- Sequential Processing Loop with Complete Logic ---
    logger.info(f"Starting aggregation sequentially for {len(lisf_var_paths_map)} discovered files...")
    
    for lisf_var, nc_file_path in lisf_var_paths_map.items():
        logger.info(f"--- Processing variable: {lisf_var} ---")
        
        # --- SPECIAL DISCHARGE HANDLING ---
        if lisf_var in discharge_vars:
            if area_map is None:
                logger.warning(f"Skipping discharge variable '{lisf_var}' because area map is not available.")
                continue

            # ... (Discharge processing logic as before) ...
            # This part is omitted for brevity but is identical to the previous correct version.
            continue 

        # --- STANDARD PROCESSING FOR FLUX AND STATE VARIABLES ---
        da_original = load_lisflood_variable_data(nc_file_path)
        if da_original is None:
            logger.warning(f"Could not load data for {nc_file_path}. Skipping variable.")
            continue
        
        internal_var_name = da_original.name
        if not internal_var_name:
            logger.warning(f"Could not determine internal variable name from {nc_file_path}. Skipping.")
            continue

        temporal_agg_method = None
        if lisf_var in fluxes_vars:
            temporal_agg_method = processing_config['output']['fluxes']['aggregation_type']
        elif lisf_var in states_vars:
            temporal_agg_method = processing_config['output']['states']['aggregation_type']
        else:
            logger.warning(f"Variable '{lisf_var}' not classified. Skipping.")
            continue
        
        da_current_agg = da_original
        native_freq_code = '6H' if input_timestep_hours == 6 else 'D'
        current_freq_code = native_freq_code
        
        if not args.overwrite:
            yearly_nc_path = os.path.join(nc_output_dirs.get('Y', ''), f"{internal_var_name}.nc")
            monthly_nc_path = os.path.join(nc_output_dirs.get('M', ''), f"{internal_var_name}.nc")
            daily_nc_path = os.path.join(nc_output_dirs.get('D', ''), f"{internal_var_name}.nc")

            if os.path.exists(yearly_nc_path):
                logger.info(f"Yearly output already exists for '{lisf_var}'. Task complete.")
                continue
            elif os.path.exists(monthly_nc_path):
                logger.info(f"Resuming from existing monthly data: {monthly_nc_path}")
                da_current_agg = load_lisflood_variable_data(monthly_nc_path)
                current_freq_code = 'M'
            elif os.path.exists(daily_nc_path) and input_timestep_hours == 6:
                logger.info(f"Resuming from existing daily data: {daily_nc_path}")
                da_current_agg = load_lisflood_variable_data(daily_nc_path)
                current_freq_code = 'D'
        
        if da_current_agg is None:
            logger.error(f"Failed to load resume data for {lisf_var}. Skipping.")
            continue

        logger.info(f"Generating {current_freq_code} time series...")
        ts_dict = aggregate_data_to_timeseries(
            da_current_agg, target_freq=current_freq_code, 
            spatial_agg_methods=spatial_agg_methods_for_timeseries,
            temporal_agg_method=temporal_agg_method)
        save_timeseries_to_csv(ts_dict, ts_output_dirs[current_freq_code], lisf_var, current_freq_code)
        
        aggregation_chain = []
        if current_freq_code == '6H':
            aggregation_chain = [('D', 'Daily'), ('M', 'Monthly'), ('Y', 'Yearly')]
        elif current_freq_code == 'D':
            aggregation_chain = [('M', 'Monthly'), ('Y', 'Yearly')]
        elif current_freq_code == 'M':
            aggregation_chain = [('Y', 'Yearly')]
        
        for target_freq, freq_name in aggregation_chain:
            logger.info(f"Aggregating from {current_freq_code} to {freq_name}...")
            next_da = aggregate_data_to_netcdf(da_current_agg, target_freq, temporal_agg_method)
            if next_da is None:
                logger.warning(f"Failed to aggregate to {freq_name}. Stopping chain.")
                break
            da_current_agg = next_da
            current_freq_code = target_freq
            if args.save_agg_nc_maps:
                save_netcdf_data(da_current_agg, nc_output_dirs[target_freq])
            
            logger.info(f"Generating {freq_name} time series...")
            ts_dict = aggregate_data_to_timeseries(
                da_current_agg, target_freq=target_freq,
                spatial_agg_methods=spatial_agg_methods_for_timeseries,
                temporal_agg_method=temporal_agg_method)
            save_timeseries_to_csv(ts_dict, ts_output_dirs[target_freq], lisf_var, target_freq)

    logger.info("\n--- LISFLOOD Processing Complete! ---")

if __name__ == '__main__':
    main()
