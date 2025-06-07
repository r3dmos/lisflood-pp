# aggregator/config.py

import os

# --- Default Paths (These are now relative or point to internal package resources) ---
# POSTPROCESS_SETTINGS_YAML: Path to the YAML file defining aggregation rules.
POSTPROCESS_SETTINGS_YAML = os.path.join(os.path.dirname(__file__), 'settings', 'settings.yaml')


# --- General NetCDF/Xarray Processing Configuration ---
# Variables often found in NetCDF files that are not actual data variables
# (e.g., coordinate bounds, CRS information) to be removed *after* loading a file.
VAR_TO_REMOVE = ['time_bnds', 'crs']




# Common names for latitude and longitude dimensions for flexibility
LAT_DIM_NAMES = ["Latitude", "lat", "y"]
LON_DIM_NAMES = ["Longitude", "lon", "x"]

# --- Aggregation Configuration (DEFAULT settings if YAML is not found or incomplete) ---
# This dictionary defines which LISFLOOD variables are considered 'fluxes' or 'states'
# and their default temporal aggregation type (sum for fluxes, mean for states).
# This will be *merged with* or *overridden by* the actual settings.yaml content.
DEFAULT_AGGREGATION_SETTINGS = {
    'time_processing': {
        'output': {
            'fluxes': {
                'aggregation_type': 'sum',
                'var_name': [
                    'EWIntForestMaps','GwPercUZLZMaps','DirectRunoffMaps','TotalToChanMaps',
                    'TotalRunoffMaps','TotalToChanMaps','UZOutflowMaps','SurfaceRunoffMaps','ESActMaps','ETActMaps','EWIntMaps','EWater',
                    'SnowMeltMaps','SnowMaps','TaMaps','InfiltrationMaps','PrefFlowMaps','UZOutflowMaps','LZOutflowMaps'
                ]
            },
            'states': {
                'aggregation_type': 'mean',
                'var_name': [
                    'AvailGWMaps','WaterDepthState','LZMaps','UZMaps','SnowCoverMaps',
                    'Theta1ForestMaps', 'Theta2ForestMaps', 'Theta3ForestMaps', 'FrostIndexState','WaterDepthMaps',
                    'CumInterceptionMaps','CumInterceptionForestMaps'
                ]
            },
            'discharge': {
                'aggregation_type': 'sum',
                'var_name': [
                    'DischargeMaps'
            ]
            },
            'save_aggregated_netcdf_maps': False
        }
    }
}


# Define mapping of LISFLOOD timestep seconds to common hourly representations
TIMESTEP_SECONDS_TO_HOURS = {
    6 * 3600: 6,  # 21600 seconds = 6 hours
    24 * 3600: 24 # 86400 seconds = 24 hours (daily)
}

# A list of specific LISFLOOD variable names to ignore completely during file discovery.
# This is useful for excluding state files that are only for starting or ending a run.
VARS_TO_IGNORE = [
    # End-of-run state files
    'Theta1End', 'Theta2End', 'Theta3End', 'UZEnd', 'LZEnd', 'DSLREnd',
    'WaterDepthEnd', 'OFDirectEnd', 'OFOtherEnd', 'OFForestEnd',
    'ChanCrossSectionEnd', 'SnowCoverAEnd', 'SnowCoverBEnd', 'SnowCoverCEnd',
    'FrostIndexEnd', 'CumInterceptionEnd', 'LakeLevelEnd', 'LakePrevInflowEnd',
    'LakePrevOutflowEnd', 'LakeStorageM3', 'ReservoirFillEnd', 'CrossSection2End',
    'ChSideEnd', 'ChanQEnd', 'DischargeEnd', 'DSLRForestEnd',
    'CumInterceptionForestEnd', 'Theta1ForestEnd', 'Theta2ForestEnd',
    'Theta3ForestEnd', 'UZForestEnd', 'CumIntSealedEnd', 'DSLRIrrigationEnd',
    'CumInterceptionIrrigationEnd', 'Theta1IrrigationEnd', 'Theta2IrrigationEnd',
    'Theta3IrrigationEnd', 'UZIrrigationEnd',

    # Initial condition values (often single values or paths, but good to list)
    'OFDirectInitValue', 'OFOtherInitValue', 'OFForestInitValue',
    'SnowCoverAInitValue', 'SnowCoverBInitValue', 'SnowCoverCInitValue',
    'FrostIndexInitValue', 'CumIntInitValue', 'UZInitValue', 'DSLRInitValue',
    'LZInitValue', 'TotalCrossSectionAreaInitValue', 'ThetaInit1Value',
    'ThetaInit2Value', 'ThetaInit3Value', 'CrossSection2AreaInitValue',
    'PrevSideflowInitValue', 'LakeInitialLevelValue', 'LakePrevInflowValue',
    'LakePrevOutflowValue', 'PrevDischarge', 'CumIntForestInitValue',
    'UZForestInitValue', 'DSLRForestInitValue', 'ThetaForestInit1Value',
    'ThetaForestInit2Value', 'ThetaForestInit3Value',
    'CumIntIrrigationInitValue', 'UZIrrigationInitValue',
    'DSLRIrrigationInitValue', 'ThetaIrrigationInit1Value',
    'ThetaIrrigationInit2Value', 'ThetaIrrigationInit3Value',
    'CumIntSealedInitValue'
]