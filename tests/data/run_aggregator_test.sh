#!/bin/bash

# Script to run the lisflood_temporal_aggregator with specific test data.
# Place this script in your tests/data/ folder.

# --- Configuration ---
# Assuming this script is located in tests/data/
BASE_TEST_DATA_DIR=$(cd "$(dirname "$0")" && pwd) # More robust way to get script's directory
# Navigate to the project root (lisflood-pp) to run the Python module correctly
PROJECT_ROOT_DIR="${BASE_TEST_DATA_DIR}/../.." # Adjust if your tests/data is nested differently

# Original locations of your test input files
LISFLOOD_SETTINGS_XML_ORIGINAL="${BASE_TEST_DATA_DIR}/run_lat_lon.xml"
LISFLOOD_NC_SOURCE_DIR="${BASE_TEST_DATA_DIR}/reference" # Directory containing your .nc files

# Temporary root directory for setting up the input structure for the aggregator
TEMP_INPUT_ROOT="${BASE_TEST_DATA_DIR}/temp_aggregator_input"
# Define a dummy experiment and catchment name for the temporary structure
DUMMY_EXPERIMENT_NAME="my_test_exp"
DUMMY_CATCHMENT_NAME="my_test_catchment"
# The 'output' subdirectory expected by your script
LISFLOOD_OUTPUT_SUBDIR_NAME="output"

# Full path to where the .nc files and .xml file will be temporarily placed
TEMP_LISFLOOD_OUTPUT_PATH="${TEMP_INPUT_ROOT}/${DUMMY_EXPERIMENT_NAME}/${DUMMY_CATCHMENT_NAME}/${LISFLOOD_OUTPUT_SUBDIR_NAME}"

# Desired final location for the aggregated 'pp' output folder
FINAL_PP_TARGET_DIR="${BASE_TEST_DATA_DIR}/pp"

# Name of the LISFLOOD settings XML file (as passed to the script)
LISFLOOD_SETTINGS_XML_ARG_NAME="run_lat_lon.xml"


# --- Helper Functions ---
cleanup_and_exit() {
    echo "Cleaning up temporary directory: ${TEMP_INPUT_ROOT}"
    rm -rf "${TEMP_INPUT_ROOT}"
    exit "$1"
}

# --- 1. Setup Temporary Directory Structure ---
echo "INFO: Setting up temporary directory structure at ${TEMP_INPUT_ROOT}..."
# Remove any previous temporary directory to ensure a clean run
rm -rf "${TEMP_INPUT_ROOT}"
mkdir -p "${TEMP_LISFLOOD_OUTPUT_PATH}"

# Check if the original LISFLOOD settings XML exists
if [ ! -f "${LISFLOOD_SETTINGS_XML_ORIGINAL}" ]; then
    echo "ERROR: LISFLOOD settings XML not found at ${LISFLOOD_SETTINGS_XML_ORIGINAL}"
    cleanup_and_exit 1
fi
# Copy the LISFLOOD settings XML to the temporary output path
cp "${LISFLOOD_SETTINGS_XML_ORIGINAL}" "${TEMP_LISFLOOD_OUTPUT_PATH}/${LISFLOOD_SETTINGS_XML_ARG_NAME}"
echo "INFO: Copied settings XML to ${TEMP_LISFLOOD_OUTPUT_PATH}/${LISFLOOD_SETTINGS_XML_ARG_NAME}"

# Check if the source NetCDF directory exists
if [ ! -d "${LISFLOOD_NC_SOURCE_DIR}" ]; then
    echo "ERROR: LISFLOOD NetCDF source directory not found at ${LISFLOOD_NC_SOURCE_DIR}"
    cleanup_and_exit 1
fi
# Copy NetCDF files
NC_FILES_COUNT=$(find "${LISFLOOD_NC_SOURCE_DIR}" -maxdepth 1 -name "*.nc" -type f 2>/dev/null | wc -l | tr -d ' ') # Make sure to count files and trim whitespace
if [ "${NC_FILES_COUNT}" -eq 0 ]; then
    echo "WARNING: No .nc files found in ${LISFLOOD_NC_SOURCE_DIR}"
    # Decide if this is an error or just a warning. For a test, it might be an error.
    # cleanup_and_exit 1
else
    # Use find to robustly copy files, avoids issues with too many files for *
    find "${LISFLOOD_NC_SOURCE_DIR}" -maxdepth 1 -name "*.nc" -type f -exec cp {} "${TEMP_LISFLOOD_OUTPUT_PATH}/" \;
    echo "INFO: Copied ${NC_FILES_COUNT} NetCDF files from ${LISFLOOD_NC_SOURCE_DIR} to ${TEMP_LISFLOOD_OUTPUT_PATH}/"
fi


# --- 2. Run the Aggregator Script ---

# Activate your Python environment
# Ensure this points to your conda installation and your environment name is correct.
CONDA_BASE_PATH=$(conda info --base)
if [ -z "${CONDA_BASE_PATH}" ]; then
    echo "ERROR: Conda base path not found. Is Conda installed and initialized?"
    cleanup_and_exit 1
fi

echo "INFO: Activating Conda environment 'lisflood'..." # Replace 'lisflood' with your env name
# shellcheck source=/dev/null
source "${CONDA_BASE_PATH}/etc/profile.d/conda.sh"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to source conda.sh. Check Conda installation."
    cleanup_and_exit 1
fi

conda activate lisflood # Replace 'lisflood' with your actual environment name
if [ $? -ne 0 ]; then
   echo "ERROR: Failed to activate Conda environment 'lisflood'."
   cleanup_and_exit 1
fi
echo "INFO: Python executable: $(which python)"
echo "INFO: PYTHONPATH: ${PYTHONPATH}"


echo "INFO: Changing to project root directory: ${PROJECT_ROOT_DIR}"
cd "${PROJECT_ROOT_DIR}" || cleanup_and_exit 1 # Exit if cd fails

echo "INFO: Running lisflood_temporal_aggregator script..."

# Execute the __main__.py script directly
python __main__.py \
    --output_root "${TEMP_INPUT_ROOT}" \
    --lisflood_settings_xml "${LISFLOOD_SETTINGS_XML_ARG_NAME}" \
    --loglevel DEBUG
    # You can add --settings_yaml path/to/your_agg_settings.yaml if needed
    # You can add --save_agg_nc_maps if needed

AGGREGATOR_EXIT_CODE=$?

# Return to the original directory (tests/data)
echo "INFO: Changing back to script directory: ${BASE_TEST_DATA_DIR}"
cd "${BASE_TEST_DATA_DIR}" || exit 1 # Exit if cd fails


# --- 3. Handle Aggregated Output ---
# The aggregator script creates a 'pp' folder inside the catchment directory:
# TEMP_INPUT_ROOT / DUMMY_EXPERIMENT_NAME / DUMMY_CATCHMENT_NAME / pp
GENERATED_PP_DIR_PATH="${TEMP_INPUT_ROOT}/${DUMMY_EXPERIMENT_NAME}/${DUMMY_CATCHMENT_NAME}/pp"

if [ ${AGGREGATOR_EXIT_CODE} -ne 0 ]; then
    echo "ERROR: Aggregator script exited with error code ${AGGREGATOR_EXIT_CODE}."
    echo "PYTHONPATH was: ${PYTHONPATH}" # Print PYTHONPATH on error
    cleanup_and_exit ${AGGREGATOR_EXIT_CODE}
fi

if [ -d "${GENERATED_PP_DIR_PATH}" ]; then
    echo "INFO: Aggregator finished successfully. Output generated in ${GENERATED_PP_DIR_PATH}"
    # Ensure the final target directory's parent exists, then remove old and move new
    mkdir -p "$(dirname "${FINAL_PP_TARGET_DIR}")"
    rm -rf "${FINAL_PP_TARGET_DIR}" # Remove if it already exists to avoid issues with mv
    mv "${GENERATED_PP_DIR_PATH}" "${FINAL_PP_TARGET_DIR}"
    if [ $? -eq 0 ]; then
        echo "INFO: Aggregated output successfully moved to ${FINAL_PP_TARGET_DIR}"
    else
        echo "ERROR: Failed to move aggregated output from ${GENERATED_PP_DIR_PATH} to ${FINAL_PP_TARGET_DIR}"
        cleanup_and_exit 1
    fi
else
    echo "ERROR: Aggregator script finished, but no 'pp' output directory found at expected location: ${GENERATED_PP_DIR_PATH}"
    cleanup_and_exit 1
fi

# --- 4. Cleanup ---
cleanup_and_exit 0
