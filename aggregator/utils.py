# aggregator/utils.py

import os
import pandas as pd
import datetime
import logging
import fnmatch # Import the fnmatch module for wildcard matching

# Ensure lisflood is installed or its path is in PYTHONPATH
from lisflood.global_modules.settings import LisSettings

logger = logging.getLogger(__name__)

def ncname2lisname(nc_filename_base, binding_lisflood):
    """
    Finds the LISFLOOD variable name (key) from a NetCDF file's base name
    by checking if the base name matches any binding path value, including wildcards.

    Args:
        nc_filename_base (str): The base name of the NetCDF file without extension.
        binding_lisflood (dict): The settings.binding dictionary from a LisSettings object.

    Returns:
        str or None: The corresponding LISFLOOD variable name, or None if no match is found.
    """
    logger.debug(f"--- ncname2lisname: trying to find match for '{nc_filename_base}' ---")
    for lisf_var_name, binding_value in binding_lisflood.items():
        if isinstance(binding_value, (list, tuple)):
            # This handles cases where a binding has multiple paths
            for v_path in binding_value:
                if isinstance(v_path, str):
                    binding_pattern = os.path.basename(v_path)
                    # For robustness, remove potential .nc extension from the binding's basename
                    if binding_pattern.endswith('.nc'):
                        binding_pattern = binding_pattern[:-3]
                    
                    logger.debug(f"  Checking '{nc_filename_base}' against list pattern '{binding_pattern}' for variable '{lisf_var_name}'")
                    # --- MODIFIED: Use fnmatch for wildcard comparison ---
                    if fnmatch.fnmatch(nc_filename_base, binding_pattern):
                        logger.debug(f"  >> Match found! Returning '{lisf_var_name}'")
                        return lisf_var_name
        elif isinstance(binding_value, str):
            binding_pattern = os.path.basename(binding_value)
            # For robustness, remove potential .nc extension from the binding's basename
            if binding_pattern.endswith('.nc'):
                binding_pattern = binding_pattern[:-3]

            logger.debug(f"  Checking '{nc_filename_base}' against binding pattern '{binding_pattern}' for variable '{lisf_var_name}'")
            # --- MODIFIED: Use fnmatch for wildcard comparison ---
            if fnmatch.fnmatch(nc_filename_base, binding_pattern):
                logger.debug(f"  >> Match found! Returning '{lisf_var_name}'")
                return lisf_var_name
                
    logger.debug(f"--- No match found for '{nc_filename_base}' ---")
    return None

def get_lisflood_settings(settings_xml_path):
    """
    Loads LISFLOOD settings from an XML file using LisSettings.

    Args:
        settings_xml_path (str): Full path to the LISFLOOD XML settings file.

    Returns:
        lisflood.global_modules.settings.LisSettings: The parsed LisSettings object.
    """
    try:
        return LisSettings(settings_xml_path)
    except Exception as e:
        logger.error(f"Error loading LISFLOOD settings XML from {settings_xml_path}: {e}", exc_info=True)
        return None

def create_output_dirs(base_path, sub_dirs):
    """
    Creates a list of nested output directories if they do not already exist.
    """
    for sub_dir in sub_dirs:
        full_path = os.path.join(base_path, sub_dir)
        os.makedirs(full_path, exist_ok=True)




##############################################################
# This is a script containing Lisflood utility functions.
# To use it:
# import sys
# sys.path.append('/perm/mo/mocm/prg/EFTools/lisflood/')
# import lsu #lisflood utilities
##############################################################
# 2019-01-15 v 01.00 CM Initial version
##############################################################
import pandas as pd
import datetime

def read_tss(tssfilename):
    """Read tss file in LISFLOOD file format
    
    :param tssfilename: LISFLOOD tss file
    :return: tssdata: LISFLOOD time series
    """
    with open(tssfilename) as fp:
        rec = fp.readline()
        if rec.split()[0] == 'timeseries':
            # LISFLOOD tss file with header
            # get total number of outlets
            outlets_tot_number = int(fp.readline())
            next(fp)
            outlets_id = []
            for i in range(0, outlets_tot_number - 1):
                rec = fp.readline()
                rec = rec.strip()
                outlets_id.append(rec)  #Lisflood ID code for output points
            fp.close()
            tssdata = pd.read_table(tssfilename, delim_whitespace=True, header=None, names=outlets_id, index_col=0,
                                   skiprows=outlets_tot_number + 2)
            # tssdata = pd.read_csv(tssfilename, sep='\t', header=None, names=outlets_id, index_col=0,
            #                       skiprows=outlets_tot_number + 2)

        else:
            # LISFLOOD tss file without header (table)
            numserie = len(rec.split())
            outlets_id = []
            for i in range(1, numserie):
                outlets_id.append(str(i))  #Lisflood progressive ID code for output points
            fp.close()
            tssdata = pd.read_table(tssfilename, delim_whitespace=True, header=None, names=outlets_id, index_col=0)
            # tssdata = pd.read_csv(tssfilename, sep='\t', header=None, names=outlets_id, index_col=0)

    return tssdata


def make_time_index(tssdata, firststepDate,timestep):
    """Assign dates as index to qts from tss file
    
    :param tssdata: tss time series as pandas dataframe
    :param firststepDate: starting date of tss file
    :param timestep: step of tss file in minutes
    :return: tssdata
    """
    # get date of last step in tss file
    numsteps = tssdata.shape[0]
    laststepDate = firststepDate + datetime.timedelta(minutes=((numsteps - 1) * timestep))
    # get frequency for ts data in minutes
    ts_frequency = str(timestep)+"min"
    # generate dates index for tss data
    dates = pd.date_range(firststepDate, laststepDate, freq=ts_frequency)
    #dates = pd.date_range('2012-01-01 00:00', '2014-12-31 00:00', freq='D')
    tssdata.index = dates
    return tssdata


def getxmlpairs(xmlfile,branchname):
    """Get key-value pairs from 'branchname' section in Lisflood XML file
    
    :param xmlfile: string  Path and name of Lisflood XML file
    :param branchname: string  Name of section in Lisflood XML file
    :return: xmlpairs: list  Key-value pairs as in Lisflood XML file
              [nodeag, elemkeys[0], value[0], elemkeys[1], value[1]]
              ['setoption', 'choice', '1', 'name', 'repSnowMeltMaps']
    """
    import xml.etree.ElementTree as ET

    try:
        tree = ET.ElementTree(file=xmlfile)         # Parse XML file to tree
    except:
        msg = "Cannot read XML file: " + xmlfile
        print(msg)

    root = tree.getroot()                           # root Element

    xmlpairs = []   # list of key-values pairs
    eltaglist = []  # list of tags in 'branchname' section of Lisflood XML file
    value = []


    for elem in root.findall(branchname):           # Elementi con tag 'branchname' nel root ('lfoption' 'lfuser' 'lfbinding')
        # all_descendants = list(elem.iter())       # Trovo TUTTI i tag dei child di elem, compreso elem stesso (element object)
        for subelem in elem.iter():
            if(str(subelem.tag) != branchname):     # escludo 'branchname' dalla lista
                eltaglist.append(str(subelem.tag))  # creo una lista con i tag di tutti i sottoelementi di 'branchname'

    singleentry = set(eltaglist)
    taglist = list(singleentry)     # creo una lista con valori unici dei tag

    for tagname in taglist:
        for elem in root.findall(branchname):
            for node in elem.iter(tagname):
                # print(node.tag)
                # print(node.attrib)
                nodekeys = list(node.attrib)    # trovo il nome di tutte le chiavi usate in node e le salvo in una lista
                if(len(nodekeys) == 0):
                    continue

                else:
                    value.clear()
                    for i in range(len(nodekeys)):
                        value.append(node.attrib.get(nodekeys[i]))  # salvo il valore associato alla chiave in value

                    option = [tagname]
                    for i in range(len(nodekeys)):  # metto tutte le coppie key-value in option
                        option.append(nodekeys[i])
                        option.append(value[i])

                xmlpairs.append(option)      # salvo in una list

    return xmlpairs
