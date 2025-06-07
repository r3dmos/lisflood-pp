#! /home/mosfran/Documents/conda/envs/hydro/bin/python3
"""
LISFLOOD utility tool to transform tss file to csv
    
Run:
See test folder.
./tss2csv.py -i /vol/efas/mocm/xdom/calib_XDOM/tss_1990-2018/1990-2014/disWin.tss -O /vol/efas/mocm/xdom/calib_XDOM/tss_1990-2018/1990-2014/disWin.csv -I /vol/efas/mocm/xdom/calib_XDOM/settings/lisfloodSettings_ecWB_1990-2014_cold_day.xml
./tss2csv.py -i disWin.tss -O tss.csv -C 199001020600 -S 21600

2018-01-23  00.01.01    CM  Fixed error in timestamp caused by LISFLOOD step 1 = CalendarDayStart
2018-01-15  00.01.00    CM  Initial code
"""

#-----------------------------------------------
# Import EFTools library
import pathlib
import sys 
import os
import xarray as xr
import numpy as np
# loc_name='utils'
# loc=__file__.find('tools')
# tools_dir=__file__[0:(loc+len(loc_name))]
# sys.path.append(os.path.join(tools_dir))
# import lsu 
# #lisflood utilities
#-----------------------------------------------

import datetime

from aggregator.utils import *
def getarg():
    """ Get program arguments.

    :return: args:  namespace of program arguments
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--lisfloodtssfile', type=argparse.FileType('r'), required=True,
                        help='Name and path of LISFLOOD tss file or folder where tss files are')
    parser.add_argument('-O', '--outputcsvfile', type=argparse.FileType('w'), required=True,
                        help='Name and path of output csv file')
    parser.add_argument('-I', '--lisfloodxmlfile', type=argparse.FileType('r'), required=False,
                        help='Name and path of LISFLOOD xml settings file')
    parser.add_argument('-V', '--outletsfile', type=argparse.FileType('r'), required=False,
                        help='Name and path of LISFLOOD xml settings file')
    parser.add_argument('-C', '--calendardate', type=str, required=False,
                        help='CalendarDayStart of tss time series [YYYYMMDDhhmm]')
    parser.add_argument('-S', '--steping', type=int, required=False,
                        help='Step of tss time series in [sec]')
    parser.add_argument('-ID', '--station_id', type=int, required=False,
                        help='Station ID')
    args = parser.parse_args()  # assign namespace to args
    return args


# read settings

args = getarg()
tssfilename = args.lisfloodtssfile.name
csvfilename = args.outputcsvfile.name
# outletsname=args.outlets.name

try:
    # try reading info from settings xml file
    xmlfilename = args.lisfloodxmlfile
    xmlpairs = getxmlpairs(xmlfilename,"lfuser")
    search = 'CalendarDayStart'
    for sublist in xmlpairs:
        if sublist[2] == search:
            calendarday = datetime.datetime.strptime(sublist[4], '%d/%m/%Y %H:%M')
            break
    search = 'DtSec'
    for sublist in xmlpairs:
        if sublist[2] == search:
            tsstep = int(sublist[4])
            break
except:
    calendarday = datetime.datetime.strptime(args.calendardate, '%Y%m%d%H%M')
    tsstep = args.steping
    codeID = args.station_id
# Subtract 1 timestep because LISFLOOD step 1 = CalendarDayStart
calendarday = calendarday - datetime.timedelta(seconds = int(tsstep))

# read qts data from LISFLOOD tss file

tssdata = read_tss(tssfilename)

#add time index to qts
startdate = calendarday + datetime.timedelta(seconds = int(tssdata.index[0]) * tsstep)
enddate = calendarday + datetime.timedelta(seconds = int(tssdata.index[tssdata.shape[0]-1]) * tsstep)
tssdata = make_time_index(tssdata, startdate,int(tsstep/60))
# outlets_ds=xr.open_dataset(outletsname)
# outlets_flat=outlets_ds['outlets'].values.ravel()
# id_nc=outlets_flat[~np.isnan(outlets_flat)]
# outlets_ds['outlets'].values[~np.isnan(outlets_ds['outlets'].values)]
# print qts to csv file
#if (codeID):
#    tssdata = tssdata.rename(columns={'1': codeID})
tssdata.to_csv(csvfilename, sep=',')

