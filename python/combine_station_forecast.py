#!/usr/bin/env python

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'September 2019'
############################################################################################

'''
Creates a csv-file for each MetService product in which it exctracts the forecasted precipitation value for a particular timestamp, nr. of forecast hours and station, and
extracts the observed precipitation station value for that same timestamp. MetService forecasted precipitation is extracted from the grid cell in which the precipitation station
is located.
'''

import pandas as pd
import os, subprocess, glob

pd.options.display.max_columns = 100

#-Directory where result files should be saved
resultDir = r'C:\Active\Projects\MetService_precip_analysis\Data\station_metservice_comparison'
#-Directory that contains the MetService converted GTiff products
tifDir = r'C:\Active\Projects\MetService_precip_analysis\Data\tif_forecasts'
#-List with directories that contain
#Fproducts = ['ECMWF_8km', 'NCEP_4km', 'NCEP_8km', 'UKMO_8km']
#Fproducts = ['ECMWF_8km', 'NCEP_8km']
Fproducts = ['ECMWF_8km']


from_date = '2018-08-01 00:00'
to_date = '2019-09-12 23:00'

#-Station location file
statXY = r'C:\Active\Projects\MetService_precip_analysis\Data\Stations\station_xy.csv'
#-Station time-series file
statTS = r'C:\Active\Projects\MetService_precip_analysis\Data\Stations\station_ts.csv'

#-Read the station locations into a dataframe
statXY_df = pd.read_csv(statXY)
#-Read the station time-series into a dataframe
statTS_df = pd.read_csv(statTS, parse_dates=[1], dayfirst=True)
#-station IDs
statIDs = pd.unique(statXY_df.ExtSiteID).tolist()
#-Date range to process
datetime_range = pd.date_range(pd.Timestamp(from_date), pd.Timestamp(to_date), freq='H')

#-set the columns of the dataframe to be filled
base_cols = ['Station precipitation [mm]']
forecast_cols = [i for i in range(1, 85+1)]
base_cols.extend(forecast_cols)


for fprod in Fproducts:
    #-create the template for the dataframe to be filled
    iterables = [statIDs, datetime_range, [fprod]]
    index = pd.MultiIndex.from_product(iterables, names=['ExtSiteID', 'DateTime of forecast', 'MetService product'])
    df_final = pd.DataFrame(columns=base_cols, index=index)
    df_final.reset_index(inplace=True)
    
    
    #-Get all Tiff files for that product and loop over those tifs
    ff = glob.glob(os.path.join(tifDir, fprod) + '\\*.tif')
    for f in ff:
        tifFile = f
        f = os.path.basename(f)
        fstring = f[:17]
        fstring = fstring.split('_')
        hours = int(fstring[0].rstrip('h'))
        tstamp = pd.Timestamp(fstring[1] + fstring[2])
        fstring = None
        #-Loop over the station IDs
        for ID in statIDs:
            print('Processing %s ID %s %s fhours %s' %(fprod, ID, tstamp, hours))
            try:
                statValue = statTS_df.loc[(statTS_df.ExtSiteID == ID) & (statTS_df.DateTime == tstamp), 'Value'].to_numpy()[0]
            except:
                statValue = False
            #-only proceed if station value can be found for that timestamp
            if statValue:
                X = statXY_df.loc[statXY_df.ExtSiteID == ID, 'NZTMX'].to_numpy()[0]
                Y = statXY_df.loc[statXY_df.ExtSiteID == ID, 'NZTMY'].to_numpy()[0]
                #-Add station observed precipitation to dataframe
                df_final.loc[(df_final.ExtSiteID == ID) & (df_final['DateTime of forecast']==tstamp) & (df_final['MetService product']==fprod), 'Station precipitation [mm]'] = statValue
                #-Get the forecasted precipitation value for the timestamp
                try:
                    child = subprocess.Popen('gdallocationinfo ' + tifFile + ' -valonly -geoloc %s %s' %(X,Y), stdout=subprocess.PIPE)
                    streamdata = child.communicate()[0]
                    streamdata = float(streamdata.split()[0])
                    #-Add the forecasted precipitation value for the timestamp and forecast hours
                    df_final.loc[(df_final.ExtSiteID == ID) & (df_final['DateTime of forecast']==tstamp) & (df_final['MetService product']==fprod), hours] = streamdata
                except:
                    pass
    
    
    df_final.dropna(how='all', subset = forecast_cols, inplace=True)            
    df_final.to_csv(os.path.join(resultDir, fprod + '.csv'), index=False)
    df_final = None;    
    
