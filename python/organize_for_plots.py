#!/usr/bin/env python

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'October 2019'
############################################################################################

'''
Organizes the MetSerice with station comparison data in a format that it is easily to plot. Reads the csv-file
that resulted from combine_station_forecast.py.

Plots to make:
1) Boxplot with procentual difference between forecast pixel and station value. This boxplot contains all stations (NIWA and ECan) that are
   installed in Canterbury --> one boxplot
2) Idem, but then splitted into 12 months --> twelve boxplots
3) Boxplot with procentual difference between the average of the forecast pixels and the average of the stations within each catchment. Stations are selected to be
   within a 10 km buffer from the catchment border. There are 46 catchments, so this will lead to 46 boxplots.
4) Bar or line plots of R-squared and RMSE between accumulated station precipitation and accumulated forecasted precipitation. Lines or bars correspond with the hours of
   forecast.
5) Same as 4), but then averaged per catchment before calculating the statistics like R-squared and RMSE.   
'''

import pandas as pd
import numpy as np
import geopandas as gpd
from gistools import vector
import os
import statsmodels.api as sm

pd.options.display.max_columns = 100

#-Directory where result files should be saved
resultDir = r'C:\Active\Projects\MetService_precip_analysis\Data\station_metservice_comparison'
#-List with directories that contain
#Fproducts = ['ECMWF_8km', 'NCEP_4km', 'NCEP_8km', 'UKMO_8km']
#Fproducts = ['ECMWF_8km', 'NCEP_8km']
Fproducts = ['NCEP_8km']

#-Period to analyse for
from_date = '2018-08-01 00:00'
to_date = '2019-09-12 23:00'

#-Shapefils that determine station locations and catchments
catchment_shp = r'C:\Active\Projects\MetService_precip_analysis\Data\GIS\Catchments_NZTM_major_10kmbuffer.shp'
stations_shp = r'C:\Active\Projects\MetService_precip_analysis\Data\GIS\station_xy.shp'

#-read catchment shapefile into dataframe
catchment_gdf = gpd.read_file(catchment_shp)
unique_catchments = pd.unique(catchment_gdf['CATCH_NAME']).tolist()
#-read stations shapefile into dataframe
stations_gdf = gpd.read_file(stations_shp)
unique_stations = pd.unique(stations_gdf['ExtSiteID']).tolist()


##-PART 1 BELOW IS FOR FORMATTING THE DATA INTO FORMAT SUITABLE FOR BOXPLOTS AND LOOKS AT INDIVIDUAL PERCENTUAL ERRORS (I.E. NOT ACCUMULATED SUMS)
 
#-define some columns that will be used throughout the remainder of the 
proc_cols = [str(i)+'_proc' for i in range(1,85+1)]
df_proc = pd.DataFrame(columns=proc_cols)
cols = [str(i) for i in range(1, 85+1)]
   
for fprod in Fproducts:
    df = pd.read_csv(os.path.join(resultDir, fprod + '.csv'), parse_dates=[1], dayfirst=True)
    df = df.loc[(df['DateTime of forecast']>=pd.Timestamp(from_date)) & (df['DateTime of forecast']<=pd.Timestamp(to_date))]
       
    df['Month'] = df['DateTime of forecast'].dt.month
        
    ###-copy of df for procentual difference of all stations (plots 1) and 2)).
    df_canterbury = df.copy()
    #-Add columns to calculate procentual difference
    df_canterbury = pd.concat([df_canterbury, df_proc], axis=1)
    #-Calculate percentual difference
    for h in range(1, 85+1):
        df_canterbury[str(h) + '_proc'] = ((df_canterbury[str(h)] - df_canterbury['Station precipitation [mm]'])/df_canterbury['Station precipitation [mm]'])*100
    #-drop columns that are not needed
    df_canterbury.drop(cols, axis=1, inplace=True)
    #-rename the columns
    for h in range(1, 85+1):
        df_canterbury.rename(columns={str(h) + '_proc': h}, inplace=True)
    df_canterbury.to_csv(os.path.join(resultDir, 'all_stations_canterbury_' + fprod + '.csv'), index=False)
    df_canterbury = None
        
    #-empty final dataframes to fill for the catchment scale
    df_catchment_final = pd.DataFrame(columns=['Catchment', 'ExtSiteID', 'DateTime of forecast', 'MetService product', 'Station precipitation [mm]', 'Month'])
    df_catchment_final = pd.concat([df_catchment_final, pd.DataFrame(columns=[i for i in range(1, 85+1)])], axis=1)
    df_catchment_avg_final = pd.DataFrame(columns=['Catchment', 'DateTime of forecast', 'MetService product', 'Catchment precipitation [mm]', 'Month'])
    df_catchment_avg_final = pd.concat([df_catchment_avg_final, pd.DataFrame(columns=[i for i in range(1, 85+1)])], axis=1)
        
        
    ###-now per catchment
    for catch in unique_catchments:
        print('Processing %s' %catch)
        #-Select only the geopandas dataframe of the catchment of interest
        sel_catch = catchment_gdf.loc[catchment_gdf['CATCH_NAME']==catch]
        #-Select the stations that can be found within this catchment
        sel_stats = vector.sel_sites_poly(stations_gdf, sel_catch)
        #-Select only the precipitation data for the stations part of that catchment
        df_catchment = df.loc[df.ExtSiteID.isin(sel_stats.ExtSiteID.tolist())]
        df_catchment.insert(0, 'Catchment', catch)
        #-Create a copy of the catchment df that contains the stations to be used for averaging
        df_catchment_avg = df_catchment.copy()
        df_catchment_avg.drop('ExtSiteID', axis=1, inplace=True)
        #-Average precipitation station and forecast values
        df_catchment_avg = df_catchment_avg.groupby('DateTime of forecast').mean()
        df_catchment_avg.reset_index(inplace=True)
        df_catchment_avg.insert(1, 'MetService product', fprod)            
        df_catchment_avg.insert(0, 'Catchment', catch)
            
        #-Add columns for calculating percentual difference for individual stations
        df_catchment = pd.concat([df_catchment, df_proc], axis=1)
        #-Calculate percentual difference
        for h in range(1, 85+1):
            df_catchment[str(h) + '_proc'] = ((df_catchment[str(h)] - df_catchment['Station precipitation [mm]'])/df_catchment['Station precipitation [mm]'])*100
        #-drop columns that are not needed
        df_catchment.drop(cols, axis=1, inplace=True)
        #-rename the columns
        for h in range(1, 85+1):
            df_catchment.rename(columns={str(h) + '_proc': h}, inplace=True)
        #-concat to the final dataframe
        df_catchment_final = pd.concat([df_catchment_final, df_catchment], axis=0); df_catchment = None
                
        #-Add columns for calculating percentual difference for catchment average
        df_catchment_avg = pd.concat([df_catchment_avg, df_proc], axis=1)
        #-Calculate percentual difference
        for h in range(1, 85+1):
            df_catchment_avg[str(h) + '_proc'] = ((df_catchment_avg[str(h)] - df_catchment_avg['Station precipitation [mm]'])/df_catchment_avg['Station precipitation [mm]'])*100
        #-drop columns that are not needed
        df_catchment_avg.drop(cols, axis=1, inplace=True)
        #-rename the columns
        for h in range(1, 85+1):
            df_catchment_avg.rename(columns={str(h) + '_proc': h}, inplace=True)
        df_catchment_avg.rename(columns={'Station precipitation [mm]': 'Catchment precipitation [mm]'}, inplace=True)
        #-concat to the final dataframe
        df_catchment_avg_final = pd.concat([df_catchment_avg_final, df_catchment_avg], axis=0); df_catchment_avg = None
            
    df_catchment_final.to_csv(os.path.join(resultDir, 'all_stations_catchments_' + fprod + '.csv'), index=False)
    df_catchment_avg_final.to_csv(os.path.join(resultDir, 'catchments_avg_' + fprod + '.csv'), index=False)
       
    df = None;
    df_catchment_final = None;
    df_catchment_avg_final = None;
     
##-PART 2 BELOW IS FOR LOOKING AT CUMULATIVE SUMS FOR BOTH STATIONS AND FORECASTS
 
cols = [str(i) for i in range(1, 24+1)]  #-only consider forecasts up to 24 hours ahead
df_final_template = pd.DataFrame(columns=['DateTime of forecast', 'Forecasted hours', 'Accum. hours', 'ExtSiteID', 'MetService product', 'Accum. station precipitation [mm]', 'Accum. forecasted precipitation [mm]', 'Hcount'])
 
#-list with hours over which to accumulate
accum_hours = [1, 3, 6, 12, 24]
 
for fprod in Fproducts:
    df_canterbury = df_final_template.copy()
    df = pd.read_csv(os.path.join(resultDir, fprod + '.csv'), parse_dates=[1], dayfirst=True)
    df = df.loc[(df['DateTime of forecast']>=pd.Timestamp(from_date)) & (df['DateTime of forecast']<=pd.Timestamp(to_date))]
    df_fcols = df.iloc[:,0:4]
    for fhours in cols:
        df_fhours = df[[fhours]]
        df_merged = pd.concat([df_fcols, df_fhours], axis=1)
        df_merged.dropna(how='any', inplace=True)
        #-Add column with ones to be used later to know how many hours with data were really aggregated over the interval
        df_merged['Hcount'] = 1
        for st in unique_stations:
            print('Processing %s fhours:%s ExtSiteID:%s' %(fprod, fhours, st))
            df_sel = df_merged.loc[df_merged.ExtSiteID == st]
            df_sel.set_index('DateTime of forecast', inplace=True)
            for ah in accum_hours:
                #-accumulate to the hours ah
                df_accum = df_sel.resample(str(ah)+'H', label='right').sum()
                df_accum.loc[df_accum.ExtSiteID==0] = np.nan
                df_accum.dropna(inplace=True)
                df_accum.drop('ExtSiteID', axis=1, inplace=True)
                df_accum.reset_index(inplace=True)
                #-re-organize dataframe to concat hereafter
                df_temp = df_final_template.copy()
                df_temp['DateTime of forecast'] = df_accum['DateTime of forecast']
                df_temp['Forecasted hours'] = fhours
                df_temp['Accum. hours'] = ah
                df_temp['ExtSiteID'] = st
                df_temp['MetService product'] = fprod
                df_temp['Accum. station precipitation [mm]'] = df_accum['Station precipitation [mm]']
                df_temp['Accum. forecasted precipitation [mm]'] = df_accum[fhours]
                df_temp['Hcount'] = df_accum['Hcount']
                #-concat to final dataframe                
                df_canterbury = pd.concat([df_canterbury, df_temp], axis=0)
                #-clear variables
                df_accum = None; df_temp = None
            #-clear variables    
            df_sel = None
        #-clear variables
        df_fhours = None; df_merged = None    
    #-write to csv file
    temp_df = df_canterbury['DateTime of forecast'].dt.month
    df_canterbury.insert(1, 'Month', temp_df)
    df_canterbury.to_csv(os.path.join(resultDir, 'cumsum_all_stations_canterbury_' + fprod + '.csv'), index=False)
    df_canterbury.drop('Month', axis=1, inplace=True)
    #-rename station column to catchment precipitation for the next catchment averages
    df_canterbury.rename(columns={'Accum. station precipitation [mm]': 'Accum. catchment precipitation [mm]'}, inplace=True)
    #-empty final dataframe for the catchment averages.
    df_catchment_avg_final = pd.DataFrame(columns=['Catchment', 'DateTime of forecast', 'MetService product', 'Forecasted hours', 'Accum. hours', 'Accum. catchment precipitation [mm]', 'Accum. forecasted precipitation [mm]', 'Hcount'])
    #-now create the averages per catchment
    for catch in unique_catchments:
        print('Processing %s' %catch)
        #-Select only the geopandas dataframe of the catchment of interest
        sel_catch = catchment_gdf.loc[catchment_gdf['CATCH_NAME']==catch]
        #-Select the stations that can be found within this catchment
        sel_stats = vector.sel_sites_poly(stations_gdf, sel_catch)
        #-Select only the precipitation data for the stations part of that catchment
        df_catchment = df_canterbury.loc[df_canterbury.ExtSiteID.isin(sel_stats.ExtSiteID.tolist())]
        df_catchment.insert(0, 'Catchment', catch)
        #-Create a copy of the catchment df that contains the stations to be used for averaging
        df_catchment_avg = df_catchment.copy()
        df_catchment_avg.drop('ExtSiteID', axis=1, inplace=True)
        #-Average precipitation station and forecast values
        df_catchment_avg = df_catchment_avg.groupby(['DateTime of forecast', 'Forecasted hours', 'Accum. hours']).mean()
        df_catchment_avg.reset_index(inplace=True)
        df_catchment_avg.insert(1, 'MetService product', fprod)            
        df_catchment_avg.insert(0, 'Catchment', catch)
        #-concat with final dataframe
        df_catchment_avg_final = pd.concat([df_catchment_avg_final, df_catchment_avg], axis=0)
        df_catchment = None; df_catchment_avg = None;
    #-write catchment averages to csv file
    temp_df = df_catchment_avg_final['DateTime of forecast'].dt.month
    df_catchment_avg_final.insert(2, 'Month', temp_df)
    df_catchment_avg_final.to_csv(os.path.join(resultDir, 'cumsum_catchments_avg_' + fprod + '.csv'), index=False)
 
 
#-Calculate the statistics
for fprod in Fproducts:
    df_canterbury_stats = pd.DataFrame(columns=['Forecasted hours', 'Accum. hours', 'R-squared [-]', 'RMSE [mm]', 'Bias [%]', 'Nr. of observations'])
    df = pd.read_csv(os.path.join(resultDir, 'cumsum_all_stations_canterbury_' + fprod + '.csv'), parse_dates=[0], dayfirst=True)
    for fhours in range(1,24+1):
        for ah in accum_hours:
            df_sel = df.loc[(df['Forecasted hours']==fhours) & (df['Accum. hours']==ah)]
            hcount = df_sel['Hcount'].mean()
            x = df_sel[['Accum. station precipitation [mm]']]
            y = df_sel[['Accum. forecasted precipitation [mm]']]
            
            #-calculate the bias
            fbias = ((y.mean().to_numpy()[0] - x.mean().to_numpy()[0]) / x.mean().to_numpy()[0])*100
            
            x = sm.add_constant(x)  #-needed for intercept
                
            #-linear fit
            model = sm.OLS(y, x).fit()
            #-Calculate some stats
            rmse_val = np.sqrt(  np.mean(   (y.to_numpy()[:,0] - x.to_numpy()[:,1])**2   )  ) / hcount
            R2 = model.rsquared_adj
              
            nrObs = len(y)
            #-Put the statistics in the dataframe
            df_stats = pd.DataFrame(data={'Forecasted hours': [fhours], 'Accum. hours': [ah], 'R-squared [-]': [R2], 'RMSE [mm]': [rmse_val], 'Bias [%]': [fbias], 'Nr. of observations': [nrObs]})
            #-concat with final dataframe
            df_canterbury_stats = pd.concat([df_canterbury_stats, df_stats], axis=0)
            df_stats = None; df_sel = None;
        
    df_canterbury_stats.to_csv(os.path.join(resultDir, 'cumsum_statistics_all_stations_canterbury_' + fprod + '.csv'), index=False)
    df_canterbury_stats = None; df = None;
        
    #-Now for all the catchments
    df = pd.read_csv(os.path.join(resultDir, 'cumsum_catchments_avg_' + fprod + '.csv'), parse_dates=[0], dayfirst=True)
    df_catchment_stats = pd.DataFrame(columns=['Catchment', 'Forecasted hours', 'Accum. hours', 'R-squared [-]', 'RMSE [mm]', 'Bias [%]', 'Nr. of observations'])
    for catch in unique_catchments:
        print('Processing %s' %catch)
        for fhours in range(1,24+1):
            for ah in accum_hours:
                df_sel = df.loc[(df['Catchment']==catch) & (df['Forecasted hours']==fhours) & (df['Accum. hours']==ah)]
                try:
                    hcount = df_sel['Hcount'].mean()
                    x = df_sel[['Accum. catchment precipitation [mm]']]
                    y = df_sel[['Accum. forecasted precipitation [mm]']]
                    
                    #-calculate the bias
                    fbias = ((y.mean().to_numpy()[0] - x.mean().to_numpy()[0]) / x.mean().to_numpy()[0])*100
                    
                    x = sm.add_constant(x)  #-needed for intercept
                    #-linear fit
                    model = sm.OLS(y, x).fit()
                    #-Calculate some stats
                    rmse_val = np.sqrt(  np.mean(   (y.to_numpy()[:,0] - x.to_numpy()[:,1])**2   )  ) / hcount
                    R2 = model.rsquared_adj
                    nrObs = len(y)
                    flag = True
                except:
                    flag = False
                if flag:
                    #-Put the statistics in the dataframe
                    df_stats = pd.DataFrame(data={'Catchment': [catch], 'Forecasted hours': [fhours], 'Accum. hours': [ah], 'R-squared [-]': [R2], 'RMSE [mm]': [rmse_val], 'Bias [%]': [fbias], 'Nr. of observations': [nrObs]})
                    #-concat with final dataframe
                    df_catchment_stats = pd.concat([df_catchment_stats, df_stats], axis=0)
                df_stats = None; df_sel = None;
        
    df_catchment_stats.to_csv(os.path.join(resultDir, 'cumsum_statistics_catchments_avg_' + fprod + '.csv'), index=False)
    df = None; df_catchment_stats = None;
       
