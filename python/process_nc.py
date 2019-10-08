#!/usr/bin/env python

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'October 2019'
############################################################################################


import xarray as xr
import numpy as np
import pandas as pd
import os, pytz, glob
from osgeo import gdal
from osgeo import osr
from osgeo import ogr

'''
Reads MetService netCDF files and converts them into GTiff files 
'''


def reproject(src_epsg, dst_epsg, x, y):
    '''
    Reproject coordinates x and y from EPSG defined by src_epsg to EPSG defined by dst_epsg
    Returns the coordinates x and y in the dst_epsg projection
    
    Input:
    ------
        src_epsg: EPSG number of source coordinates
        dst_epsg: EPSG number of target coordinates
        x:        x-coordinate of source coordinates
        y:        y-coordinate of source coordinates
        
    Returns:
    --------
        [x,y]:    x and y coordinate in target coordinate system
    '''
    source = osr.SpatialReference()
    source.ImportFromEPSG(src_epsg)
     
    target = osr.SpatialReference()
    target.ImportFromEPSG(dst_epsg)
     
    transform = osr.CoordinateTransformation(source, target)
    point = ogr.Geometry(ogr.wkbPoint)
    point.AddPoint(x,y)
    point.Transform(transform)
    return point.GetX(), point.GetY()
    
def ncToDataFrame(ncF, subdataset=None, dropcols=None):
    '''
    Converts a NetCDF file to a pandas dataframe and returns that dataframe.
    
    Input:
    ------
        ncF:    Full path to NetCDF file (*.nc).
    
    Optional Input:
    ---------------
        subdataset: Name (str) of subdataset that needs to be extracted and converted.
        dropcols: List of column names that should be dropped from the dataframe.
    
    Returns:
    --------
        dataset: Pandas dataframe
    '''
    
    dataset = xr.open_dataset(ncF)
    if subdataset:
        dataset = dataset[subdataset]
    dataset = dataset.to_dataframe().reset_index()
    if dropcols:
        dataset.drop(dropcols, axis=1, inplace=True)
        
    return dataset



###-working directory
workDir = r'C:\Active\Projects\MetService_precip_analysis\Data'
#-root directory for netCDF forecast products
ncRootDir = r'C:\Active\Projects\MetService_precip_analysis\Data\nc_forecasts' 
#-directory for tempory files
tempDir = r'C:\Active\Projects\MetService_precip_analysis\Data\temp_files'
#-directory where the netCDF to converted GTiff files of the different products will be saved
tifDir = r'C:\Active\Projects\MetService_precip_analysis\Data\tif_forecasts'
#-file to log errors
logFile = 'errors.log' 

#-List with directories that contain
#Fproducts = ['ECMWF_8km', 'NCEP_4km', 'NCEP_8km', 'UKMO_8km']
#Fproducts = ['NCEP_4km', 'NCEP_8km', 'UKMO_8km']
Fproducts = ['UKMO_8km']

vrtFile = r'C:\Active\Eclipse_workspace\MetService_precip_forecasts\python\prec.vrt'
csvFile = r'C:\Active\Projects\MetService_precip_analysis\Data\temp_files\prec.csv'


##-Output extent settings for the Canterbury region
xmin = 1323766.5234000002965331; xmin = xmin - 5000
ymin = 5004696.7684000004082918; ymin = ymin - 5000
xmax = 1692368.8068000003695488; xmax = xmax + 5000 
ymax = 5361879.5686999997124076; ymax = ymax + 5000
##-Set raster output resolution to interpolate to and calculate number of rows and columns
res = 1000
rows = np.ceil((ymax-ymin)/res)
cols = np.ceil((xmax-xmin)/res)

##-Time zone settings
nzTimeZones = pytz.country_timezones['nz']
nzTimeZones = nzTimeZones[0]

for fProduct in Fproducts:
    ncDir = os.path.join(ncRootDir, fProduct)
    #-get list of *.nc files in the directory of the forecast product
    ncFiles = glob.glob(ncDir + '\*.nc')
    fProdTifDir = os.path.join(tifDir, fProduct)
    
    #-create GTiff folder of that forecast product if it does not exist yet
    if not os.path.exists(fProdTifDir):
        os.mkdir(fProdTifDir)
        
    #-open logfile for writing errors during processing
    errorLog = open(os.path.join(fProdTifDir, logFile), 'w')        
    
    #-Get the dataframe of one nc file to extract lat lon and convert to NZTMX and NZTMY
    ncF = ncFiles[0]
    df = ncToDataFrame(ncF, subdataset='precipitation_amount', dropcols=['south_north', 'west_east'])
    df_short = df.copy(); df=None;
    t = pd.unique(df_short['time'])
    df_short = df_short.loc[df_short['time']==t[0]]
    #-Convert lat lon to NZTMY and NZTMX
    X = []
    Y = []
    for i in df_short.iterrows():
        [x,y] = reproject(4326, 2193, float(i[1]['longitude']), float(i[1]['latitude']))
        X.append(x)
        Y.append(y)
    df_short = None
    
    #-Loop over all the netCDF files in the folder and create GTiffs for each forecast time in that product
    for ncF in ncFiles:
        try:
            #-Get dataframe from netcdf
            df = ncToDataFrame(ncF, subdataset='precipitation_amount', dropcols=['south_north', 'west_east'])
            #-Array of unique timestamps
            Tunique = pd.unique(df['time'])
            #-Loop over the timestamps (forecasts) within the netcdf file.
            i = 0
            for t in Tunique:
                if i>0:
                    df_short = df.copy()
                    df2 = df_short.loc[df_short['time'] == Tunique[i]]
                    df1 = df_short.loc[df_short['time'] == Tunique[i-1]]
                    df_short = None
                    prec = df2['precipitation_amount'].values - df1['precipitation_amount'].values  #-calculate precipitation from difference
                    df_final = df2.copy(); df1 = None; df2 = None
                    df_final['precipitation_amount'] = prec; prec = None
                    #-Replace lat lon fields with NZTMX and NZTMY as calculated before
                    df_final.rename(columns={'latitude': 'NZTMY', 'longitude': 'NZTMX', 'precipitation_amount':'prec'}, inplace=True)
                    df_final['NZTMX'] = X
                    df_final['NZTMY'] = Y
                    #-Convert UTC to NZ timezone
                    df_final['time'] = df_final['time'].dt.tz_localize('utc')
                    df_final['time'] = df_final['time'].dt.tz_convert(nzTimeZones)
                    df_final['time'] = df_final['time'].dt.tz_localize(None)
                    #-Get NZ time of the forecast
                    forecastTime = df_final.iloc[0,0]
                    df_final.drop('time', axis=1, inplace=True)
                    #-Write to csv before converting to GTiff
                    df_final.to_csv(csvFile, index=False)
                    df_final = None
                    #-Get the UTC of the nc filename and convert to a datestime of NZ time zone
                    ncFileStr = ncF[-13:].split('.nc')[0]
                    year = int(ncFileStr[:4])
                    month = int(ncFileStr[4:6])
                    day = int(ncFileStr[6:8])
                    hour = int(ncFileStr[8:10])
                    fileTime = pd.Timestamp(year=year, month=month, day=day, hour=hour, tz='utc').tz_convert(nzTimeZones)
                    fileTime = fileTime.tz_localize(None)
                    #-Convert csv to GTiff
                    tifOut = os.path.join(fProdTifDir, str(i) + 'h_' + forecastTime.strftime('%Y%m%d_%H%M') + '_' + fileTime.strftime('%Y%m%d_%H%M')  + '.tif')
                    z = gdal.Grid(tifOut, vrtFile, width = cols, height=rows, algorithm='linear',format='GTiff', outputSRS='EPSG:2193', 
                               spatFilter=(xmin,ymin,xmax,ymax), zfield='prec')
                    z=None
                i+=1
                
        except:
            errorLog.write('%s could not be processed because of unknown file format\n' %ncF)
    errorLog.close()
