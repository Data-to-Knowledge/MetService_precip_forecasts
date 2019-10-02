#!/usr/bin/env python

#-Authorship information-###################################################################
__author__ = 'Wilco Terink'
__copyright__ = 'Wilco Terink'
__version__ = '1.0'
__email__ = 'wilco.terink@ecan.govt.nz'
__date__ = 'September 2019'
############################################################################################

import pdsql
import pandas as pd

'''
Gets ECan and NIWA precipitation station coordinates and time-series and writes those to csv-files
'''


server = 'edwprod01'
database = 'hydro'
 

from_date = '2018-01-01'
to_date = '2019-09-12'

sites_df = pdsql.mssql.rd_sql(server, database, 'TSDataNumericHourlySumm', col_names=['ExtSiteID', 'DatasetTypeID'], where_in={'DatasetTypeID': [38, 15]})

prec_ts_df = pdsql.mssql.rd_sql(server, database, 'TSDataNumericHourly', col_names=['ExtSiteID', 'DateTime', 'Value'], where_in={'DatasetTypeID': [38, 15],
                'ExtSiteID': pd.unique(sites_df.ExtSiteID).tolist(), 'QualityCode': [600]})
prec_ts_df['DateTime'] = pd.to_datetime(prec_ts_df['DateTime']) 
prec_ts_df = prec_ts_df.loc[(prec_ts_df.DateTime>=pd.Timestamp(from_date)) & (prec_ts_df.DateTime<=pd.Timestamp(to_date))]
prec_ts_df.to_csv(r'C:\Active\Projects\MetService_precip_analysis\Data\Stations\station_ts.csv', index=False)

#-Get the locations of the sites and write to csv
sites_xy =  pdsql.mssql.rd_sql(server, database, 'ExternalSite', col_names=['ExtSiteID', 'NZTMX', 'NZTMY'], where_in={'ExtSiteID': list(pd.unique(prec_ts_df.ExtSiteID))})
sites_xy.drop_duplicates(inplace=True)
sites_xy.to_csv(r'C:\Active\Projects\MetService_precip_analysis\Data\Stations\station_xy.csv', index=False)
