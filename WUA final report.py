# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 11:34:51 2019

@author: KatieSi

WUA Final Report

Output tables e-mailed to Carly Waddleton Eila Gendig
"""

# Import packages
import pandas as pd
import pdsql
from datetime import datetime, timedelta

# Set Variables
ReportName= 'WUA Final Report v1.0'
RunDate = datetime.now()


# Input Parameters
InspCols = [
        'InspectionID',
        'B1_ALT_ID',
        'Subtype',
        'InspectionStatus',
        'InspectionCompleteDate',
        'R3_DEPTNAME',
        'GA_FNAME',
        'GA_LNAME',
        'REC_FUL_NAM',        
        'StatusDate'
        ]

AlertCols = [
        'ID',
        'ConsentID or Water Group',
        'Meters',
        'Type',
        'Limit',
        'Max Use',
        'Percent Over Limit',
        'RunDate',
        'StatusID',
        'Ignore',
        'Comment',
        'Username',
        'LastModifiedDate'
        ]

AlertStatusCols = [
        
        
        
        
        ]

#
#WUAInspectionsFilter = {
#        'InspectionStatus': ['In process'],
#        'Subtype': ['Water Use - Alert']
#        }
#
## Query SQL tables
#WUA_Inspections = pdsql.mssql.rd_sql(
#        'SQL2012PROD03',
#        'DataWarehouse', 
#        table = 'D_ACC_Inspections',
#        col_names = Insp_col,
#        where_op = 'AND',
#        where_in = WUA_Inspections_Filter,
#        date_col = 'StatusDate',
#        from_date = '2019-01-01'
#        )
#
#
## Reformat dataframe
#WUA_Inspections['GA_FNAME'] = (
#        WUA_Inspections['GA_FNAME'].fillna('') +
#        ' ' + 
#         WUA_Inspections['GA_LNAME']
#         )
#
#WUA_Inspections.drop('GA_LNAME', axis=1, inplace=True)
#WUA_Inspections.drop('InspectionStatus', axis=1, inplace=True)
#WUA_Inspections.drop('Subtype', axis=1, inplace=True)
#WUA_Inspections.rename(columns={'B1_ALT_ID' : 'Consent'}, inplace=True)
#WUA_Inspections.rename(columns={'GA_FNAME' : 'OfficerAssigned'}, inplace=True)
#
#
## Create aggragate info
#WUA_Inspections_count = pd.DataFrame(
#        WUA_Inspections.groupby(['OfficerAssigned'])['InspectionID'].count())
##### Missing the (nospecific officer)
#
#WUA_Inspections_count.columns = ['Inspection Count']
#
#
## Print email
#print('Hi Carly,\n\n Below are the WUA inspections still in process as of ',
#      datetime.strftime(datetime.now() - timedelta(days =1), '%d-%m-%Y'),
#      '\n\nThere are',len(WUA_Inspections.index),'inspections this week.'
#      '\n\n\n',WUA_Inspections_count,
#      '\n\n\nWater Use - Alert inspections still in process\n\n',
#      WUA_Inspections.to_string(index=False),
#      '\n\n\n\nCheers,\nKatie')
##
##
##
##
