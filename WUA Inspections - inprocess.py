# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 11:34:51 2019

@author: KatieSi

Weekly WUA Inspection Outcomes
Due Thursday 10am until April 30th.

Run at 8:30 am Thursdays
Output tables e-mailed to Carly Waddleton
"""

# Import packages
import pandas as pd
import pdsql
from datetime import datetime, timedelta


# Set Variables
ReportName= 'WUA Inspections v1.0'
RunDate = datetime.now()

## Query Variables
InspCol = [
        'GA_FNAME',
        'GA_LNAME',
        'StatusDate',
        'InspectionID',
        'B1_ALT_ID',
        'InspectionStatus',
        'Subtype'
        ]

InspFilter = {
        'InspectionStatus': ['In process'],
         'Subtype': ['Water Use - Alert']
         }

## Dataframe Formatting Variables
InspColumnNames = {
        'B1_ALT_ID' : 'Consent',
        'GA_FNAME' : 'OfficerAssigned'
        }

InspColumnDrop = ['GA_LNAME','InspectionStatus','Subtype']



# Query SQL tables
WUAInspections = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = InspCol,
                   where_op = 'AND',
                   where_in = InspFilter,
                   date_col = 'StatusDate',
                   from_date = '2019-01-01'
                   )


# Reformatting dataframe
WUAInspections['GA_FNAME'] = (
        WUAInspections['GA_FNAME'].fillna('') +
        ' ' + 
         WUAInspections['GA_LNAME']
         )

WUAInspections.drop(InspColumnDrop, axis=1, inplace=True)
WUAInspections.rename(columns=InspColumnNames, inplace=True)


# Create aggragate info
WUAInspectionsCount = pd.DataFrame(
        WUAInspections.groupby(['OfficerAssigned'])['InspectionID'].count()
        )

WUAInspectionsCount.columns = ['Inspection Count']


# Print email
print('Hi Carly,\n\nBelow are the WUA inspections still in process as of ',
      datetime.strftime(datetime.now() - timedelta(days =1), '%d-%m-%Y'),
      '\n\nThere are',len(WUAInspections.index),'inspections this week.',
      '\n\n\n',WUAInspectionsCount,
      '\n\n\nWater Use - Alert inspections still in process\n\n',
      WUAInspections.to_string(index=False),
      '\n\n* Note: This report was created at ',
      RunDate.strftime("%H:%M %Y-%m-%d"), ' using ', ReportName,
      '\n\nCheers,\nKatie\n'
      )



