# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 11:34:51 2019

@author: KatieSi

WUA Final Report

Output tables e-mailed to Regional Implementation
"""

# Import packages
import pandas as pd
import pdsql
from datetime import datetime, timedelta

# Set Variables
ReportName= 'WUA Final Report v1.0'
RunDate = datetime.now()


# Input Parameters
InspectionCol = [
        'InspectionID',
        'B1_ALT_ID',
        #'Subtype',
        'InspectionStatus',
        'InspectionCompleteDate',
        'R3_DEPTNAME',
        'GA_FNAME',
        'GA_LNAME',
        'REC_FUL_NAM',        
        'StatusDate'
        ]

InspectionColNames = {
        'B1_ALT_ID': 'ConsentNo',
        'R3_DEPTNAME': 'MonitoringDepartment',
        'REC_FUL_NAM': 'RMOA'
        }

InspectionImportFilter = {
        'Subtype': ['Water Use - Alert']
        }



AlertCol = [
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

AlertColNames = {
        'ID' : 'AlertID',
        'ConsentID or Water Group': 'ConsentNo'
        }

AlertImportFilter = {
        
        }



AlertStatusCol = [  
        'ID',
        'Description'
        ]

AlertStatusColNames = {
        'ID' : 'StatusID',
        'Description' : 'Status'
        }

AlertStatusImportFilter = {
        
        }



IgnoredConsentsCol = [
       # 'ID',
        'ConsentID_or_Water_Group',
       # 'IgnoreReasonID',
       # 'Comment',
       # 'CreatedBy',
       # 'StartDate',
       # 'EndDate',
        'ReasonID'
        ]


IgnoredConsentsColNames = {
        'ConsentID_or_Water_Group': 'ConsentNo',
        'ReasonID': 'IgnoreType'
        }

IgnoredConsentsImportFilter = {
        
        }

# Query SQL tables : FEP Individual
#Inspection = pdsql.mssql.rd_sql(
#                   'SQL2012PROD03',
#                   'DataWarehouse', 
#                   table = 'D_ACC_FEP_IndividualSummary',
#                   col_names = AuditCol,
#                   where_op = 'AND',
#                   where_in = AlertFilter,
#                   date_col = 'AuditDate',
#                   from_date = '2018-07-01'
#                   )

# Query SQL tables : FEP Individual
Inspection = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = InspectionCol,
                   where_op = 'AND',
                   where_in = InspectionImportFilter,
                   date_col = 'StatusDate',
                   from_date = '2019-01-01'
                   )
Inspection.rename(columns=InspectionColNames, inplace=True)


Alert = pdsql.mssql.rd_sql(
                   'SQL2012Test01',
                   'Hilltop', 
                   table = 'DailyAlert',
                   col_names = AlertCol
                   )
#NULL imports as 'None' but reacts like NULL in groupby
# Alert.groupby(['Username'])['ID'].aggregate('count')
Alert.rename(columns=AlertColNames, inplace=True)

AlertStatus = pdsql.mssql.rd_sql(
                   'SQL2012Test01',
                   'Hilltop', 
                   table = 'DailyAlertStatus',
                   col_names = AlertStatusCol,
                   )

AlertStatus.rename(columns=AlertStatusColNames, inplace=True)

IgnoredConsents = pdsql.mssql.rd_sql(
                   'SQL2012Test01',
                   'Hilltop', 
                   table = 'DailyAlertConsentsToFilter',
                   col_names = IgnoredConsentsCol
                   )
IgnoredConsents.rename(columns=IgnoredConsentsColNames, inplace=True)
IgnoredConsents.groupby(['IgnoreType'])['ConsentNo'].aggregate('count')

# linking status ID
Alert = pd.merge(Alert, AlertStatus, on ='StatusID', how = 'left')

Alert.groupby(['Status'])['AlertID'].aggregate('count')

#Pull out Processed Alerts and Inspection Numbers
Processed = Alert[Alert['Username'].notnull()]

Processed['InspectionID'] = Processed['Comment'].str[:7]
#
Processed['InspectionID']= pd.to_numeric(Processed['InspectionID'],errors='coerce', downcast='integer')

WUAInspections = Processed['InspectionID'].dropna()

WUAInspections_List = WUAInspections.values.tolist()

#Import Inspections created by Alerts Process
LinkedInspection = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = InspectionCol,
                   where_in = {'InspectionID': WUAInspections_List}
                   )
LinkedInspection.rename(columns=InspectionColNames, inplace=True)


Processed = pd.merge(Processed, LinkedInspection, on ='InspectionID', how = 'left')





Processed.groupby(['Status'])['AlertID'].aggregate('count')
# Linking Ignored Consents
Alert = pd.merge(Alert, IgnoredConsents, on ='ConsentNo', how = 'left')

Alert.groupby(['IgnoreType'])['AlertID'].aggregate('count')

#linking Inspection
Inspection.count()
Alert.groupby(['Status'])['AlertID'].aggregate('count')







#x = pd.merge(LinkedInspection, Processed, on ='InspectionID', how = 'left')
#
#
#
#x.to_csv('test.csv')




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
