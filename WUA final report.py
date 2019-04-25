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
        # 'B1_ALT_ID',
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
        'R3_DEPTNAME': 'MonitoringDepartment',
        'REC_FUL_NAM': 'RMOA'
        }
InspectionImportFilter = {
        'Subtype': ['Water Use - Alert']
        }


ConsentCol = [
        'B1_ALT_ID',
        'MonOfficerDepartment',
        'MonOfficer'
        ]
ConsentColNames = {
        'B1_ALT_ID' :'ConsentNo'
        }
ConsentImportFilter = {

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




# Query SQL tables
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



# Joining Tables: AlertStatus and IgnoredConsents
Alert = pd.merge(Alert, AlertStatus, on ='StatusID', how = 'left')
Alert = pd.merge(Alert, IgnoredConsents, on ='ConsentNo', how = 'left')


Alert['Processed'] = Alert['Username'].notnull()


Alert['InspectionID'] = Alert['Comment'].str[:7]
#
Alert['InspectionID']= pd.to_numeric(Alert['InspectionID'],errors='coerce', downcast='integer')

WUAInspections = Alert['InspectionID'].dropna()

WUAInspections_List = WUAInspections.values.tolist()

 

#Import and join Inspections created by Alerts Process
LinkedInspection = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_Inspections',
                   col_names = InspectionCol,
                   where_in = {'InspectionID': WUAInspections_List}
                   )
LinkedInspection.rename(columns=InspectionColNames, inplace=True)


Alert = pd.merge(Alert, LinkedInspection, on ='InspectionID', how = 'left')



#Import and join Consents created by Alerts Process
Consent_List = Alert['ConsentNo'].unique().tolist()

Consent = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'F_ACC_Permit',
                   col_names = ConsentCol,
                   where_in = {'B1_ALT_ID': Consent_List}
                   )

Consent.rename(columns=ConsentColNames, inplace=True)

Alert = pd.merge(Alert, Consent, on ='ConsentNo', how = 'left')


#Report numbers




Alert.groupby(['IgnoreType'])['AlertID'].aggregate('count')
Alert.groupby(['Status'])['AlertID'].aggregate('count')



