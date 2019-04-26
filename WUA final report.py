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


# Data Import Parmaters
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
        'B1_ALT_ID' : 'ConsentNo',
        'MonOfficerDepartment': 'ConsentDepartment'
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
        'IgnoreReasonID',
       # 'Comment',
       # 'CreatedBy',
       # 'StartDate',
       # 'EndDate',
       # 'ReasonID'
        ]
IgnoredConsentsColNames = {
        'ConsentID_or_Water_Group': 'ConsentNo',
        'IgnoreReasonID': 'IgnoreType'
        }
IgnoredConsentsImportFilter = {
        
        }


AllocationCol = [  
        'crc',
        'feav'
        ]
AllocationColNames = {
        'crc' : 'ConsentNo',
        'feav' : 'AllocatedAnnualVolume'
        }
AllocationImportFilter = {
        
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




#Import and join Inspections created by Alerts Process

Alert['InspectionID'] = Alert['Comment'].str[:7]
Alert['InspectionID']= pd.to_numeric(Alert['InspectionID'],errors='coerce', downcast='integer')
WUAInspections = Alert['InspectionID'].dropna()

WUAInspections_List = WUAInspections.values.tolist()

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


Allocation = pdsql.mssql.rd_sql(
                   'EDWPROD01',
                   'Hydro', 
                   table = 'CrcAllo',
                   col_names = AllocationCol,
                   where_in = {'crc': Consent_List}
                   )

Allocation.rename(columns=AllocationColNames, inplace=True)

Allocation = Allocation.groupby(['ConsentNo'], as_index=False).agg({'AllocatedAnnualVolume': 'sum'})

Alert = pd.merge(Alert, Allocation, on ='ConsentNo', how = 'left')

IgnoredConsents = pd.merge(IgnoredConsents, Allocation, on ='ConsentNo', how = 'left')
IgnoredConsents = pd.merge(IgnoredConsents, Consent, on ='ConsentNo', how = 'left')


# CRC041857 missing allocation: 96040.00

len(Alert['ConsentNo'].unique().tolist())


Alert.to_csv('test.csv')

#Add Cycle
Alert['Week'] = Alert['RunDate'].dt.week
Alert['Cycle'] = "Cycle " + (Alert['Week']-3).astype(str)
Alert['Processed'] = Alert['Username'].notnull() 









#Report numbers



Alert.loc[Alert['Ignore'] == 0].shape[0]
Alert['Processed'].sum()
r_AlertsIgnored = Alert.loc[Alert['Ignore'] == 1].shape[0]

len(Alert['ConsentNo'].loc[Alert['Ignore'] == 0].unique().tolist())
len(Alert['ConsentNo'].loc[Alert['Processed'] == 1].unique().tolist())
len(Alert['ConsentNo'].loc[Alert['Ignore'] == 1].unique().tolist())
 = Alert.loc[Alert['Processed'] == 1 ].groupby(['Status'])['AlertID'].aggregate('count')
 = Alert.loc[Alert['Processed'] == 1 ].groupby(['InspectionStatus'])['AlertID'].aggregate('count')

AlertToProcess = Alert.loc[Alert['Ignore']==0]
AlertProcessed = Alert.loc[Alert['Processed']==1]
AlertIgnored = Alert.loc[Alert['Ignore']==1]
AlertEscaltated = Alert.loc[Alert['Status']== 'Escalated']
AlertInspected = Alert[Alert['InspectionID'].notnull()]
temp = AlertInspected[['ConsentNo','InspectionID','MonitoringDepartment','ConsentDepartment']]
UniqueInspections = temp.drop_duplicates()


AlertEscaltated.groupby(['MonitoringDepartment'])['InspectionID'].count()


r_AlertsGenerated = Alert.shape[0]
r_AlertsToProcess = AlertToProcess.shape[0]
r_AlertsProcessed = AlertProcessed.shape[0]
r_AlertsIgnored = AlertIgnored.shape[0]
r_AlertsEscalated = AlertEscaltated.shape[0]
r_ConsentsGenerated = len(Alert['ConsentNo'].unique().tolist())
r_ConsentsToProcess = len(AlertToProcess['ConsentNo'].unique().tolist())
r_ConsentsProcessed = len(AlertProcessed['ConsentNo'].unique().tolist())
r_ConsentsIgnored = len(AlertIgnored['ConsentNo'].unique().tolist())
r_ConsentsEscalated = len(AlertEscaltated['ConsentNo'].unique().tolist())

r_ProcessOutcomes = AlertProcessed.groupby(['Status'])['AlertID'].aggregate('count')
r_RMOA = AlertProcessed.groupby(['Username'])['AlertID'].aggregate('count')
r_InspectionOutcomes = AlertProcessed.groupby(['InspectionStatus'])['AlertID'].aggregate('count')
r_ConsentZone = UniqueInspections.groupby(['ConsentDepartment'])['InspectionID'].count()
r_MonitoringZone = UniqueInspections.groupby(['MonitoringDepartment'])['InspectionID'].count()


RepeatAlert = AlertProcessed.groupby(['ConsentNo'])['AlertID'] \
                             .count().reset_index(name='count')
RepeatAlert = RepeatAlert.loc[RepeatAlert['count'] >1] 
r_ConsentsRerocessed = RepeatAlert.shape[0]             
r_MaxConsentsReprocessed = RepeatAlert['count'].max()
r_AverageConsentReprocessed = int(round(RepeatAlert['count'].mean()))
r_EffortConsentReprocessed = RepeatAlert['count'].sum()-RepeatAlert.shape[0]


r_AlertIgnoredType = AlertIgnored.groupby(['IgnoreType'])['AlertID'].count().reset_index(name='count')
r_AlertIgnoredTypeCycle = Alert.groupby(['IgnoreType','Cycle'])['AlertID'].count().unstack()
r_ProcessOutcomesCycle = Alert.groupby(['Ignore','Status','Cycle'])['AlertID'].count().unstack()

r_ZoneIgnored = IgnoredConsents.groupby(['ConsentDepartment'])['ConsentNo'].count()

r_VolumeAlerted = Alert.groupby([])
r_VolumeToProcess = 
r_VolumeProcessed = 
r_VolumeIgnored = 
r_VolumeInspected = 





r_AlertsGeneratedCycle = Alert.groupby(['Cycle'])['AlertID'].count()
r_AlertsToProcessCycle = AlertToProcess.groupby(['Cycle'])['AlertID'].count()
r_AlertsProcessedCycle = AlertProcessed.groupby(['Cycle'])['AlertID'].count()





Alert[['RunDate','Week','Cycle']]


Alert.to_csv('test.csv')

.isocalendar()[1]
##Issues##

##number escalted
UniqueInspections
ConsentsEscalated

#important to note <5ls status
Alert.groupby(['Status'])['AlertID'].aggregate('count')






x= AlertProcessed.groupby(['ConsentNo'])['AlertID'].aggregate('count')

AlertProcessed.sort_values(['ConsentNo','count'],ascending=False).groupby(['ConsentNo'])['AlertID'].aggregate('count')







                             
Alert.groupby(['IgnoreType'])['AlertID'].count()


Alert.groupby(['IgnoreType'])['AlertID'].aggregate('count')
Alert.groupby(['Status'])['AlertID'].aggregate('count')


AlertProcessed.to_csv('test.csv')

