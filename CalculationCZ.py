# -*- coding: utf-8 -*-
"""
Created on Thu Jul 09 13:28:05 2017

@author: Marie Jankujova
"""

import sys
import os
from datetime import datetime
import sqlite3

try:
    import pandas as pd
except ImportError:
    print("Please, install the package pandas!")
    
try:
    import numpy as np
    from numpy import inf
except ImportError:
    print("Please, install the package numpy!")

class ComputeStats:
    """
    This module compute statistics from the raw data. 
    
    Keyword arguments:
    df -- the raw dataframe
    country -- True if country should be included as site into results (default False)
    country_code -- the country code of country if data were filtered (default "")
    comparison -- True if we are compare the data (e.g. countries between each other)
    """

    def __init__(self, df, country = False, country_code = "", comparison=False):
        
        self.df = df.copy()
        self.df.fillna(0, inplace=True)

        # Get short Protocol IDs - it means RESQ-v - PL_001 will be PL_001
        
        if comparison == False:
            self.df['Protocol ID'] = self.df.apply(lambda row: row['Protocol ID'].split()[2] if (len(row['Protocol ID'].split()) == 3) else row['Protocol ID'].split()[0], axis=1)
            # uncomment if you want stats between countries and set comparison == True
            #self.df['Protocol ID'] = self.df.apply(lambda x: x['Protocol ID'].split("_")[0], axis=1)
        #print(set(list(self.df['Protocol ID'])))
        
        if (country):
            country = self.df.copy()
            self.country_name = pytz.country_names[country_code]
            country['Protocol ID'] = self.country_name
            country['Site Name'] = self.country_name
            self.df = pd.concat([self.df, country])
        else:
            self.country_name = ""
        
        # Get Protocol IDs and Total Patients 
        self.statsDf = self.df.groupby(['Protocol ID', 'Site Name']).size().reset_index(name="Total Patients")

        #self.statsDf['Site Name'] = 

        self.statsDf = self.statsDf[['Protocol ID', 'Site Name', 'Total Patients']]

        # If you want to compare, instead of Site Names will be Country names. 
        if comparison:
            self.statsDf['Site Name'] = self.statsDf.apply(lambda x: pytz.country_names[x['Protocol ID']] if pytz.country_names[x['Protocol ID']] != "" else x['Protocol ID'], axis=1)
        
        # Median age
        self.statsDf['Median patient age'] = self.df.groupby(['Protocol ID']).AGE.agg(['median']).rename(columns={'median': 'Median patient age'})['Median patient age'].tolist()

        # get patietns with ischemic stroke (ISch) (1)
        isch = self.df[self.df['STROKE_TYPE'].isin([1])]
        self.statsDf['isch_patients'] = self._count_patients(dataframe=isch)

        # get patietns with ischemic stroke (IS), intracerebral hemorrhage (ICH), transient ischemic attack (TIA) or cerebral venous thrombosis (CVT) (1, 2, 3, 5)
        is_ich_tia_cvt = self.df[self.df['STROKE_TYPE'].isin([1, 2, 3, 5])]
        self.statsDf['is_ich_tia_cvt_patients'] = self._count_patients(dataframe=is_ich_tia_cvt)

        # get patietns with ischemic stroke (IS), intracerebral hemorrhage (ICH), or cerebral venous thrombosis (CVT) (1, 2, 5)
        is_ich_cvt = self.df[self.df['STROKE_TYPE'].isin([1, 2, 5])]
        self.statsDf['is_ich_cvt_patients'] = self._count_patients(dataframe=is_ich_cvt)

        # Get dataframe with patients who had ischemic stroke (IS) or intracerebral hemorrhage (ICH)
        is_ich = self.df[self.df['STROKE_TYPE'].isin([1,2])]
        self.statsDf['is_ich_patients'] = self._count_patients(dataframe=is_ich)

        # get patietns with ischemic stroke (IS) and transient ischemic attack (TIA) (1, 3)
        is_tia = self.df[self.df['STROKE_TYPE'].isin([1,3])]
        self.statsDf['is_tia_patients'] = self._count_patients(dataframe=is_tia)

        # get patietns with ischemic stroke (IS), intracerebral hemorrhage (ICH), subarrachnoid hemorrhage (SAH) or cerebral venous thrombosis (CVT) (1, 2, 4, 5)
        is_ich_sah_cvt = self.df[self.df['STROKE_TYPE'].isin([1, 2, 4, 5])]
        self.statsDf['is_ich_sah_cvt_patients'] = self._count_patients(dataframe=is_ich_sah_cvt)

        # get patietns with ischemic stroke (IS), transient ischemic attack (TIA) or cerebral venous thrombosis (CVT) (1, 3, 5)
        is_tia_cvt = self.df[self.df['STROKE_TYPE'].isin([1, 3, 5])]
        self.statsDf['is_tia_cvt_patients'] = self._count_patients(dataframe=is_tia_cvt)

        # get patients with cerebral venous thrombosis (CVT) (5)
        cvt = self.df[self.df['STROKE_TYPE'].isin([5])]
        self.statsDf['cvt_patients'] = self._count_patients(dataframe=cvt)

        # get patietns with intracerebral hemorrhage (ICH) and subarrachnoid hemorrhage (SAH) (2, 4)
        ich_sah = self.df[self.df['STROKE_TYPE'].isin([2, 4])]
        self.statsDf['ich_sah_patients'] = self._count_patients(dataframe=ich_sah)
        
        # get patietns with intracerebral hemorrhage (ICH) (2)
        ich = self.df[self.df['STROKE_TYPE'].isin([2])]
        self.statsDf['ich_patients'] = self._count_patients(dataframe=ich)

        # get patietns with subarrachnoid hemorrhage (SAH) (4)
        sah = self.df[self.df['STROKE_TYPE'].isin([4])]
        self.statsDf['sah_patients'] = self._count_patients(dataframe=sah)

        # create subset with no referrals (RECANALIZATION_PROCEDURE != [5,6]) AND (HEMICRANIECTOMY != 3)
        discharge_subset = self.df[~self.df['RECANALIZATION_PROCEDURES'].isin([5, 6]) & ~self.df['HEMICRANIECTOMY'].isin([3])]
        self.statsDf['discharge_subset_patients'] = self._count_patients(dataframe=discharge_subset)

        # Create discharge subset alive
        discharge_subset_alive = self.df[~self.df['DISCHARGE_DESTINATION'].isin([5])]
        self.statsDf['discharge_subset_alive_patients'] = self._count_patients(dataframe=discharge_subset_alive)


        ##########
        # GENDER #
        ##########
        # Get gender calculation in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'GENDER']).size().to_frame('count').reset_index()

        # Get female patients 
        self.statsDf = self._get_values_for_factors(column_name="GENDER", value=2, new_column_name='# patients female')

        # Get only female patients in % out of total patients. 
        self.statsDf['% patients female'] = self.statsDf.apply(lambda x: round(((x['# patients female']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get male patients 
        self.statsDf = self._get_values_for_factors(column_name="GENDER", value=1, new_column_name='# patients male')

        # Get % of male patients
        self.statsDf['% patients male'] = self.statsDf.apply(lambda x: round(((x['# patients male']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        #self.statsDf['sum'] = self.statsDf['% patients female'] + self.statsDf['% patients male'] 
        #self.statsDf['% patients female'] = self.statsDf.apply(lambda x: x['% patients female']-0.01 if x['sum'] > 100.00 else x['% patients female'], axis=1)

        # Drop tmp column 
        #self.statsDf.drop(['sum'], inplace=True, axis=1)


        ######################
        # STROKE IN HOSPITAL #
        ######################
        # Get hospital stoke in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'HOSPITAL_STROKE']).size().to_frame('count').reset_index()

        # Get patients having stroke in the hospital - Yes
        self.statsDf = self._get_values_for_factors(column_name="HOSPITAL_STROKE", value=1, new_column_name='# patients having stroke in the hospital - Yes')

        # Get % patients having stroke in the hospital - Yes
        self.statsDf['% patients having stroke in the hospital - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients having stroke in the hospital - Yes']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get patients having stroke in the hospital - No
        self.statsDf = self._get_values_for_factors(column_name="HOSPITAL_STROKE", value=2, new_column_name='# patients having stroke in the hospital - No')

        # Get % patients having stroke in the hospital - Yes
        self.statsDf['% patients having stroke in the hospital - No'] = self.statsDf.apply(lambda x: round(((x['# patients having stroke in the hospital - No']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        ####################
        # RECURRENT STROKE #
        ####################
        # Get recurrent stoke in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'RECURRENT_STROKE']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=-999, new_column_name='tmp')

        # Get patients having recurrent stroke - Yes
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=1, new_column_name='# recurrent stroke - Yes')

        # Get % patients having recurrent stroke - Yes
        self.statsDf['% recurrent stroke - Yes'] = self.statsDf.apply(lambda x: round(((x['# recurrent stroke - Yes']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get ppatients having recurrent stroke - No
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=2, new_column_name='# recurrent stroke - No')

        # Get % patients having recurrent stroke - No
        self.statsDf['% recurrent stroke - No'] = self.statsDf.apply(lambda x: round(((x['# recurrent stroke - No']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # DEPARTMENT TYPE #
        ###################
        # Get department type in one dataframe
        self.tmp = self.df.groupby(['Protocol ID', 'DEPARTMENT_TYPE']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=-999, new_column_name='tmp')

        # Get patients in neurology
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=1, new_column_name='# department type - neurology')

        # Get % patients in neurology
        self.statsDf['% department type - neurology'] = self.statsDf.apply(lambda x: round(((x['# department type - neurology']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients in neurosurgery
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=2, new_column_name='# department type - neurosurgery')

        # Get % patients in neurosurgery
        self.statsDf['% department type - neurosurgery'] = self.statsDf.apply(lambda x: round(((x['# department type - neurosurgery']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients in anesthesiology/resuscitation/critical care
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=3, new_column_name='# department type - anesthesiology/resuscitation/critical care')

        # Get % patients in anesthesiology/resuscitation/critical care
        self.statsDf['% department type - anesthesiology/resuscitation/critical care'] = self.statsDf.apply(lambda x: round(((x['# department type - anesthesiology/resuscitation/critical care']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients in internal medicine
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=4, new_column_name='# department type - internal medicine')

        # Get % patients in internal medicine
        self.statsDf['% department type - internal medicine'] = self.statsDf.apply(lambda x: round(((x['# department type - internal medicine']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients in geriatrics
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=5, new_column_name='# department type - geriatrics')

        # Get % patients in geriatrics
        self.statsDf['% department type - geriatrics'] = self.statsDf.apply(lambda x: round(((x['# department type - geriatrics']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients in Other
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=6, new_column_name='# department type - Other')

        # Get % patients in Other
        self.statsDf['% department type - Other'] = self.statsDf.apply(lambda x: round(((x['# department type - Other']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # HOSPITALIZED IN #
        ###################
        # Get hospitalized in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'HOSPITALIZED_IN']).size().to_frame('count').reset_index()

        # Get patients hospitalized in stroke unit / ICU
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=1, new_column_name='# patients hospitalized in stroke unit / ICU')

        # Get % patients hospitalized in stroke unit / ICU
        self.statsDf['% patients hospitalized in stroke unit / ICU'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in stroke unit / ICU']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)


        # Get patients hospitalized in monitored bed with telemetry
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=2, new_column_name='# patients hospitalized in monitored bed with telemetry')

        # Get % patients hospitalized in monitored bed with telemetry
        self.statsDf['% patients hospitalized in monitored bed with telemetry'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in monitored bed with telemetry']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get patients hospitalized in standard bed
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=3, new_column_name='# patients hospitalized in standard bed')

        # Get % patients hospitalized in standard bed
        self.statsDf['% patients hospitalized in standard bed'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in standard bed']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Check if sum of % is > 100 (100.01)
        #self.statsDf['sum'] = self.statsDf['% patients hospitalized in stroke unit / ICU'] + self.statsDf['% patients hospitalized in monitored bed with telemetry'] + self.statsDf['% patients hospitalized in standard bed']
        #self.statsDf['% patients hospitalized in stroke unit / ICU'] = self.statsDf.apply(lambda x: x['% patients hospitalized in stroke unit / ICU']-0.01 if x['sum'] > 100.00 else x['% patients hospitalized in stroke unit / ICU'], axis=1)

        # Drop tmp column 
        #self.statsDf.drop(['sum'], inplace=True, axis=1)


        ###############################
        # ASSESSED FOR REHABILITATION #
        ###############################
        # Get assessed for rehabilitation in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'ASSESSED_FOR_REHAB']).size().to_frame('count').reset_index()

        # Get patients assessed for rehabilitation - Not known
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=3, new_column_name='# patients assessed for rehabilitation - Not known')

        # Get % patients assessed for rehabilitation - Not known
        self.statsDf['% patients assessed for rehabilitation - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - Not known']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get patients assessed for rehabilitation - Yes
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=1, new_column_name='# patients assessed for rehabilitation - Yes')

        # Get % patients assessed for rehabilitation - Yes
        self.statsDf['% patients assessed for rehabilitation - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - Yes']/(x['Total Patients'] - x['# patients assessed for rehabilitation - Not known'])) * 100), 2) if (x['Total Patients'] - x['# patients assessed for rehabilitation - Not known']) > 0 else 0, axis=1)

        # Get patients assessed for rehabilitation - No
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=2, new_column_name='# patients assessed for rehabilitation - No')

        # Get % patients assessed for rehabilitation - No
        self.statsDf['% patients assessed for rehabilitation - No'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - No']/(x['Total Patients'] - x['# patients assessed for rehabilitation - Not known'])) * 100), 2) if (x['Total Patients'] - x['# patients assessed for rehabilitation - Not known']) > 0 else 0, axis=1)

        ###############
        # STROKE TYPE #
        ###############
        # Get stroke type in one table 
        self.tmp = self.df.groupby(['Protocol ID', 'STROKE_TYPE']).size().to_frame('count').reset_index()

        # Get stroke type - ischemic stroke
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=1, new_column_name='# stroke type - ischemic stroke')

        # Get % stroke type - ischemic stroke
        self.statsDf['% stroke type - ischemic stroke'] = self.statsDf.apply(lambda x: round(((x['# stroke type - ischemic stroke']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get stroke type - intracerebral hemorrhage
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=2, new_column_name='# stroke type - intracerebral hemorrhage')

        # Get % stroke type - intracerebral hemorrhage
        self.statsDf['% stroke type - intracerebral hemorrhage'] = self.statsDf.apply(lambda x: round(((x['# stroke type - intracerebral hemorrhage']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get stroke type - transient ischemic attack
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=3, new_column_name='# stroke type - transient ischemic attack')

        # Get % stroke type - transient ischemic attack
        self.statsDf['% stroke type - transient ischemic attack'] = self.statsDf.apply(lambda x: round(((x['# stroke type - transient ischemic attack']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get stroke type - subarrachnoid hemorrhage
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=4, new_column_name='# stroke type - subarrachnoid hemorrhage')

        # Get % stroke type - subarrachnoid hemorrhage
        self.statsDf['% stroke type - subarrachnoid hemorrhage'] = self.statsDf.apply(lambda x: round(((x['# stroke type - subarrachnoid hemorrhage']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get stroke type - cerebral venous thrombosis
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=5, new_column_name='# stroke type - cerebral venous thrombosis')

        # Get % stroke type - cerebral venous thrombosis
        self.statsDf['% stroke type - cerebral venous thrombosis'] = self.statsDf.apply(lambda x: round(((x['# stroke type - cerebral venous thrombosis']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        # Get stroke type - undetermined stroke
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=6, new_column_name='# stroke type - undetermined stroke')

        # Get % stroke type - undetermined stroke
        self.statsDf['% stroke type - undetermined stroke'] = self.statsDf.apply(lambda x: round(((x['# stroke type - undetermined stroke']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        #######################
        # CONSCIOUSNESS LEVEL #
        #######################
        # Get consciousness level in one table 
        self.tmp = is_ich_sah_cvt.groupby(['Protocol ID', 'CONSCIOUSNESS_LEVEL']).size().to_frame('count').reset_index()

        # Get level of consciousness - not known
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=5, new_column_name='# level of consciousness - not known')

        # Get % level of consciousness - not known
        self.statsDf['% level of consciousness - not known'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - not known']/x['is_ich_sah_cvt_patients']) * 100), 2) if x['is_ich_sah_cvt_patients'] > 0 else 0, axis=1)

        # Get level of consciousness - alert
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=1, new_column_name='# level of consciousness - alert')

        # Get % level of consciousness - alert
        self.statsDf['% level of consciousness - alert'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - alert']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        # Get level of consciousness - drowsy
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=2, new_column_name='# level of consciousness - drowsy')

        # Get % level of consciousness - drowsy
        self.statsDf['% level of consciousness - drowsy'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - drowsy']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        # Get level of consciousness - comatose
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=3, new_column_name='# level of consciousness - comatose')

        # Get % level of consciousness - comatose
        self.statsDf['% level of consciousness - comatose'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - comatose']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        # Get level of consciousness - GCS
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=4, new_column_name='# level of consciousness - GCS')

        # Get % level of consciousness - GCS
        self.statsDf['% level of consciousness - GCS'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - GCS']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        #######
        # GCS #
        #######
        # GCS subset
        gcs = is_ich_sah_cvt[is_ich_sah_cvt['CONSCIOUSNESS_LEVEL'].isin([4])].copy()

        # Get GCS patients
        self.statsDf['gcs_patients'] = self._count_patients(dataframe=gcs)

        # Get GCS in one table 
        self.tmp = gcs.groupby(['Protocol ID', 'GCS']).size().to_frame('count').reset_index()

        # Get GCS - 15-13
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=1, new_column_name='# GCS - 15-13')

        # Get % GCS - 15-13
        self.statsDf['% GCS - 15-13'] = self.statsDf.apply(lambda x: round(((x['# GCS - 15-13']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)

        # Get GCS - 12-8
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=2, new_column_name='# GCS - 12-8')

        # Get % GCS - 12-8
        self.statsDf['% GCS - 12-8'] = self.statsDf.apply(lambda x: round(((x['# GCS - 12-8']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)
        
        # Get GCS - <8
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=3, new_column_name='# GCS - <8')

        # Get % GCS - <8
        self.statsDf['% GCS - <8'] = self.statsDf.apply(lambda x: round(((x['# GCS - <8']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['gcs_patients'], inplace=True, axis=1)

        # GCS is mapped to the consciousness level. GCS 15-13 is mapped to alert
        self.statsDf['alert_all'] = self.statsDf['# level of consciousness - alert'] + self.statsDf['# GCS - 15-13']

        self.statsDf['alert_all_perc'] = self.statsDf.apply(lambda x: round(((x['alert_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        #  12-8 to drowsy 
        self.statsDf['drowsy_all'] = self.statsDf['# level of consciousness - drowsy'] + self.statsDf['# GCS - 12-8']

        self.statsDf['drowsy_all_perc'] = self.statsDf.apply(lambda x: round(((x['drowsy_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        # <8 to comatose. 
        self.statsDf['comatose_all'] = self.statsDf['# level of consciousness - comatose'] + self.statsDf['# GCS - <8']

        self.statsDf['comatose_all_perc'] = self.statsDf.apply(lambda x: round(((x['comatose_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        #self.statsDf['sum'] = self.statsDf['alert_all_perc'] + self.statsDf['drowsy_all_perc'] + self.statsDf['comatose_all_perc']
        #self.statsDf['alert_all_perc'] = self.statsDf.apply(lambda x: x['alert_all_perc']-0.01 if x['sum'] > 100.00 else x['alert_all_perc'], axis=1)

        # Drop tmp column 
        #self.statsDf.drop(['sum'], inplace=True, axis=1)

        #########
        # NIHSS #
        #########
        # Get NIHSS patients
        # Get nihss in one table 
        self.tmp = is_ich.groupby(['Protocol ID', 'NIHSS']).size().to_frame('count').reset_index()

        # Get Not performed 
        self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=1, new_column_name='# NIHSS - Not performed')

        # Get % Not performed   
        self.statsDf['% NIHSS - Not performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not performed']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)

        # Get Prformed  
        self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=2, new_column_name='# NIHSS - Performed')

        # Get % Performed   
        self.statsDf['% NIHSS - Performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Performed']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)

        # Get Not known 
        self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=3, new_column_name='# NIHSS - Not known')

        # Get % Not known   
        self.statsDf['% NIHSS - Not known'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not known']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)

        # Get NIHSS performed subset
        nihss = is_ich[is_ich['NIHSS'].isin([2])]

        # Get Median of NIHSS score
        tmpDf = nihss.groupby(['Protocol ID']).NIHSS_SCORE.agg(['median']).rename(columns={'median': 'NIHSS median score'})
        #self.statsDf['NIHSS median score'] = nihss.groupby(['Protocol ID']).NIHSS_SCORE.agg(['median']).rename(columns={'median': 'NIHSS median score'})['NIHSS median score'].tolist()
        factorDf = self.statsDf.merge(tmpDf, how='outer', left_on='Protocol ID', right_on='Protocol ID')
        factorDf.fillna(0, inplace=True)
        self.statsDf['NIHSS median score'] = factorDf['NIHSS median score']

        ##########
        # CT/MRI #
        ##########
        # Get CT/MRI in one table
        self.tmp = is_ich_tia_cvt.groupby(['Protocol ID', 'CT_MRI']).size().to_frame('count').reset_index()

        # Get patients CT/MRI not performed
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=1, new_column_name='# CT/MRI - Not performed')

        # Get % CT/MRI Not performed    
        self.statsDf['% CT/MRI - Not performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Not performed']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)

        # Get patients CT/MRI performed
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=2, new_column_name='# CT/MRI - performed')

        # Get % CT/MRI performed    
        self.statsDf['% CT/MRI - performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - performed']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)

        # Get patients CT/MRI not known
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=3, new_column_name='# CT/MRI - Not known')

        # Get % CT/MRI not known    
        self.statsDf['% CT/MRI - Not known'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Not known']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)

        # Get CT/MRI performed subset
        ct_mri = is_ich_tia_cvt[is_ich_tia_cvt['CT_MRI'].isin([2])]

        # Get CT/MRI time in one table
        self.tmp = ct_mri.groupby(['Protocol ID', 'CT_TIME']).size().to_frame('count').reset_index()
        #print(self.tmp)

        # Get patients CT/MRI within first hour after admission
        self.statsDf = self._get_values_for_factors(column_name="CT_TIME", value=1, new_column_name='# CT/MRI - Performed within 1 hour after admission')

        # Get % CT/MRI within first hour after admission    
        self.statsDf['% CT/MRI - Performed within 1 hour after admission'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Performed within 1 hour after admission']/x['# CT/MRI - performed']) * 100), 2) if x['# CT/MRI - performed'] > 0 else 0, axis=1)

        # Get patients CT/MRI within first hour after admission
        self.statsDf = self._get_values_for_factors(column_name="CT_TIME", value=2, new_column_name='# CT/MRI - Performed later than 1 hour after admission')

        # Get % CT/MRI within first hour after admission    
        self.statsDf['% CT/MRI - Performed later than 1 hour after admission'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Performed later than 1 hour after admission']/x['# CT/MRI - performed']) * 100), 2) if x['# CT/MRI - performed'] > 0 else 0, axis=1)

        ####################
        # VASCULAR IMAGING #
        ####################
        self.tmp = ich_sah.groupby(['Protocol ID', 'CTA_MRA_DSA']).size().to_frame('count').reset_index()
        
        # Get patients with vascular imaging - CTA
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'1', '1,2', '1,3'}, new_column_name='# vascular imaging - CTA')

        # Get % patients with vascular imaging - CTA
        self.statsDf['% vascular imaging - CTA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - CTA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)

        # Get patients with vascular imaging - MRA
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'2', '1,2', '2,3'}, new_column_name='# vascular imaging - MRA')

        # Get % patients with vascular imaging - MRA
        self.statsDf['% vascular imaging - MRA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - MRA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)

        # Get patients with vascular imaging - DSA
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'3', '1,3', '2,3'}, new_column_name='# vascular imaging - DSA')

        # Get % patients with vascular imaging - DSA
        self.statsDf['% vascular imaging - DSA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - DSA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)

        # Get patients with vascular imaging - None
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'4'}, new_column_name='# vascular imaging - None')

        # Get % patients with vascular imaging - None   
        self.statsDf['% vascular imaging - None'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - None']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)

        # Get patients with vascular imaging - two modalities
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'1,2', '1,3', '2,3'}, new_column_name='# vascular imaging - two modalities')

        # Get % patients with vascular imaging - two modalities
        self.statsDf['% vascular imaging - two modalities'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - two modalities']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)

        ### DATA NORMLAIZATION
        norm_tmp = self.statsDf[['% vascular imaging - CTA', '% vascular imaging - MRA', '% vascular imaging - DSA', '% vascular imaging - None']].copy()

        norm_tmp.loc[:,'rowsums'] = norm_tmp.sum(axis=1)

        self.statsDf['vascular_imaging_cta_norm'] = ((norm_tmp['% vascular imaging - CTA']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vascular_imaging_mra_norm'] = ((norm_tmp['% vascular imaging - MRA']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vascular_imaging_dsa_norm'] = ((norm_tmp['% vascular imaging - DSA']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vascular_imaging_none_norm'] = ((norm_tmp['% vascular imaging - None']/norm_tmp['rowsums']) * 100).round(decimals=2)

        ##############
        # VENTILATOR #
        ##############
        self.tmp = is_ich.groupby(['Protocol ID', 'VENTILATOR']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=-999, new_column_name='tmp')

        # Get patients put on ventilator - Not known
        self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=3, new_column_name='# patients put on ventilator - Not known')

        # Get % patients put on ventilator - Not known  
        self.statsDf['% patients put on ventilator - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Not known']/(x['is_ich_patients'] - x['tmp'])) * 100), 2) if (x['is_ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients put on ventilator - Yes
        self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=1, new_column_name='# patients put on ventilator - Yes')

        # Get % patients put on ventilator - Yes    
        self.statsDf['% patients put on ventilator - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Yes']/(x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)

        # Get patients put on ventilator - No
        self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=2, new_column_name='# patients put on ventilator - No')

        # Get % patients put on ventilator - No 
        self.statsDf['% patients put on ventilator - No'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - No']/(x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        #############################
        # RECANALIZATION PROCEDURES #
        #############################
        self.tmp = isch.groupby(['Protocol ID', 'RECANALIZATION_PROCEDURES']).size().to_frame('count').reset_index()

        # Get patients recanalization procedures - Not done
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=1, new_column_name='# recanalization procedures - Not done')

        # Get % patients recanalization procedures - Not done
        self.statsDf['% recanalization procedures - Not done'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Not done']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - IV tPa
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=2, new_column_name='# recanalization procedures - IV tPa')

        # Get % patients recanalization procedures - IV tPa
        self.statsDf['% recanalization procedures - IV tPa'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - IV tPa + endovascular treatment
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=3, new_column_name='# recanalization procedures - IV tPa + endovascular treatment')

        # Get % patients recanalization procedures - IV tPa + endovascular treatment
        self.statsDf['% recanalization procedures - IV tPa + endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa + endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - Endovascular treatment alone
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=4, new_column_name='# recanalization procedures - Endovascular treatment alone')

        # Get % patients recanalization procedures - Endovascular treatment alone
        self.statsDf['% recanalization procedures - Endovascular treatment alone'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Endovascular treatment alone']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - IV tPa + referred to another centre for endovascular treatment
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=5, new_column_name='# recanalization procedures - IV tPa + referred to another centre for endovascular treatment')

        # Get % patients recanalization procedures - IV tPa + referred to another centre for endovascular treatment
        self.statsDf['% recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - Referred to another centre for endovascular treatment
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=6, new_column_name='# recanalization procedures - Referred to another centre for endovascular treatment')

        # Get % patients recanalization procedures - Referred to another centre for endovascular treatment
        self.statsDf['% recanalization procedures - Referred to another centre for endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred to another centre for endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=7, new_column_name='# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre')

        # Get % patients recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre
        self.statsDf['% recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=8, new_column_name='# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre')

        # Get % patients recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre
        self.statsDf['% recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=9, new_column_name='# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre')

        # Get % patients recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre
        self.statsDf['% recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients recanalized
        self.statsDf['# patients recanalized'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] +  x['# recanalization procedures - Endovascular treatment alone'], axis=1)

        # Get % patients recanalized
        self.statsDf['% patients recanalized'] = self.statsDf.apply(lambda x: round(((x['# patients recanalized']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']) > 0 else 0, axis=1)

        ##############
        # MEDIAN DTN #
        ##############
        # Get patients receiving IV tpa
        self.statsDf.loc[:, '# IV tPa'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)

        # Get patients receiving IV tpa %
        self.statsDf['% IV tPa'] = self.statsDf.apply(lambda x: round(((x['# IV tPa']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get only patients recanalized
        recanalization_procedure_iv_tpa = isch[isch['RECANALIZATION_PROCEDURES'].isin([2, 3, 5])].copy()

        # Replace NA values by 0
        recanalization_procedure_iv_tpa.fillna(0, inplace=True)

        recanalization_procedure_iv_tpa['IVTPA'] = recanalization_procedure_iv_tpa['IVT_ONLY_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_ONLY_NEEDLE_TIME_MIN'] + recanalization_procedure_iv_tpa['IVT_TBY_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_TBY_NEEDLE_TIME_MIN'] + recanalization_procedure_iv_tpa['IVT_TBY_REFER_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_TBY_REFER_NEEDLE_TIME_MIN']
        #print(recanalization_procedure_iv_tpa['IVTPA'].tolist())

        tmp = recanalization_procedure_iv_tpa.groupby(['Protocol ID']).IVTPA.agg(['median']).rename(columns={'median': 'Median DTN (minutes)'}).reset_index()
        #print(tmp)
        self.statsDf = self.statsDf.merge(tmp, how='outer')
        self.statsDf.fillna(0, inplace=True)
        
        ##############
        # MEDIAN DTG #
        ##############
        # Get patients receiving TBY
        self.statsDf.loc[:, '# TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + endovascular treatment'], axis=1)

        # Get patients receiving TBY %
        self.statsDf['% TBY'] = self.statsDf.apply(lambda x: round(((x['# TBY']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get only patients recanalized TBY
        recanalization_procedure_tby_dtg = isch[isch['RECANALIZATION_PROCEDURES'].isin([4, 3])].copy()
        recanalization_procedure_tby_dtg.fillna(0, inplace=True)

        # Get IVTPA in minutes
        recanalization_procedure_tby_dtg['TBY'] = recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_TIME_MIN'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME_MIN']
        #print(recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['TBY'] > 0]['TBY'].tolist())

        #recanalization_procedure_tby['TBY'] = recanalization_procedure_tby.loc[:, ['TBY_ONLY_GROIN_PUNCTURE_TIME', 'TBY_ONLY_GROIN_PUNCTURE_TIME_MIN', 'IVT_TBY_GROIN_TIME', 'IVT_TBY_GROIN_TIME_MIN']].sum(1).reset_index()[0].tolist()

        tmp = recanalization_procedure_tby_dtg.groupby(['Protocol ID']).TBY.agg(['median']).rename(columns={'median': 'Median DTG (minutes)'}).reset_index()
        #print(tmp)
        self.statsDf = self.statsDf.merge(tmp, how='outer')
        self.statsDf.fillna(0, inplace=True)

        ###############
        # MEDIAN DIDO #
        ###############
        # Get patients receiving dido
        self.statsDf.loc[:, '# DIDO TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'], axis=1)

        # Get patients receiving TBY %
        self.statsDf['% DIDO TBY'] = self.statsDf.apply(lambda x: round(((x['# DIDO TBY']/(x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done']) > 0 else 0, axis=1)

        # Get only patients recanalized TBY
        recanalization_procedure_tby_dido = isch[isch['RECANALIZATION_PROCEDURES'].isin([5, 6, 7, 8])].copy()
        recanalization_procedure_tby_dido.fillna(0, inplace=True)

        # Get DIDO in minutes
        recanalization_procedure_tby_dido['DIDO'] = recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME_MIN']

        # Create temporary dataframe with calculated median value
        tmp = recanalization_procedure_tby_dido.groupby(['Protocol ID']).DIDO.agg(['median']).rename(columns={'median': 'Median TBY DIDO (minutes)'}).reset_index()
        self.statsDf = self.statsDf.merge(tmp, how='outer')
        self.statsDf.fillna(0, inplace=True)

        #######################
        # DYPSHAGIA SCREENING #
        #######################
        self.tmp = is_ich.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING']).size().to_frame('count').reset_index()

        # Get patients dysphagia screening - not known
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=6, new_column_name='# dysphagia screening - not known')

        # Get % patients dysphagia screening - not known
        self.statsDf['% dysphagia screening - not known'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - not known']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)

        # Get patients dysphagia screening - Guss test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=1, new_column_name='# dysphagia screening - Guss test')

        # Get % patients dysphagia screening - Guss test
        self.statsDf['% dysphagia screening - Guss test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Guss test']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Other test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=2, new_column_name='# dysphagia screening - Other test')

        # Get % patients dysphagia screening - Other test
        self.statsDf['% dysphagia screening - Other test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Other test']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Another centre
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=3, new_column_name='# dysphagia screening - Another centre')

        # Get % patients dysphagia screening - Another centre
        self.statsDf['% dysphagia screening - Another centre'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Another centre']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Not done
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=4, new_column_name='# dysphagia screening - Not done')

        # Get % patients dysphagia screening - Not done
        self.statsDf['% dysphagia screening - Not done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Not done']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Unable to test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=5, new_column_name='# dysphagia screening - Unable to test')

        # Get % patients dysphagia screening - Unable to test
        self.statsDf['% dysphagia screening - Unable to test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Unable to test']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        self.statsDf['# dysphagia screening done'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test'] + self.statsDf['# dysphagia screening - Another centre']
        self.statsDf['% dysphagia screening done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening done']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        ############################
        # DYPSHAGIA SCREENING TIME #
        ############################
        self.tmp = self.df.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING_TIME']).size().to_frame('count').reset_index()

        # Get patients dysphagia screening time - Within first 24 hours
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING_TIME", value=1, new_column_name='# dysphagia screening time - Within first 24 hours')

        # Get patients dysphagia screening time - After first 24 hours
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING_TIME", value=2, new_column_name='# dysphagia screening time - After first 24 hours')

        # Get % patients dysphagia screening time - Within first 24 hours
        self.statsDf['% dysphagia screening time - Within first 24 hours'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening time - Within first 24 hours']/(x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours'])) * 100), 2) if (x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours']) > 0 else 0, axis=1)

        # Get % patients dysphagia screening time - After first 24 hours
        self.statsDf['% dysphagia screening time - After first 24 hours'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening time - After first 24 hours']/(x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours'])) * 100), 2) if (x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours']) > 0 else 0, axis=1)

        ###################
        # HEMICRANIECTOMY #
        ###################
        self.tmp = isch.groupby(['Protocol ID', 'HEMICRANIECTOMY']).size().to_frame('count').reset_index()

        # Get patients hemicraniectomy - Yes
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=1, new_column_name='# hemicraniectomy - Yes')

        # Get % patients hemicraniectomy - Yes
        self.statsDf['% hemicraniectomy - Yes'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - Yes']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients hemicraniectomy - No
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=2, new_column_name='# hemicraniectomy - No')

        # Get % patients hemicraniectomy - No
        self.statsDf['% hemicraniectomy - No'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - No']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get patients hemicraniectomy - Referred to another centre
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=3, new_column_name='# hemicraniectomy - Referred to another centre')

        # Get % patients hemicraniectomy - Referred to another centre
        self.statsDf['% hemicraniectomy - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - Referred to another centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        ################
        # NEUROSURGERY #
        ################
        self.tmp = ich.groupby(['Protocol ID', 'NEUROSURGERY']).size().to_frame('count').reset_index()

        # Get patients neurosurgery - Not known
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=3, new_column_name='# neurosurgery - Not known')

        # Get % patients neurosurgery - Not known
        self.statsDf['% neurosurgery - Not known'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - Not known']/x['ich_patients']) * 100), 2) if x['ich_patients'] > 0 else 0, axis=1)

        # Get patients neurosurgery - Yes
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=1, new_column_name='# neurosurgery - Yes')

        # Get % patients neurosurgery - Yes
        self.statsDf['% neurosurgery - Yes'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - Yes']/(x['ich_patients'] - x['# neurosurgery - Not known'])) * 100), 2) if (x['ich_patients'] - x['# neurosurgery - Not known']) > 0 else 0, axis=1)

        # Get patients neurosurgery - No
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=2, new_column_name='# neurosurgery - No')

        # Get % patients neurosurgery - No
        self.statsDf['% neurosurgery - No'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - No']/(x['ich_patients'] - x['# neurosurgery - Not known'])) * 100), 2) if (x['ich_patients'] - x['# neurosurgery - Not known']) > 0 else 0, axis=1)

        #####################
        # NEUROSURGERY TYPE #
        #####################
        # Get temporary dataframe of patients who have undergone neurosurgery 
        neurosurgery = ich[ich['NEUROSURGERY'].isin([1])].copy()

        if neurosurgery.empty:
            # Get neurosurgery patients
            self.statsDf['neurosurgery_patients'] = 0

            # Get # patients neurosurgery type - intracranial hematoma evacuation
            self.statsDf['# neurosurgery type - intracranial hematoma evacuation'] = 0

            # Get % patients neurosurgery type - intracranial hematoma evacuation
            self.statsDf['% neurosurgery type - intracranial hematoma evacuation'] = 0

            # Get # patients neurosurgery type - external ventricular drainage
            self.statsDf['# neurosurgery type - external ventricular drainage'] = 0

            # Get % patients neurosurgery type - external ventricular drainage
            self.statsDf['% neurosurgery type - external ventricular drainage'] = 0

            # Get # patients neurosurgery type - decompressive craniectomy
            self.statsDf['# neurosurgery type - decompressive craniectomy'] = 0

            # Get % patients neurosurgery type - decompressive craniectomy
            self.statsDf['% neurosurgery type - decompressive craniectomy'] = 0

            # Get # patients neurosurgery type - Referred to another centre
            self.statsDf['# neurosurgery type - Referred to another centre'] = 0

            # Get % patients neurosurgery type - Referred to another centre
            self.statsDf['% neurosurgery type - Referred to another centre'] = 0
        else:
            self.tmp = neurosurgery.groupby(['Protocol ID', 'NEUROSURGERY_TYPE']).size().to_frame('count').reset_index()

            # Get neurosurgery patients
            self.statsDf['neurosurgery_patients'] = self._count_patients(dataframe=neurosurgery)

            # Get patients neurosurgery type - intracranial hematoma evacuation
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=1, new_column_name='# neurosurgery type - intracranial hematoma evacuation')

            # Get % patients neurosurgery type - intracranial hematoma evacuation
            self.statsDf['% neurosurgery type - intracranial hematoma evacuation'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - intracranial hematoma evacuation']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)

            # Get patients neurosurgery type - external ventricular drainage
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=2, new_column_name='# neurosurgery type - external ventricular drainage')

            # Get % patients neurosurgery type - external ventricular drainage
            self.statsDf['% neurosurgery type - external ventricular drainage'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - external ventricular drainage']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)

            # Get patients neurosurgery type - decompressive craniectomy
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=3, new_column_name='# neurosurgery type - decompressive craniectomy')

            # Get % patients neurosurgery type - decompressive craniectomy
            self.statsDf['% neurosurgery type - decompressive craniectomy'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - decompressive craniectomy']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)

            # Get patients neurosurgery type - Referred to another centre
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=4, new_column_name='# neurosurgery type - Referred to another centre')

            # Get % patients neurosurgery type - Referred to another centre
            self.statsDf['% neurosurgery type - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - Referred to another centre']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)

        ###################
        # BLEEDING REASON #
        ###################
        self.tmp = ich.groupby(['Protocol ID', 'BLEEDING_REASON']).size().to_frame('count').reset_index()
        self.tmp['BLEEDING_REASON'] = self.tmp['BLEEDING_REASON'].astype(str)

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="BLEEDING_REASON", value=-999, new_column_name='tmp')

        # Get patients bleeding reason - arterial hypertension
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value='1', new_column_name='# bleeding reason - arterial hypertension')

        # Get % patients bleeding reason - arterial hypertension
        self.statsDf['% bleeding reason - arterial hypertension'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - arterial hypertension']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding reason - aneurysm
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="2", new_column_name='# bleeding reason - aneurysm')

        # Get % patients bleeding reason - aneurysm
        self.statsDf['% bleeding reason - aneurysm'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - aneurysm']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding reason - arterio-venous malformation
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="3", new_column_name='# bleeding reason - arterio-venous malformation')

        # Get % patients bleeding reason - arterio-venous malformation
        self.statsDf['% bleeding reason - arterio-venous malformation'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - arterio-venous malformation']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding reason - anticoagulation therapy
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="4", new_column_name='# bleeding reason - anticoagulation therapy')

        # Get % patients bleeding reason - anticoagulation therapy
        self.statsDf['% bleeding reason - anticoagulation therapy'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - anticoagulation therapy']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding reason - amyloid angiopathy
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="5", new_column_name='# bleeding reason - amyloid angiopathy')

        # Get % patients bleeding reason - amyloid angiopathy
        self.statsDf['% bleeding reason - amyloid angiopathy'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - amyloid angiopathy']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding reason - Other
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="6", new_column_name='# bleeding reason - Other')

        # Get % patients bleeding reason - Other
        self.statsDf['% bleeding reason - Other'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - Other']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        ### DATA NORMLAIZATION
        norm_tmp = self.statsDf[['% bleeding reason - arterial hypertension', '% bleeding reason - aneurysm', '% bleeding reason - arterio-venous malformation', '% bleeding reason - anticoagulation therapy', '% bleeding reason - amyloid angiopathy', '% bleeding reason - Other']].copy()

        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)

        self.statsDf['bleeding_arterial_hypertension_perc_norm'] = ((norm_tmp['% bleeding reason - arterial hypertension']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['bleeding_aneurysm_perc_norm'] = ((norm_tmp['% bleeding reason - aneurysm']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['bleeding_arterio_venous_malformation_perc_norm'] = ((norm_tmp['% bleeding reason - arterio-venous malformation']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['bleeding_anticoagulation_therapy_perc_norm'] = ((norm_tmp['% bleeding reason - anticoagulation therapy']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['bleeding_amyloid_angiopathy_perc_norm'] = ((norm_tmp['% bleeding reason - amyloid angiopathy']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['bleeding_other_perc_norm'] = ((norm_tmp['% bleeding reason - Other']/norm_tmp['rowsums']) * 100).round(decimals=2)

        # MORE THAN ONE POSIBILITY
        # Get patients bleeding reason - more than one
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value=",", new_column_name='# bleeding reason - more than one')

        # Get % patients bleeding reason - more than one
        self.statsDf['% bleeding reason - more than one'] =  self.statsDf.apply(lambda x: round(((x['# bleeding reason - more than one']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # BLEEDING SOURCE #
        ###################
        self.tmp = sah.groupby(['Protocol ID', 'BLEEDING_SOURCE']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value=-999, new_column_name='tmp')

        # Get patients bleeding source - known
        self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value=1, new_column_name='# bleeding source - Known')

        # Get % patients bleeding source - known
        self.statsDf['% bleeding source - Known'] = self.statsDf.apply(lambda x: round(((x['# bleeding source - Known']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get patients bleeding source - Not known
        self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value=2, new_column_name='# bleeding source - Not known')

        # Get % patients bleeding source - Not known
        self.statsDf['% bleeding source - Not known'] = self.statsDf.apply(lambda x: round(((x['# bleeding source - Not known']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ################
        # INTERVENTION #
        ################
        self.tmp = sah.groupby(['Protocol ID', 'INTERVENTION']).size().to_frame('count').reset_index()
        self.tmp['INTERVENTION'] = self.tmp['INTERVENTION'].astype(str)

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="INTERVENTION", value=-999, new_column_name='tmp')

        # Get patients intervention - endovascular (coiling)
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="1", new_column_name='# intervention - endovascular (coiling)')

        # Get % patients intervention - endovascular (coiling)
        self.statsDf['% intervention - endovascular (coiling)'] = self.statsDf.apply(lambda x: round(((x['# intervention - endovascular (coiling)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        # Get patients intervention - neurosurgical (clipping)
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="2", new_column_name='# intervention - neurosurgical (clipping)')

        # Get % patients intervention - neurosurgical (clipping)
        self.statsDf['% intervention - neurosurgical (clipping)'] = self.statsDf.apply(lambda x: round(((x['# intervention - neurosurgical (clipping)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        # Get patients intervention - Other neurosurgical treatment (decompression, drainage)
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="3", new_column_name='# intervention - Other neurosurgical treatment (decompression, drainage)')

        # Get % patients intervention - Other neurosurgical treatment (decompression, drainage)
        self.statsDf['% intervention - Other neurosurgical treatment (decompression, drainage)'] = self.statsDf.apply(lambda x: round(((x['# intervention - Other neurosurgical treatment (decompression, drainage)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        # Get patients intervention - Referred to another hospital for intervention
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="4", new_column_name='# intervention - Referred to another hospital for intervention')

        # Get % patients intervention - Referred to another hospital for intervention
        self.statsDf['% intervention - Referred to another hospital for intervention'] = self.statsDf.apply(lambda x: round(((x['# intervention - Referred to another hospital for intervention']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        # Get patients intervention - None/no intervention
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="5|6", new_column_name='# intervention - None / no intervention')

        # Get % patients intervention - None/no intervention
        self.statsDf['% intervention - None / no intervention'] = self.statsDf.apply(lambda x: round(((x['# intervention - None / no intervention']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        ### DATA NORMLAIZATION
        norm_tmp = self.statsDf[['% intervention - endovascular (coiling)', '% intervention - neurosurgical (clipping)', '% intervention - Other neurosurgical treatment (decompression, drainage)', '% intervention - Referred to another hospital for intervention', '% intervention - None / no intervention']].copy()

        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)

        self.statsDf['intervention_endovascular_perc_norm'] = ((norm_tmp['% intervention - endovascular (coiling)']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['intervention_neurosurgical_perc_norm'] = ((norm_tmp['% intervention - neurosurgical (clipping)']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['intervention_other_perc_norm'] = ((norm_tmp['% intervention - Other neurosurgical treatment (decompression, drainage)']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['intervention_referred_perc_norm'] = ((norm_tmp['% intervention - Referred to another hospital for intervention']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['intervention_none_perc_norm'] = ((norm_tmp['% intervention - None / no intervention']/norm_tmp['rowsums']) * 100).round(decimals=2)

        # Get patients intervention - more than one
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value=",", new_column_name='# intervention - more than one')

        # Get % patients intervention - more than one
        self.statsDf['% intervention - more than one'] = self.statsDf.apply(lambda x: round(((x['# intervention - more than one']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ################
        # VT TREATMENT #
        ################
        if ('VT_TREATMENT' not in cvt.columns):
            cvt['VT_TREATMENT'] = np.nan
            
        self.tmp = cvt.groupby(['Protocol ID', 'VT_TREATMENT']).size().to_frame('count').reset_index()
        self.tmp[['VT_TREATMENT']] = self.tmp[['VT_TREATMENT']].astype(str)

        # Get patients VT treatment - anticoagulation
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="1", new_column_name='# VT treatment - anticoagulation')

        # Get % patients VT treatment - anticoagulation
        self.statsDf['% VT treatment - anticoagulation'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - anticoagulation']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        # Get patients VT treatment - thrombectomy
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="2", new_column_name='# VT treatment - thrombectomy')

        # Get % patients VT treatment - thrombectomy
        self.statsDf['% VT treatment - thrombectomy'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - thrombectomy']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        # Get patients VT treatment - local thrombolysis
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="3", new_column_name='# VT treatment - local thrombolysis')

        # Get % patients VT treatment - local thrombolysis
        self.statsDf['% VT treatment - local thrombolysis'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - local thrombolysis']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        # Get patients VT treatment - local neurological treatment
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="4", new_column_name='# VT treatment - local neurological treatment')

        # Get % patients VT treatment - local neurological treatment
        self.statsDf['% VT treatment - local neurological treatment'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - local neurological treatment']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        # Get patients VT treatment - more than one treatment
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value=",", new_column_name='# VT treatment - more than one treatment')

        # Get % patients VT treatment - more than one treatment
        self.statsDf['% VT treatment - more than one treatment'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - more than one treatment']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        ### DATA NORMLAIZATION
        norm_tmp = self.statsDf[['% VT treatment - anticoagulation', '% VT treatment - thrombectomy', '% VT treatment - local thrombolysis', '% VT treatment - local neurological treatment']].copy()

        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)

        self.statsDf['vt_treatment_anticoagulation_perc_norm'] = ((norm_tmp['% VT treatment - anticoagulation']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vt_treatment_thrombectomy_perc_norm'] = ((norm_tmp['% VT treatment - thrombectomy']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vt_treatment_local_thrombolysis_perc_norm'] = ((norm_tmp['% VT treatment - local thrombolysis']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf['vt_treatment_local_neurological_treatment_perc_norm'] = ((norm_tmp['% VT treatment - local neurological treatment']/norm_tmp['rowsums']) * 100).round(decimals=2)


        ########
        # AFIB #
        ########
        # patients not reffered 
        not_reffered = is_tia[~is_tia['RECANALIZATION_PROCEDURES'].isin([7])].copy()
        self.statsDf['not_reffered_patients'] = self._count_patients(dataframe=not_reffered)

        # patients referred to another hospital
        reffered = is_tia[is_tia['RECANALIZATION_PROCEDURES'].isin([7])].copy()
        self.statsDf['reffered_patients'] = self._count_patients(dataframe=reffered)

        # 
        self.tmp = not_reffered.groupby(['Protocol ID', 'AFIB_FLUTTER']).size().to_frame('count').reset_index()

        # Get patients afib/flutter - Known
        self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=1, new_column_name='# afib/flutter - Known')

        # Get % patients afib/flutter - Known
        self.statsDf['% afib/flutter - Known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 

        # Get patients afib/flutter - Newly-detected at admission
        self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=2, new_column_name='# afib/flutter - Newly-detected at admission')

        # Get % patients afib/flutter - Newly-detected at admission
        self.statsDf['% afib/flutter - Newly-detected at admission'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Newly-detected at admission']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 

        # Get patients afib/flutter - Detected during hospitalization
        self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=3, new_column_name='# afib/flutter - Detected during hospitalization')

        # Get % patients afib/flutter - Detected during hospitalization
        self.statsDf['% afib/flutter - Detected during hospitalization'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Detected during hospitalization']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
        # Get patients afib/flutter - Not detected
        self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=4, new_column_name='# afib/flutter - Not detected')

        # Get % patients afib/flutter - Not detected
        self.statsDf['% afib/flutter - Not detected'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not detected']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)

        # Get patients afib/flutter - Not known
        self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=5, new_column_name='# afib/flutter - Not known')

        # Get % patients afib/flutter - Not known
        self.statsDf['% afib/flutter - Not known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)

        #########################
        # AFIB DETECTION METHOD #
        #########################
        # patients referred to another hospital
        afib_detected_during_hospitalization = not_reffered[not_reffered['AFIB_FLUTTER'].isin([3])].copy()
        self.statsDf['afib_detected_during_hospitalization_patients'] = self._count_patients(dataframe=afib_detected_during_hospitalization)

        afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'] = afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'].astype(str)

        # 
        self.tmp = afib_detected_during_hospitalization.groupby(['Protocol ID', 'AFIB_DETECTION_METHOD']).size().to_frame('count').reset_index()

        # Get patients afib detection method - Telemetry with monitor allowing automatic detection of aFib
        self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="1", new_column_name='# afib detection method - Telemetry with monitor allowing automatic detection of aFib')

        # Get % patients afib detection method - Telemetry with monitor allowing automatic detection of aFib
        self.statsDf['% afib detection method - Telemetry with monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry with monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        # Get patients afib detection method - Telemetry without monitor allowing automatic detection of aFib
        self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="2", new_column_name='# afib detection method - Telemetry without monitor allowing automatic detection of aFib')

        # Get % patients afib detection method - Telemetry without monitor allowing automatic detection of aFib
        self.statsDf['% afib detection method - Telemetry without monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry without monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        # Get patients afib detection method - Holter-type monitoring
        self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="3", new_column_name='# afib detection method - Holter-type monitoring')

        # Get % patients afib detection method - Holter-type monitoring
        self.statsDf['% afib detection method - Holter-type monitoring'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Holter-type monitoring']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        # Get patients afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib
        self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="4", new_column_name='# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib')

        # Get % patients afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib
        self.statsDf['% afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        # Get patients afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib
        self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="5", new_column_name='# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib')
        
        # Get % patients afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib
        self.statsDf['% afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        ###############################
        # AFIB OTHER DETECTION METHOD #
        ###############################
        # patients referred to another hospital
        afib_not_detected_or_not_known = not_reffered[not_reffered['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_not_detected_or_not_known_patients'] = self._count_patients(dataframe=afib_not_detected_or_not_known)

        # 
        self.tmp = afib_not_detected_or_not_known.groupby(['Protocol ID', 'AFIB_OTHER_RECS']).size().to_frame('count').reset_index()

        # Get patients other afib detection method - Yes
        self.statsDf = self._get_values_for_factors(column_name="AFIB_OTHER_RECS", value=1, new_column_name='# other afib detection method - Yes')

        # Get % patients other afib detection method - Yes
        self.statsDf['% other afib detection method - Yes'] = self.statsDf.apply(lambda x: round(((x['# other afib detection method - Yes']/x['afib_not_detected_or_not_known_patients']) * 100), 2) if x['afib_not_detected_or_not_known_patients'] > 0 else 0, axis=1)

        # Get patients other afib detection method - Not detected or not known
        self.statsDf = self._get_values_for_factors(column_name="AFIB_OTHER_RECS", value=2, new_column_name='# other afib detection method - Not detected or not known')

        # Get % patients other afib detection method - Not detected or not known
        self.statsDf['% other afib detection method - Not detected or not known'] = self.statsDf.apply(lambda x: round(((x['# other afib detection method - Not detected or not known']/x['afib_not_detected_or_not_known_patients']) * 100), 2) if x['afib_not_detected_or_not_known_patients'] > 0 else 0, axis=1)

        ############################
        # CAROTID ARTERIES IMAGING #
        ############################
        # 
        self.tmp = is_tia.groupby(['Protocol ID', 'CAROTID_ARTERIES_IMAGING']).size().to_frame('count').reset_index()

        # Get patients carotid arteries imaging - Not known
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=3, new_column_name='# carotid arteries imaging - Not known')

        # Get % patients carotid arteries imaging - Not known
        self.statsDf['% carotid arteries imaging - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get patients carotid arteries imaging - Yes
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=1, new_column_name='# carotid arteries imaging - Yes')

        # Get % patients carotid arteries imaging - Yes
        self.statsDf['% carotid arteries imaging - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Yes']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)

        # Get patients carotid arteries imaging - No
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=2, new_column_name='# carotid arteries imaging - No')

        # Get % patients carotid arteries imaging - No
        self.statsDf['% carotid arteries imaging - No'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - No']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)

        ############################
        # ANTITHROMBOTICS WITH CVT #
        ############################
        # patients not reffered 
        antithrombotics_with_cvt = is_tia_cvt[~is_tia_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['antithrombotics_patients_with_cvt'] = self._count_patients(dataframe=antithrombotics_with_cvt)

        ischemic_transient_cerebral_dead = is_tia_cvt[is_tia_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['ischemic_transient_cerebral_dead_patients'] = self._count_patients(dataframe=ischemic_transient_cerebral_dead)

        self.tmp = antithrombotics_with_cvt.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()

        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients receiving antiplatelets with CVT')

        # Get % patients receiving antiplatelets
        self.statsDf['% patients receiving antiplatelets with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving antiplatelets with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving Vit. K antagonist
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist with CVT')

        # Get % patients receiving Vit. K antagonist
        self.statsDf['% patients receiving Vit. K antagonist with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving dabigatran
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran with CVT')

        # Get % patients receiving dabigatran
        self.statsDf['% patients receiving dabigatran with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving rivaroxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban with CVT')

        # Get % patients receiving rivaroxaban
        self.statsDf['% patients receiving rivaroxaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving apixaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban with CVT')

        # Get % patients receiving apixaban
        self.statsDf['% patients receiving apixaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving edoxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban with CVT')

        # Get % patients receiving edoxaban
        self.statsDf['% patients receiving edoxaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving LMWH or heparin in prophylactic dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose with CVT')

        # Get % patients receiving LMWH or heparin in prophylactic dose
        self.statsDf['% patients receiving LMWH or heparin in prophylactic dose with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose with CVT')

        # Get % patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients not prescribed antithrombotics, but recommended
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=9, new_column_name='# patients not prescribed antithrombotics, but recommended with CVT')

        # Get % patients not prescribed antithrombotics, but recommended
        self.statsDf['% patients not prescribed antithrombotics, but recommended with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients not prescribed antithrombotics, but recommended with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        # Get patients neither receiving antithrombotics nor recommended
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=10, new_column_name='# patients neither receiving antithrombotics nor recommended with CVT')

        # Get % patients neither receiving antithrombotics nor recommended
        self.statsDf['% patients neither receiving antithrombotics nor recommended with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients neither receiving antithrombotics nor recommended with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        ## ANTITHROMBOTICS - PATIENTS PRESCRIBED + RECOMMENDED
        # patients prescribed antithrombotics
        self.statsDf.loc[:, '# patients prescribed antithrombotics with CVT'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets with CVT'] + x['# patients receiving Vit. K antagonist with CVT'] + x['# patients receiving dabigatran with CVT'] + x['# patients receiving rivaroxaban with CVT'] + x['# patients receiving apixaban with CVT'] + x['# patients receiving edoxaban with CVT'] + x['# patients receiving LMWH or heparin in prophylactic dose with CVT'] + x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT'], axis=1)

        # Get % patients prescribed antithrombotics
        #self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0 else 0, axis=1)
        self.statsDf['% patients prescribed antithrombotics with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        #  patients prescribed or recommended antithrombotics
        self.statsDf.loc[:, '# patients prescribed or recommended antithrombotics with CVT'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets with CVT'] + x['# patients receiving Vit. K antagonist with CVT'] + x['# patients receiving dabigatran with CVT'] + x['# patients receiving rivaroxaban with CVT'] + x['# patients receiving apixaban with CVT'] + x['# patients receiving edoxaban with CVT'] + x['# patients receiving LMWH or heparin in prophylactic dose with CVT'] + x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT'] + x['# patients not prescribed antithrombotics, but recommended with CVT'], axis=1)

        # Get % patients prescribed or recommended antithrombotics
        self.statsDf['% patients prescribed or recommended antithrombotics with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics with CVT'] - x['ischemic_transient_cerebral_dead_patients'])/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended with CVT'])) * 100, 2) if ((x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended with CVT']) > 0) else 0, axis=1)

        #.round(decimals=2)) 

        self.statsDf.fillna(0, inplace=True)

        ###########################################
        # ANTIPLATELETS - PRESCRIBED WITHOUT AFIB #
        ###########################################
        # patients not reffered 
        afib_flutter_not_detected_or_not_known_with_cvt = is_tia_cvt[is_tia_cvt['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_with_cvt)

        afib_flutter_not_detected_or_not_known_with_cvt_dead = afib_flutter_not_detected_or_not_known_with_cvt[afib_flutter_not_detected_or_not_known_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_with_cvt_dead)

        prescribed_antiplatelets_no_afib_with_cvt = afib_flutter_not_detected_or_not_known_with_cvt[afib_flutter_not_detected_or_not_known_with_cvt['ANTITHROMBOTICS'].isin([1])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_patients_with_cvt'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_with_cvt)

        prescribed_antiplatelets_no_afib_dead_with_cvt = prescribed_antiplatelets_no_afib_with_cvt[prescribed_antiplatelets_no_afib_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_dead_patients_with_cvt'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_dead_with_cvt)

        self.tmp = afib_flutter_not_detected_or_not_known_with_cvt.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed antiplatelets without aFib with CVT')

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antiplatelets without aFib with CVT'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed antiplatelets without aFib with CVT'] - x['prescribed_antiplatelets_no_afib_dead_patients_with_cvt'])/(x['afib_flutter_not_detected_or_not_known_patients_with_cvt'] - x['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt'])) * 100, 2) if ((x['afib_flutter_not_detected_or_not_known_patients_with_cvt'] - x['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt']) > 0) else 0, axis=1)

        #########################################
        # ANTICOAGULANTS - PRESCRIBED WITH AFIB #
        #########################################
        # patients not reffered 
        afib_flutter_detected_with_cvt = is_tia_cvt[is_tia_cvt['AFIB_FLUTTER'].isin([1, 2, 3])].copy()
        self.statsDf['afib_flutter_detected_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_detected_with_cvt)

        anticoagulants_prescribed_with_cvt = afib_flutter_detected_with_cvt[~afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([1, 10, 9]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed anticoagulants with aFib with CVT'] = self._count_patients(dataframe=anticoagulants_prescribed_with_cvt)
        
        anticoagulants_recommended_with_cvt = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9])].copy()
        self.statsDf['anticoagulants_recommended_patients_with_cvt'] = self._count_patients(dataframe=anticoagulants_recommended_with_cvt)

        afib_flutter_detected_dead_with = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_dead_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_detected_dead_with)

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed anticoagulants with aFib with CVT'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed anticoagulants with aFib with CVT']/(x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'])) * 100), 2) if (x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt']) > 0 else 0, axis=1)

        ##########################################
        # ANTITHROMBOTICS - PRESCRIBED WITH AFIB #
        ##########################################
        # patients not reffered 
        antithrombotics_prescribed_with_cvt = afib_flutter_detected_with_cvt[~afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9, 10]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed antithrombotics with aFib with CVT'] = self._count_patients(dataframe=antithrombotics_prescribed_with_cvt)

        recommended_antithrombotics_with_afib_alive_with_cvt = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['recommended_antithrombotics_with_afib_alive_patients_with_cvt'] = self._count_patients(dataframe=recommended_antithrombotics_with_afib_alive_with_cvt)

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antithrombotics with aFib with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with aFib with CVT']/(x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'] - x['recommended_antithrombotics_with_afib_alive_patients_with_cvt'])) * 100), 2) if (x['afib_flutter_detected_dead_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'] - x['recommended_antithrombotics_with_afib_alive_patients_with_cvt']) > 0 else 0, axis=1)
        
        
        
        ###############################
        # ANTITHROMBOTICS WITHOUT CVT #
        ###############################
        # patients not reffered 
        antithrombotics = is_tia[~is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['antithrombotics_patients'] = self._count_patients(dataframe=antithrombotics)

        ischemic_transient_dead = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['ischemic_transient_dead_patients'] = self._count_patients(dataframe=ischemic_transient_dead)
        
        self.tmp = antithrombotics.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()

        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients receiving antiplatelets')

        
        # Get % patients receiving antiplatelets
        self.statsDf['% patients receiving antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# patients receiving antiplatelets']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        
        # Get patients receiving Vit. K antagonist
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist')

        # Get % patients receiving Vit. K antagonist
        #self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving dabigatran
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran')

        # Get % patients receiving dabigatran
        #self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving rivaroxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban')

        # Get % patients receiving rivaroxaban
        #self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving apixaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban')

        # Get % patients receiving apixaban
        #self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving edoxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban')

        # Get % patients receiving edoxaban
        #self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving LMWH or heparin in prophylactic dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')

        # Get % patients receiving LMWH or heparin in prophylactic dose
        #self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')

        # Get % patients receiving LMWH or heparin in full anticoagulant dose
        #self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)
        
        # Get patients not prescribed antithrombotics, but recommended
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=9, new_column_name='# patients not prescribed antithrombotics, but recommended')

        # Get % patients not prescribed antithrombotics, but recommended
        self.statsDf['% patients not prescribed antithrombotics, but recommended'] = self.statsDf.apply(lambda x: round(((x['# patients not prescribed antithrombotics, but recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients neither receiving antithrombotics nor recommended
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=10, new_column_name='# patients neither receiving antithrombotics nor recommended')

        # Get % patients neither receiving antithrombotics nor recommended
        self.statsDf['% patients neither receiving antithrombotics nor recommended'] = self.statsDf.apply(lambda x: round(((x['# patients neither receiving antithrombotics nor recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        ## ANTITHROMBOTICS - PATIENTS PRESCRIBED + RECOMMENDED
        # patients prescribed antithrombotics
        self.statsDf.loc[:, '# patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets'] + x['# patients receiving Vit. K antagonist'] + x['# patients receiving dabigatran'] + x['# patients receiving rivaroxaban'] + x['# patients receiving apixaban'] + x['# patients receiving edoxaban'] + x['# patients receiving LMWH or heparin in prophylactic dose'] + x['# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1)

        # Get % patients prescribed antithrombotics
        #self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0 else 0, axis=1)
        self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        
        #  patients prescribed or recommended antithrombotics
        self.statsDf.loc[:, '# patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets'] + x['# patients receiving Vit. K antagonist'] + x['# patients receiving dabigatran'] + x['# patients receiving rivaroxaban'] + x['# patients receiving apixaban'] + x['# patients receiving edoxaban'] + x['# patients receiving LMWH or heparin in prophylactic dose'] + x['# patients receiving LMWH or heparin in full anticoagulant dose'] + x['# patients not prescribed antithrombotics, but recommended'], axis=1)

        # Get % patients prescribed or recommended antithrombotics
        self.statsDf['% patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics'] - x['ischemic_transient_dead_patients'])/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100, 2) if ((x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0) else 0, axis=1)
        
        #.round(decimals=2)) 
        self.statsDf.drop(['# patients receiving Vit. K antagonist', '# patients receiving dabigatran', '# patients receiving rivaroxaban', '# patients receiving apixaban', '# patients receiving edoxaban', '# patients receiving LMWH or heparin in prophylactic dose','# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1, inplace=True)

        self.statsDf.fillna(0, inplace=True)

        ###########################################
        # ANTIPLATELETS - PRESCRIBED WITHOUT AFIB #
        ###########################################
        # patients not reffered 
        afib_flutter_not_detected_or_not_known = is_tia[is_tia['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known)

        afib_flutter_not_detected_or_not_known_dead = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_dead_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_dead)

        prescribed_antiplatelets_no_afib = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['ANTITHROMBOTICS'].isin([1])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib)

        prescribed_antiplatelets_no_afib_dead = prescribed_antiplatelets_no_afib[prescribed_antiplatelets_no_afib['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_dead_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_dead)

        self.tmp = afib_flutter_not_detected_or_not_known.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed antiplatelets without aFib')

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antiplatelets without aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed antiplatelets without aFib'] - x['prescribed_antiplatelets_no_afib_dead_patients'])/(x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients'])) * 100, 2) if ((x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients']) > 0) else 0, axis=1)

        #########################################
        # ANTICOAGULANTS - PRESCRIBED WITH AFIB #
        #########################################
        # patients not reffered 
        afib_flutter_detected = is_tia[is_tia['AFIB_FLUTTER'].isin([1, 2, 3])].copy()
        self.statsDf['afib_flutter_detected_patients'] = self._count_patients(dataframe=afib_flutter_detected)

        afib_flutter_detected_not_dead = afib_flutter_detected[~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_patients_not_dead'] = self._count_patients(dataframe=afib_flutter_detected_not_dead)

        anticoagulants_prescribed = afib_flutter_detected[~afib_flutter_detected['ANTITHROMBOTICS'].isin([1, 10, 9]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed anticoagulants with aFib'] = self._count_patients(dataframe=anticoagulants_prescribed)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        # Additional calculation 
        # Get patients receiving Vit. K antagonist
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist')

        # Get % patients receiving Vit. K antagonist
        #self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['# patients prescribed anticoagulants with aFib']) * 100), 2) if x['# patients prescribed anticoagulants with aFib'] > 0 else 0, axis=1)
        self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)


        # Get patients receiving dabigatran
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran')

        # Get % patients receiving dabigatran
        self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        # Get patients receiving rivaroxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban')

        # Get % patients receiving rivaroxaban
        self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        # Get patients receiving apixaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban')

        # Get % patients receiving apixaban
        self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        # Get patients receiving edoxaban
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban')

        # Get % patients receiving edoxaban
        self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)
        # Get patients receiving LMWH or heparin in prophylactic dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')

        # Get % patients receiving LMWH or heparin in prophylactic dose
        self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        # Get patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')

        # Get % patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        
        anticoagulants_recommended = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9])].copy()
        self.statsDf['anticoagulants_recommended_patients'] = self._count_patients(dataframe=anticoagulants_recommended)

        afib_flutter_detected_dead = afib_flutter_detected[afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_dead_patients'] = self._count_patients(dataframe=afib_flutter_detected_dead)

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed anticoagulants with aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed anticoagulants with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)

        ##########################################
        # ANTITHROMBOTICS - PRESCRIBED WITH AFIB #
        ##########################################
        # patients not reffered 
        antithrombotics_prescribed = afib_flutter_detected[~afib_flutter_detected['ANTITHROMBOTICS'].isin([9, 10]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed antithrombotics with aFib'] = self._count_patients(dataframe=antithrombotics_prescribed)

        recommended_antithrombotics_with_afib_alive = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['recommended_antithrombotics_with_afib_alive_patients'] = self._count_patients(dataframe=recommended_antithrombotics_with_afib_alive)

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antithrombotics with aFib'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients']) > 0 else 0, axis=1)
        
        ###########
        # STATINS #
        ###########
        is_tia_discharged_home = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([1])].copy()
        self.statsDf['is_tia_discharged_home_patients'] = self._count_patients(dataframe=is_tia_discharged_home)
        
        self.tmp = is_tia_discharged_home.groupby(['Protocol ID', 'STATIN']).size().to_frame('count').reset_index()

        # Get patients prescribed statins - Yes
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=1, new_column_name='# patients prescribed statins - Yes')

        # Get % patients prescribed statins - Yes
        self.statsDf['% patients prescribed statins - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Yes']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)

        # Get patients prescribed statins - No
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=2, new_column_name='# patients prescribed statins - No')

        # Get % patients prescribed statins - No
        self.statsDf['% patients prescribed statins - No'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - No']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)

        # Get patients prescribed statins - Not known
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=3, new_column_name='# patients prescribed statins - Not known')

        # Get % patients prescribed statins - Not known
        self.statsDf['% patients prescribed statins - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Not known']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)

        ####################
        # CAROTID STENOSIS #
        ####################
        self.tmp = is_tia.groupby(['Protocol ID', 'CAROTID_STENOSIS']).size().to_frame('count').reset_index()

        # Get carotid stenosis - 50%-70%
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=1, new_column_name='# carotid stenosis - 50%-70%')

        # Get % carotid stenosis - 50%-70%
        self.statsDf['% carotid stenosis - 50%-70%'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - 50%-70%']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis - >70%
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=2, new_column_name='# carotid stenosis - >70%')

        # Get % carotid stenosis - >70%
        self.statsDf['% carotid stenosis - >70%'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - >70%']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis - No
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=3, new_column_name='# carotid stenosis - No')

        # Get % carotid stenosis - No
        self.statsDf['% carotid stenosis - No'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis - Not known
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=4, new_column_name='# carotid stenosis - Not known')

        # Get % carotid stenosis - Not known
        self.statsDf['% carotid stenosis - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        ##############################
        # CAROTID STENOSIS FOLLOW-UP #
        ##############################
        #carotid_stenosis = is_tia[is_tia['CAROTID_STENOSIS'].isin([1, 2])]
        # Change to >70% patients
        carotid_stenosis = is_tia[is_tia['CAROTID_STENOSIS'].isin([1, 2])] 

        self.tmp = carotid_stenosis.groupby(['Protocol ID', 'CAROTID_STENOSIS_FOLLOWUP']).size().to_frame('count').reset_index()

        # Get carotid stenosis followup - Yes
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=1, new_column_name='# carotid stenosis followup - Yes')

        # Get % carotid stenosis followup - Yes
        self.statsDf['% carotid stenosis followup - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Yes']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis followup - No
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=2, new_column_name='# carotid stenosis followup - No')

        # Get % carotid stenosis followup - No
        self.statsDf['% carotid stenosis followup - No'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis followup - No, but planned later
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=3, new_column_name='# carotid stenosis followup - No, but planned later')

        # Get % carotid stenosis followup - No, but planned later
        self.statsDf['% carotid stenosis followup - No, but planned later'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - No, but planned later']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis followup - Yes, but planned
        carotid_stenosis_followup = carotid_stenosis[carotid_stenosis['CAROTID_STENOSIS_FOLLOWUP'].isin([1, 3])].copy()
        self.statsDf['# carotid stenosis followup - Yes, but planned'] = self._count_patients(dataframe=carotid_stenosis_followup)

        # Get % carotid stenosis followup - Yes, but planned
        self.statsDf['% carotid stenosis followup - Yes, but planned'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Yes, but planned']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get carotid stenosis followup - Referred to another centre
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=4, new_column_name='# carotid stenosis followup - Referred to another centre')

        # Get % carotid stenosis followup - Referred to another centre
        self.statsDf['% carotid stenosis followup - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Referred to another centre']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        #####################
        # ANTIHYPERTENSIVES #
        #####################
        self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'ANTIHYPERTENSIVE']).size().to_frame('count').reset_index()

        # Get prescribed antihypertensives - Not known
        self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=3, new_column_name='# prescribed antihypertensives - Not known')

        # Get % prescribed antihypertensives - Not known
        self.statsDf['% prescribed antihypertensives - Not known'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Not known']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

        # Get prescribed antihypertensives - Yes
        self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=1, new_column_name='# prescribed antihypertensives - Yes')

        # Get % prescribed antihypertensives - Yes
        self.statsDf['% prescribed antihypertensives - Yes'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Yes']/(x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)
        
        # Get prescribed antihypertensives - No
        self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=2, new_column_name='# prescribed antihypertensives - No')

        # Get % prescribed antihypertensives - No
        self.statsDf['% prescribed antihypertensives - No'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - No']/(x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)

        #####################
        # SMOKING CESSATION #
        #####################
        self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'SMOKING_CESSATION']).size().to_frame('count').reset_index()

        # Get recommended to a smoking cessation program - not a smoker
        self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=3, new_column_name='# recommended to a smoking cessation program - not a smoker')

        # Get % recommended to a smoking cessation program - not a smoker
        self.statsDf['% recommended to a smoking cessation program - not a smoker'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - not a smoker']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

        # Get recommended to a smoking cessation program - Yes
        self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=1, new_column_name='# recommended to a smoking cessation program - Yes')

        # Get % recommended to a smoking cessation program - Yes
        self.statsDf['% recommended to a smoking cessation program - Yes'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - Yes']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

        # Get recommended to a smoking cessation program - No
        self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=2, new_column_name='# recommended to a smoking cessation program - No')

        # Get % recommended to a smoking cessation program - No
        self.statsDf['% recommended to a smoking cessation program - No'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - No']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

        ##########################
        # CEREBROVASCULAR EXPERT #
        ##########################
        self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'CEREBROVASCULAR_EXPERT']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=-999, new_column_name='tmp')

        # Get recommended to a cerebrovascular expert - Recommended, and appointment was made
        self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=1, new_column_name='# recommended to a cerebrovascular expert - Recommended, and appointment was made')

        # Get % recommended to a cerebrovascular expert - Recommended, and appointment was made
        self.statsDf['% recommended to a cerebrovascular expert - Recommended, and appointment was made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, and appointment was made']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get recommended to a cerebrovascular expert - Recommended, but appointment was not made
        self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=2, new_column_name='# recommended to a cerebrovascular expert - Recommended, but appointment was not made')

        # Get % recommended to a cerebrovascular expert - Recommended, but appointment was not made
        self.statsDf['% recommended to a cerebrovascular expert - Recommended, but appointment was not made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get recommended to a cerebrovascular expert - Recommended
        self.statsDf.loc[:, '# recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: x['# recommended to a cerebrovascular expert - Recommended, and appointment was made'] + x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made'], axis=1)

        # Get % recommended to a cerebrovascular expert - Recommended
        self.statsDf['% recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get recommended to a cerebrovascular expert - Not recommended
        self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=3, new_column_name='# recommended to a cerebrovascular expert - Not recommended')

        # Get % recommended to a cerebrovascular expert - Not recommended
        self.statsDf['% recommended to a cerebrovascular expert - Not recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Not recommended']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        #########################
        # DISCHARGE DESTINATION #
        #########################
        self.tmp = discharge_subset.groupby(['Protocol ID', 'DISCHARGE_DESTINATION']).size().to_frame('count').reset_index()

        # Get discharge destination - Home
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=1, new_column_name='# discharge destination - Home')

        # Get % discharge destination - Home
        self.statsDf['% discharge destination - Home'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Home']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        # Get discharge destination - Transferred within the same centre
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=2, new_column_name='# discharge destination - Transferred within the same centre')

        # Get % discharge destination - Transferred within the same centre
        self.statsDf['% discharge destination - Transferred within the same centre'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Transferred within the same centre']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        # Get discharge destination - Transferred to another centre
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=3, new_column_name='# discharge destination - Transferred to another centre')

        # Get % discharge destination - Transferred to another centre
        self.statsDf['% discharge destination - Transferred to another centre'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Transferred to another centre']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        # Get discharge destination - Social care facility
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=4, new_column_name='# discharge destination - Social care facility')

        # Get % discharge destination - Social care facility
        self.statsDf['% discharge destination - Social care facility'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Social care facility']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        # Get discharge destination - Dead
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=5, new_column_name='# discharge destination - Dead')

        # Get % discharge destination - Dead
        self.statsDf['% discharge destination - Dead'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Dead']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        #######################################
        # DISCHARGE DESTINATION - SAME CENTRE #
        #######################################
        discharge_subset_same_centre = discharge_subset[discharge_subset['DISCHARGE_DESTINATION'].isin([2])].copy()
        self.statsDf['discharge_subset_same_centre_patients'] = self._count_patients(dataframe=discharge_subset_same_centre)

        self.tmp = discharge_subset_same_centre.groupby(['Protocol ID', 'DISCHARGE_SAME_FACILITY']).size().to_frame('count').reset_index()

        # Get transferred within the same centre - Acute rehabilitation
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=1, new_column_name='# transferred within the same centre - Acute rehabilitation')

        # Get % transferred within the same centre - Acute rehabilitation
        self.statsDf['% transferred within the same centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Acute rehabilitation']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        # Get transferred within the same centre - Post-care bed
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=2, new_column_name='# transferred within the same centre - Post-care bed')

        # Get % transferred within the same centre - Post-care bed
        self.statsDf['% transferred within the same centre - Post-care bed'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Post-care bed']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        # Get transferred within the same centre - Another department
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=3, new_column_name='# transferred within the same centre - Another department')

        # Get % transferred within the same centre - Another department
        self.statsDf['% transferred within the same centre - Another department'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Another department']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        # Check if sum of % is > 100 (100.01)
        #self.statsDf['sum'] = self.statsDf['% transferred within the same centre - Acute rehabilitation'] + self.statsDf['% transferred within the same centre - Post-care bed'] + self.statsDf['% transferred within the same centre - Another department']
        #self.statsDf['% transferred within the same centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: x['% transferred within the same centre - Acute rehabilitation']-0.01 if x['sum'] > 100.00 else x['% transferred within the same centre - Acute rehabilitation'], axis=1)

        # Drop tmp column 
        #self.statsDf.drop(['sum'], inplace=True, axis=1)

        ############################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY #
        ############################################
        discharge_subset_another_centre = discharge_subset[discharge_subset['DISCHARGE_DESTINATION'].isin([3])].copy()
        self.statsDf['discharge_subset_another_centre_patients'] = self._count_patients(dataframe=discharge_subset_another_centre)

        self.tmp = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=-999, new_column_name='tmp')

        # Get transferred to another centre - Stroke centre
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=1, new_column_name='# transferred to another centre - Stroke centre')

        # Get % transferred to another centre - Stroke centre
        self.statsDf['% transferred to another centre - Stroke centre'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Stroke centre']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get transferred to another centre - Comprehensive stroke centre
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=2, new_column_name='# transferred to another centre - Comprehensive stroke centre')

        # Get % transferred to another centre - Comprehensive stroke centre
        self.statsDf['% transferred to another centre - Comprehensive stroke centre'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Comprehensive stroke centre']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get transferred to another centre - Another hospital
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=3, new_column_name='# transferred to another centre - Another hospital')

        # Get % transferred to another centre - Another hospital
        self.statsDf['% transferred to another centre - Another hospital'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Another hospital']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        #########################################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY - DEPARTMENT #
        #########################################################
        self.tmp = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O1']).size().to_frame('count').reset_index()
        tmp_o2 = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O2']).size().to_frame('count').reset_index()
        tmp_o3 = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O3']).size().to_frame('count').reset_index()

        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY_O1", value=-999, new_column_name='tmp')

        # Get department transferred to within another centre - Acute rehabilitation
        self.statsDf['# department transferred to within another centre - Acute rehabilitation'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=1, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=1, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=1, dataframe=tmp_o3)

        # Get % department transferred to within another centre - Acute rehabilitation
        self.statsDf['% department transferred to within another centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Acute rehabilitation']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get department transferred to within another centre - Post-care bed
        self.statsDf['# department transferred to within another centre - Post-care bed'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=2, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=2, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=2, dataframe=tmp_o3)

        # Get % department transferred to within another centre - Post-care bed
        self.statsDf['% department transferred to within another centre - Post-care bed'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Post-care bed']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get department transferred to within another centre - Neurology
        self.statsDf['# department transferred to within another centre - Neurology'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=3, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=3, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=3, dataframe=tmp_o3)

        # Get % department transferred to within another centre - Neurology
        self.statsDf['% department transferred to within another centre - Neurology'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Neurology']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Get department transferred to within another centre - Another department
        self.statsDf['# department transferred to within another centre - Another department'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=4, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=4, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=4, dataframe=tmp_o3)

        # Get % department transferred to within another centre - Another department
        self.statsDf['% department transferred to within another centre - Another department'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Another department']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        # Drop tmp column 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        # Check if sum of % is > 100 (100.01)
        #self.statsDf['sum'] = self.statsDf['% department transferred to within another centre - Acute rehabilitation'] + self.statsDf['% department transferred to within another centre - Post-care bed'] + self.statsDf['% department transferred to within another centre - Neurology'] + self.statsDf['% department transferred to within another centre - Another department']
        #self.statsDf['% department transferred to within another centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: x['% department transferred to within another centre - Acute rehabilitation']-0.01 if x['sum'] > 100.00 else x['% department transferred to within another centre - Acute rehabilitation'], axis=1)

        # Drop tmp column 
        #self.statsDf.drop(['sum'], inplace=True, axis=1)

        ############################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY #
        ############################################
        discharge_subset.fillna(0, inplace=True)
        discharge_subset_mrs = discharge_subset[~discharge_subset['DISCHARGE_MRS'].isin([0])].copy()
        #discharge_subset_mrs['DISCHARGE_MRS'] = discharge_subset_mrs['DISCHARGE_MRS'].astype(float)

        def function(x):
            x = float(x)
            if (x == 1):
                x = x - 1
            else: 
                x = x - 2 

                return x

        if discharge_subset_mrs.empty:
            self.statsDf['Median discharge mRS'] = 0
            self.statsDf.fillna(0, inplace=True)
        else: 
            discharge_subset_mrs['DISCHARGE_MRS_ADJUSTED'] = discharge_subset_mrs.apply(lambda row: function(row['DISCHARGE_MRS']), axis=1)
            discharge_subset_mrs['DISCHARGE_MRS_ADDED'] = discharge_subset_mrs['DISCHARGE_MRS_ADJUSTED'] + discharge_subset_mrs['D_MRS_SCORE']
            discharge_subset_mrs.fillna(0, inplace=True)

            self.statsDf = self.statsDf.merge(discharge_subset_mrs.groupby(['Protocol ID']).DISCHARGE_MRS_ADDED.agg(['median']).rename(columns={'median': 'Median discharge mRS'})['Median discharge mRS'].reset_index(), how='outer')
            self.statsDf.fillna(0, inplace=True)

        ########################
        # MEDIAN HOSPITAL STAY #
        ########################
        positive_hospital_days = self.df[self.df['HOSPITAL_DAYS'] > 0]
        self.statsDf = self.statsDf.merge(positive_hospital_days.groupby(['Protocol ID']).HOSPITAL_DAYS.agg(['median']).rename(columns={'median': 'Median hospital stay (days)'})['Median hospital stay (days)'].reset_index(), how='outer')
        self.statsDf.fillna(0, inplace=True)

        ################
        # ANGEL AWARDS #
        ################
        #### TOTAL PATIENTS #####
        self.statsDf['# total patients >= 30'] = self.statsDf['Total Patients'] >= 30

        #### DOOR TO THROMBOLYSIS THERAPY - MINUTES ####
        self.statsDf.loc[:, 'patients_eligible_recanalization'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Not done'] + x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)

        # patients treated with door to recanalization therapy < 60 minutes
        # for tby, we are only looking at patients that have had ONLY tby, not tpa + tby, as we awould be counting those patients twice (penalizing twice)
        recanalization_procedure_tby_only_dtg =  recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['RECANALIZATION_PROCEDURES'].isin([4])]

        recanalization_procedure_iv_tpa_under_60 = recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] <= 60]

        recanalization_procedure_tby_only_dtg_under_60 = recanalization_procedure_tby_only_dtg[recanalization_procedure_tby_only_dtg['TBY'] <= 60]
        
        # patients treated with door to recanalization therapy < 60 minutes
        self.statsDf['# patients treated with door to recanalization therapy < 60 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_60) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_60)

        # % patients treated with door to recanalization therapy < 60 minutes
        self.statsDf['% patients treated with door to recanalization therapy < 60 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 60 minutes']/x['# patients recanalized']) * 100), 2) if x['# patients recanalized'] > 0 else 0, axis=1)

        recanalization_procedure_iv_tpa_under_45 = recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] <= 45]

        recanalization_procedure_tby_only_dtg_under_45 = recanalization_procedure_tby_only_dtg[recanalization_procedure_tby_only_dtg['TBY'] <= 45]

        # patients treated with door to recanalization therapy < 45 minutes
        self.statsDf['# patients treated with door to recanalization therapy < 45 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_45) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_45)

        # % patients treated with door to recanalization therapy < 45 minutes
        self.statsDf['% patients treated with door to recanalization therapy < 45 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 45 minutes']/x['# patients recanalized']) * 100), 2) if x['# patients recanalized'] > 0 else 0, axis=1)

        #### RECANALIZATION RATE ####
        # recanalization rate out of total ischemic incidence
        self.statsDf['# recanalization rate out of total ischemic incidence'] = self.statsDf['# patients recanalized']

        # % recanalization rate out of total ischemic incidence
        self.statsDf['% recanalization rate out of total ischemic incidence'] = self.statsDf['% patients recanalized']

        #### CT/MRI ####
        # suspected stroke patients undergoing CT/MRI
        self.statsDf['# suspected stroke patients undergoing CT/MRI'] = self.statsDf['# CT/MRI - performed']

        # % suspected stroke patients undergoing CT/MRI
        self.statsDf['% suspected stroke patients undergoing CT/MRI'] = self.statsDf['% CT/MRI - performed']

        #### DYSPHAGIA SCREENING ####
        # all stroke patients undergoing dysphagia screening
        self.statsDf['# all stroke patients undergoing dysphagia screening'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test']

        # % all stroke patients undergoing dysphagia screening
        self.statsDf['% all stroke patients undergoing dysphagia screening'] = self.statsDf.apply(lambda x: round(((x['# all stroke patients undergoing dysphagia screening']/(x['# all stroke patients undergoing dysphagia screening'] + x['# dysphagia screening - Not done'])) * 100), 2) if (x['# all stroke patients undergoing dysphagia screening'] + x['# dysphagia screening - Not done']) > 0 else 0, axis=1)

        #### ISCHEMIC STROKE + NO AFIB + ANTIPLATELETS ####
        # Get temporary dataframe with patients who have prescribed antithrombotics and ischemic stroke
        antiplatelets = antithrombotics[antithrombotics['STROKE_TYPE'].isin([1])]
        # Filter temporary dataframe and get only patients who have not been detected or not known for aFib flutter. 
        antiplatelets = antiplatelets[antiplatelets['AFIB_FLUTTER'].isin([4, 5])]
        # Get patients who have prescribed antithrombotics 
        except_recommended = antiplatelets[~antiplatelets['ANTITHROMBOTICS'].isin([9])]
        # Get number of patients who have prescribed antithrombotics and ischemic stroke, have not been detected or not known for aFib flutter.
        self.statsDf['except_recommended_patients'] = self._count_patients(dataframe=except_recommended)
        # Get temporary dataframe groupby protocol ID and antithrombotics column
        self.tmp = antiplatelets.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        # ischemic stroke patients discharged with antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# ischemic stroke patients discharged with antiplatelets')
        # % ischemic stroke patients discharged with antiplatelets
        self.statsDf['% ischemic stroke patients discharged with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged with antiplatelets']/x['except_recommended_patients']) * 100), 2) if x['except_recommended_patients'] > 0 else 0, axis=1)
        # afib patients discharged with anticoagulants
        self.statsDf['# afib patients discharged with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed)
        # % afib patients discharged with anticoagulants    
        self.statsDf['% afib patients discharged with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged with anticoagulants']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)
        
        #### STROKE UNIT ####
        # stroke patients treated in a dedicated stroke unit / ICU
        self.statsDf['# stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['# patients hospitalized in stroke unit / ICU']

        # % stroke patients treated in a dedicated stroke unit / ICU    
        self.statsDf['% stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['% patients hospitalized in stroke unit / ICU']

        # Get temporary dataframe with patients who have prescribed anticoagulats and were discharged home 
        anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([1])]
        # Get temporary dataframe with patients who have been discharge at home with detected aFib flutter and with prescribed antithrombotics
        afib_detected_discharged_home = afib_flutter_detected[(afib_flutter_detected['DISCHARGE_DESTINATION'].isin([1])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([9]))]

        # Check if temporary dataframe is empty. If yes, the value is calculated not only for discharged home, but only dead patients are excluded
        if (anticoagulants_prescribed_discharged_home.empty):
            # afib patients discharged home with anticoagulants 
            anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[~anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([5])]
            # Get temporary dataframe with patients who are not dead with detected aFib flutter and with prescribed antithrombotics 
            afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([9]))]
            # Get # afib patients discharged home with anticoagulants
            self.statsDf['# afib patients discharged home with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed_discharged_home)
            # Get afib patients discharged and not dead
            self.statsDf['afib_detected_discharged_home_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)
            # Get % afib patients discharge with anticoagulants and not dead
            self.statsDf['% afib patients discharged home with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged home with anticoagulants']/x['afib_detected_discharged_home_patients']) * 100), 2) if x['afib_detected_discharged_home_patients'] > 0 else 0, axis=1)
        else:
            # Get # afib patients discharged home with anticoagulants   
            self.statsDf['# afib patients discharged home with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed_discharged_home)
            # Get afib patients discharged home 
            self.statsDf['afib_detected_discharged_home_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)
            # Get % afib patients discharged home with anticoagulants
            self.statsDf['% afib patients discharged home with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged home with anticoagulants']/x['afib_detected_discharged_home_patients']) * 100), 2) if x['afib_detected_discharged_home_patients'] > 0 else 0, axis=1)

        # % stroke patients treated in a dedicated stroke unit / ICU (2nd)
        self.statsDf['% stroke patients treated in a dedicated stroke unit / ICU (2nd)'] = self.statsDf['% patients hospitalized in stroke unit / ICU']

        self.statsDf.fillna(0, inplace=True)

        self.statsDf.rename(columns={"Protocol ID": "Site ID"}, inplace=True)

        # Save results into .csv
        #self.statsDf.to_csv('results.csv', sep=',', index=False)
        #print(self.statsDf)

        self.sites = self._get_sites(self.statsDf)

    def _count_patients(self, dataframe):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            dataframe (dataframe): The dataframe with raw data. 

        Returns:
            dataframe (dataframe): The column with number of patients. 
        """

        tmpDf = dataframe.groupby(['Protocol ID']).size().reset_index(name='count_patients')
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.fillna(0, inplace=True)

        return factorDf['count_patients']

    def _get_values_only_columns(self, column_name, value, dataframe):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (string): The name of column for which we are want calculate number of patients. 
            dataframe (dataframe): The dataframe with raw data. 
            value (int): The integers value represents specific value from the dataframe[column_name]. 

        Returns:
            dataframe (dataframe): The column with number of patients. 
        """

        tmpDf = dataframe[dataframe[column_name] == value].reset_index()[['Protocol ID', 'count']]
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.fillna(0, inplace=True)

        return factorDf['count']

    def _get_values_for_factors(self, column_name, value, new_column_name):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (int): The integers value represents specific value from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
        """

        if (self.tmp[column_name].dtype != np.number):
            value = str(value)
        else:
            value = value 

        tmpDf = self.tmp[self.tmp[column_name] == value].reset_index()[['Protocol ID', 'count']]
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.rename(columns={'count': new_column_name}, inplace=True)
        factorDf.fillna(0, inplace=True)

        return factorDf

    def _get_values_for_factors_more_values(self, column_name, value, new_column_name):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (list): The list of integers representing specific values from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
        """

        tmpDf = self.tmp[self.tmp[column_name].isin(value)].reset_index()[['Protocol ID', 'count']]
        tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.rename(columns={'count': new_column_name}, inplace=True)
        factorDf.fillna(0, inplace=True)

        return factorDf

    def _get_values_for_factors_containing(self, column_name, value, new_column_name):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (str): The str of integers representing specific values from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
        """

        tmpDf = self.tmp[self.tmp[column_name].str.contains(value)].reset_index()[['Protocol ID', 'count']]
        tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.rename(columns={'count': new_column_name}, inplace=True)
        factorDf.fillna(0, inplace=True)

        return factorDf

    def _return_dataset(self):
        """Return the raw dataframe."""

        return self.df

    def _return_stats(self):
        """Return the dataframe with calculated statistics."""

        return self.statsDf

    def _get_sites(self, df):
        """Return list of sites in the dataframes."""

        site_ids = df['Site ID'].tolist()
        siteSet = set(site_ids)
        siteList = list(siteSet)
        return siteList

    def _return_sites(self):

        return self.sites