# -*- coding: utf-8 -*-
"""
Created on Thu Jul 09 13:28:05 2017

@author: Marie Jankujova
"""

import sys
import os
from datetime import datetime, time, date
import sqlite3
import pandas as pd
import numpy as np
from numpy import inf
import pytz
import logging
import scipy.stats as st
from scipy.stats import sem, t
from scipy import mean

class FilterDataset:
    """ The class filtrating the dataframe by date or by country. 

    :param df: the dataframe containing preprocessed data
    :type df: dataframe
    :param country: the country code to be included in the data
    :type country: str
    :param date1: the first date included in the filtered dataframe
    :type date1: date
    :param date2: the last date included in the filtered dataframe
    :type date2: date
    """

    def __init__(self, df, country=None, date1=None, date2=None, column='DISCHARGE_DATE', by_columns=False):

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.fdf = df.copy()
        self.country = country
        self.date1 = date1
        self.date2 = date2

        if self.country is not None:
            # Append "_" to the country code, because e.g. ES_MD was included in dataset for MD as well.
            country = self.country + "_" 

            self.fdf = self._filter_by_country()
            logging.info('FilterDataset: Data have been filtered for country {0}!'.format(self.country)) 

        if self.date1 is not None and self.date2 is not None:
            if not by_columns:
                if column == 'DISCHARGE_DATE':
                    self.fdf = self._filter_by_date()
                    logging.info('FilterDataset: Data have been filtered for date {0} - {1}!'.format(self.date1, self.date2))
                elif column == 'HOSPITAL_DATE':
                    self.fdf = self._filter_by_hospital_date()
                    logging.info('FilterDataset: Data have been filtered by hospital date for dates {} - {}!'.format(self.date1, self.date2))
            else:
                self.fdf = self._filter_by_hospital_and_discharge_date()
                logging.info('FilterDataset: Data have been filtered by hospital or discharge date for dates {} - {}!'.format(self.date1, self.date2))

        
    def _filter_by_country(self):
        """ The function filtering dataframe by country. 

        :returns: df -- the dataframe including only rows containing in Protocol ID the country code
        """
        df = self.fdf[self.fdf['Protocol ID'].str.startswith(self.country) == True].copy()

        return df

    def _filter_by_date(self):
        """ The function filtering dataframe by discharge date.
        
        :returns: df -- the dataframe including only rows where discharge date is in the period (date1, date2)
        """

        if isinstance(self.date1, datetime):
            self.date1 = self.date1.date()
        if isinstance(self.date2, datetime):
            self.date2 = self.date2.date()

        df = self.fdf[(self.fdf['DISCHARGE_DATE'] >= self.date1) & (self.fdf['DISCHARGE_DATE'] <= self.date2)].copy()

        return df

    def _filter_by_hospital_date(self):
        ''' The function filtering dataframe by admission date. 

        :returns df: the dataframe including only rows where admission date is between these two days
        '''
        if isinstance(self.date1, datetime):
            self.date1 = self.date1.date()
        if isinstance(self.date2, datetime):
            self.date2 = self.date2.date()

        df = self.fdf[(self.fdf['HOSPITAL_DATE'] >= self.date1) & (self.fdf['HOSPITAL_DATE'] <= self.date2)].copy()

        return df

    def _filter_by_hospital_and_discharge_date(self):
        ''' The function filters dataframe by admission and discharge date. Eg. include patient if hospital date or discharge date are in the range.

        '''
        if isinstance(self.date1, datetime):
            self.date1 = self.date1.date()
        if isinstance(self.date2, datetime):
            self.date2 = self.date2.date()

        df = self.fdf[((self.fdf['HOSPITAL_DATE'] >= self.date1) & (self.fdf['HOSPITAL_DATE'] <= self.date2)) | ((self.fdf['DISCHARGE_DATE'] >= self.date1) & (self.fdf['DISCHARGE_DATE'] <= self.date2))].copy()
        return df



class ComputeStats:
    """ The class calculating the general statistics from the preprocessed and filtered data. 

    :param df: the dataframe containing preprocessed data
    :type df: dataframe
    :param country: the results for whole country included in the statistics
    :type country: bool
    :param country_code: the country code used in the names of output files
    :type country_code: str
    :param comparison: the value saying if it is comparative statistics
    :type comparison: bool
    :param patient_limit: the number of patients used as limit when evaluating angels awards
    :type patiet_limit: int
    """


    def __init__(self, df, country = False, country_code = "", comparison=False, patient_limit=30, period=None, raw_data=None):

        self.df = df.copy()
        self.df.fillna(0, inplace=True)
        self.patient_limit = patient_limit
        self.period = period
        self.raw_data = raw_data

        # Rename 'RES-Q reports name' column to 'Site Name'
        if 'ESO Angels name' in self.df.columns:
            self.df.drop('Site Name', inplace=True, axis=1)
            self.df.rename(columns={'ESO Angels name': 'Site Name'}, inplace=True)

        def get_country_name(value):
            """ The function returning the country name based on country code. 
            
            :returns: country_name -- name of the country
            """
            if value == "UZB":
                value = 'UZ'
            country_name = pytz.country_names[value]

            return country_name

        #if comparison == False:
            #self.df['Protocol ID'] = self.df.apply(lambda row: row['Protocol ID'].split()[2] if (len(row['Protocol ID'].split()) == 3) else row['Protocol ID'].split()[0], axis=1)
            # uncomment if you want stats between countries and set comparison == True
            # self.df['Protocol ID'] = self.df.apply(lambda x: x['Protocol ID'].split("_")[0], axis=1)

        # If you want to compare, instead of Site Names will be Country names. 
        if comparison:
            self.df['Protocol ID'] = self.df['Country']
            self.df['Site Name'] = self.df['Country']
            #if self.df['Protocol ID'].dtype == np.object:
                #self.df['Site Name'] = self.df.apply(lambda x: get_country_name(x['Protocol ID']) if get_country_name(x['Protocol ID']) != "" else x['Protocol ID'], axis=1)
        
        if (country):
            country_df = self.df.copy()
            #self.country_name = pytz.country_names[country_code]
           # country['Protocol ID'] = self.country_name
            #country['Site Name'] = self.country_name
            country_df['Protocol ID'] = country_df['Country']
            country_df['Site Name'] = country_df['Country']
            self.df = pd.concat([self.df, country_df])
            self._country_name = country_df['Country'].iloc[0]
        else:
            self._country_name = ""
        
        self.statsDf = self.df.groupby(['Protocol ID', 'Site Name']).size().reset_index(name="Total Patients")
        # self.statsDf['Site Name'] = 

        self.statsDf = self.statsDf[['Protocol ID', 'Site Name', 'Total Patients']]
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
        is_tia = self.df[self.df['STROKE_TYPE'].isin([1, 3])]
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
        self.tmp = self.df.groupby(['Protocol ID', 'GENDER']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="GENDER", value=2, new_column_name='# patients female')
        self.statsDf['% patients female'] = self.statsDf.apply(lambda x: round(((x['# patients female']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="GENDER", value=1, new_column_name='# patients male')
        self.statsDf['% patients male'] = self.statsDf.apply(lambda x: round(((x['# patients male']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)


        ######################
        # STROKE IN HOSPITAL #
        ######################
        self.tmp = self.df.groupby(['Protocol ID', 'HOSPITAL_STROKE']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="HOSPITAL_STROKE", value=1, new_column_name='# patients having stroke in the hospital - Yes')
        self.statsDf['% patients having stroke in the hospital - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients having stroke in the hospital - Yes']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="HOSPITAL_STROKE", value=2, new_column_name='# patients having stroke in the hospital - No')
        self.statsDf['% patients having stroke in the hospital - No'] = self.statsDf.apply(lambda x: round(((x['# patients having stroke in the hospital - No']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        ####################
        # RECURRENT STROKE #
        ####################
        self.tmp = self.df.groupby(['Protocol ID', 'RECURRENT_STROKE']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=-999, new_column_name='tmp')
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=1, new_column_name='# recurrent stroke - Yes')
        self.statsDf['% recurrent stroke - Yes'] = self.statsDf.apply(lambda x: round(((x['# recurrent stroke - Yes']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECURRENT_STROKE", value=2, new_column_name='# recurrent stroke - No')
        self.statsDf['% recurrent stroke - No'] = self.statsDf.apply(lambda x: round(((x['# recurrent stroke - No']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # DEPARTMENT TYPE #
        ###################
        self.tmp = self.df.groupby(['Protocol ID', 'DEPARTMENT_TYPE']).size().to_frame('count').reset_index()
        # Get patients from old version
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=-999, new_column_name='tmp')
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=1, new_column_name='# department type - neurology')
        self.statsDf['% department type - neurology'] = self.statsDf.apply(lambda x: round(((x['# department type - neurology']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=2, new_column_name='# department type - neurosurgery')
        self.statsDf['% department type - neurosurgery'] = self.statsDf.apply(lambda x: round(((x['# department type - neurosurgery']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=3, new_column_name='# department type - anesthesiology/resuscitation/critical care')
        self.statsDf['% department type - anesthesiology/resuscitation/critical care'] = self.statsDf.apply(lambda x: round(((x['# department type - anesthesiology/resuscitation/critical care']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=4, new_column_name='# department type - internal medicine')
        self.statsDf['% department type - internal medicine'] = self.statsDf.apply(lambda x: round(((x['# department type - internal medicine']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=5, new_column_name='# department type - geriatrics')
        self.statsDf['% department type - geriatrics'] = self.statsDf.apply(lambda x: round(((x['# department type - geriatrics']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="DEPARTMENT_TYPE", value=6, new_column_name='# department type - Other')
        self.statsDf['% department type - Other'] = self.statsDf.apply(lambda x: round(((x['# department type - Other']/(x['Total Patients'] - x['tmp'])) * 100), 2) if (x['Total Patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # HOSPITALIZED IN #
        ###################
        self.tmp = self.df.groupby(['Protocol ID', 'HOSPITALIZED_IN']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=1, new_column_name='# patients hospitalized in stroke unit / ICU')
        self.statsDf['% patients hospitalized in stroke unit / ICU'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in stroke unit / ICU']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=2, new_column_name='# patients hospitalized in monitored bed with telemetry')
        self.statsDf['% patients hospitalized in monitored bed with telemetry'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in monitored bed with telemetry']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="HOSPITALIZED_IN", value=3, new_column_name='# patients hospitalized in standard bed')
        self.statsDf['% patients hospitalized in standard bed'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in standard bed']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        self.statsDf['# patients hospitalized in stroke unit / ICU or monitored bed'] = self.statsDf['# patients hospitalized in stroke unit / ICU'] + self.statsDf['# patients hospitalized in monitored bed with telemetry']
        self.statsDf['% patients hospitalized in stroke unit / ICU or monitored bed'] = self.statsDf.apply(lambda x: round(((x['# patients hospitalized in stroke unit / ICU or monitored bed']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

                

        ###############################
        # ASSESSED FOR REHABILITATION #
        ###############################
        self.tmp = is_ich_sah_cvt.groupby(['Protocol ID', 'ASSESSED_FOR_REHAB']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=3, new_column_name='# patients assessed for rehabilitation - Not known')
        self.statsDf['% patients assessed for rehabilitation - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - Not known']/x['is_ich_sah_cvt_patients']) * 100), 2) if x['is_ich_sah_cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=1, new_column_name='# patients assessed for rehabilitation - Yes')
        self.statsDf['% patients assessed for rehabilitation - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - Yes']/(x['is_ich_sah_cvt_patients'] - x['# patients assessed for rehabilitation - Not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# patients assessed for rehabilitation - Not known']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="ASSESSED_FOR_REHAB", value=2, new_column_name='# patients assessed for rehabilitation - No')
        self.statsDf['% patients assessed for rehabilitation - No'] = self.statsDf.apply(lambda x: round(((x['# patients assessed for rehabilitation - No']/(x['is_ich_sah_cvt_patients'] - x['# patients assessed for rehabilitation - Not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# patients assessed for rehabilitation - Not known']) > 0 else 0, axis=1)

        ###############
        # STROKE TYPE #
        ###############
        self.tmp = self.df.groupby(['Protocol ID', 'STROKE_TYPE']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=1, new_column_name='# stroke type - ischemic stroke')
        self.statsDf['% stroke type - ischemic stroke'] = self.statsDf.apply(lambda x: round(((x['# stroke type - ischemic stroke']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=2, new_column_name='# stroke type - intracerebral hemorrhage')
        self.statsDf['% stroke type - intracerebral hemorrhage'] = self.statsDf.apply(lambda x: round(((x['# stroke type - intracerebral hemorrhage']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=3, new_column_name='# stroke type - transient ischemic attack')
        self.statsDf['% stroke type - transient ischemic attack'] = self.statsDf.apply(lambda x: round(((x['# stroke type - transient ischemic attack']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=4, new_column_name='# stroke type - subarrachnoid hemorrhage')
        self.statsDf['% stroke type - subarrachnoid hemorrhage'] = self.statsDf.apply(lambda x: round(((x['# stroke type - subarrachnoid hemorrhage']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=5, new_column_name='# stroke type - cerebral venous thrombosis')
        self.statsDf['% stroke type - cerebral venous thrombosis'] = self.statsDf.apply(lambda x: round(((x['# stroke type - cerebral venous thrombosis']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="STROKE_TYPE", value=6, new_column_name='# stroke type - undetermined stroke')
        self.statsDf['% stroke type - undetermined stroke'] = self.statsDf.apply(lambda x: round(((x['# stroke type - undetermined stroke']/x['Total Patients']) * 100), 2) if x['Total Patients'] > 0 else 0, axis=1)

        #######################
        # CONSCIOUSNESS LEVEL #
        #######################
        self.tmp = is_ich_sah_cvt.groupby(['Protocol ID', 'CONSCIOUSNESS_LEVEL']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=5, new_column_name='# level of consciousness - not known')
        self.statsDf['% level of consciousness - not known'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - not known']/x['is_ich_sah_cvt_patients']) * 100), 2) if x['is_ich_sah_cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=1, new_column_name='# level of consciousness - alert')
        self.statsDf['% level of consciousness - alert'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - alert']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=2, new_column_name='# level of consciousness - drowsy')
        self.statsDf['% level of consciousness - drowsy'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - drowsy']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=3, new_column_name='# level of consciousness - comatose')
        self.statsDf['% level of consciousness - comatose'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - comatose']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CONSCIOUSNESS_LEVEL", value=4, new_column_name='# level of consciousness - GCS')
        self.statsDf['% level of consciousness - GCS'] = self.statsDf.apply(lambda x: round(((x['# level of consciousness - GCS']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        #######
        # GCS #
        #######
        # Get temporary dataframe with the level of consciousness - GCS
        gcs = is_ich_sah_cvt[is_ich_sah_cvt['CONSCIOUSNESS_LEVEL'].isin([4])].copy()
        # Calculate total number of patients with GCS level of consciousness per site
        self.statsDf['gcs_patients'] = self._count_patients(dataframe=gcs)
        self.tmp = gcs.groupby(['Protocol ID', 'GCS']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=1, new_column_name='# GCS - 15-13')
        self.statsDf['% GCS - 15-13'] = self.statsDf.apply(lambda x: round(((x['# GCS - 15-13']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=2, new_column_name='# GCS - 12-8')
        self.statsDf['% GCS - 12-8'] = self.statsDf.apply(lambda x: round(((x['# GCS - 12-8']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="GCS", value=3, new_column_name='# GCS - <8')
        self.statsDf['% GCS - <8'] = self.statsDf.apply(lambda x: round(((x['# GCS - <8']/x['gcs_patients']) * 100), 2) if x['gcs_patients'] > 0 else 0, axis=1)
        self.statsDf.drop(['gcs_patients'], inplace=True, axis=1)

        # GCS is mapped to the consciousness level. GCS 15-13 is mapped to alert, GCS 12-8 to drowsy and GCS < 8 to comatose
        self.statsDf['alert_all'] = self.statsDf['# level of consciousness - alert'] + self.statsDf['# GCS - 15-13']
        self.statsDf['alert_all_perc'] = self.statsDf.apply(lambda x: round(((x['alert_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        self.statsDf['drowsy_all'] = self.statsDf['# level of consciousness - drowsy'] + self.statsDf['# GCS - 12-8']
        self.statsDf['drowsy_all_perc'] = self.statsDf.apply(lambda x: round(((x['drowsy_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)
        self.statsDf['comatose_all'] = self.statsDf['# level of consciousness - comatose'] + self.statsDf['# GCS - <8']
        self.statsDf['comatose_all_perc'] = self.statsDf.apply(lambda x: round(((x['comatose_all']/(x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known'])) * 100), 2) if (x['is_ich_sah_cvt_patients'] - x['# level of consciousness - not known']) > 0 else 0, axis=1)

        #########
        # NIHSS #
        #########
        # Seperate calculation for CZ 
        if country_code == 'CZ':
            self.tmp = is_ich.groupby(['Protocol ID', 'NIHSS']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=1, new_column_name='# NIHSS - Not performed')
            self.statsDf['% NIHSS - Not performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not performed']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=2, new_column_name='# NIHSS - Performed')
            self.statsDf['% NIHSS - Performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Performed']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=3, new_column_name='# NIHSS - Not known')
            self.statsDf['% NIHSS - Not known'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not known']/x['is_ich_patients']) * 100), 2) if x['is_ich_patients'] > 0 else 0, axis=1)
            # Create temporary dataframe with patient who had performed NIHSS (NIHSS = 2)
            nihss = is_ich[is_ich['NIHSS'].isin([2])]
            tmpDf = nihss.groupby(['Protocol ID']).NIHSS_SCORE.agg(['median']).rename(columns={'median': 'NIHSS median score'})
            factorDf = self.statsDf.merge(tmpDf, how='outer', left_on='Protocol ID', right_on='Protocol ID')
            factorDf.fillna(0, inplace=True)
            self.statsDf['NIHSS median score'] = factorDf['NIHSS median score']
        else:
            self.tmp = is_ich_cvt.groupby(['Protocol ID', 'NIHSS']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=1, new_column_name='# NIHSS - Not performed')
            self.statsDf['% NIHSS - Not performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not performed']/x['is_ich_cvt_patients']) * 100), 2) if x['is_ich_cvt_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=2, new_column_name='# NIHSS - Performed')
            self.statsDf['% NIHSS - Performed'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Performed']/x['is_ich_cvt_patients']) * 100), 2) if x['is_ich_cvt_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NIHSS", value=3, new_column_name='# NIHSS - Not known')
            self.statsDf['% NIHSS - Not known'] = self.statsDf.apply(lambda x: round(((x['# NIHSS - Not known']/x['is_ich_cvt_patients']) * 100), 2) if x['is_ich_cvt_patients'] > 0 else 0, axis=1)
            # Create temporary dataframe with patient who had performed NIHSS (NIHSS = 2)
            nihss = is_ich_cvt[is_ich_cvt['NIHSS'].isin([2])]
            tmpDf = nihss.groupby(['Protocol ID']).NIHSS_SCORE.agg(['median']).rename(columns={'median': 'NIHSS median score'})
            factorDf = self.statsDf.merge(tmpDf, how='outer', left_on='Protocol ID', right_on='Protocol ID')
            factorDf.fillna(0, inplace=True)
            self.statsDf['NIHSS median score'] = factorDf['NIHSS median score']

        ##########
        # CT/MRI #
        ##########
        self.tmp = is_ich_tia_cvt.groupby(['Protocol ID', 'CT_MRI']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=1, new_column_name='# CT/MRI - Not performed')
        self.statsDf['% CT/MRI - Not performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Not performed']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=2, new_column_name='# CT/MRI - performed')
        self.statsDf['% CT/MRI - performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - performed']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=3, new_column_name='# CT/MRI - Not known')
        self.statsDf['% CT/MRI - Not known'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Not known']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)

        # Create temporary dataframe with patients who had performed CT/MRI (CT_MRI = 2)
        ct_mri = is_ich_tia_cvt[is_ich_tia_cvt['CT_MRI'].isin([2])]
        ct_mri['CT_TIME'] = pd.to_numeric(ct_mri['CT_TIME'])
        self.tmp = ct_mri.groupby(['Protocol ID', 'CT_TIME']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="CT_TIME", value=1, new_column_name='# CT/MRI - Performed within 1 hour after admission')
        self.statsDf['% CT/MRI - Performed within 1 hour after admission'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Performed within 1 hour after admission']/x['# CT/MRI - performed']) * 100), 2) if x['# CT/MRI - performed'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="CT_TIME", value=2, new_column_name='# CT/MRI - Performed later than 1 hour after admission')
        self.statsDf['% CT/MRI - Performed later than 1 hour after admission'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Performed later than 1 hour after admission']/x['# CT/MRI - performed']) * 100), 2) if x['# CT/MRI - performed'] > 0 else 0, axis=1)

        ####################
        # VASCULAR IMAGING #
        ####################
        self.tmp = ich_sah.groupby(['Protocol ID', 'CTA_MRA_DSA']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'1', '1,2', '1,3'}, new_column_name='# vascular imaging - CTA')
        self.statsDf['% vascular imaging - CTA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - CTA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'2', '1,2', '2,3'}, new_column_name='# vascular imaging - MRA')
        self.statsDf['% vascular imaging - MRA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - MRA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'3', '1,3', '2,3'}, new_column_name='# vascular imaging - DSA')
        self.statsDf['% vascular imaging - DSA'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - DSA']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'4'}, new_column_name='# vascular imaging - None')
        self.statsDf['% vascular imaging - None'] = self.statsDf.apply(lambda x: round(((x['# vascular imaging - None']/x['ich_sah_patients']) * 100), 2) if x['ich_sah_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_more_values(column_name="CTA_MRA_DSA", value={'1,2', '1,3', '2,3'}, new_column_name='# vascular imaging - two modalities')
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
        # Seperate calculation for CZ (difference in the stroke types)
        if country_code == 'CZ':
            self.tmp = is_ich.groupby(['Protocol ID', 'VENTILATOR']).size().to_frame('count').reset_index()
            # Get number of patients from the old version
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=-999, new_column_name='tmp')
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=3, new_column_name='# patients put on ventilator - Not known')
            self.statsDf['% patients put on ventilator - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Not known']/(x['is_ich_patients'] - x['tmp'])) * 100), 2) if (x['is_ich_patients'] - x['tmp']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=1, new_column_name='# patients put on ventilator - Yes')
            self.statsDf['% patients put on ventilator - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Yes']/(x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=2, new_column_name='# patients put on ventilator - No')
            self.statsDf['% patients put on ventilator - No'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - No']/(x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)
            self.statsDf.drop(['tmp'], inplace=True, axis=1)
        else:
            self.tmp = is_ich_cvt.groupby(['Protocol ID', 'VENTILATOR']).size().to_frame('count').reset_index()
            # Get number of patients from the old version
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=-999, new_column_name='tmp')
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=3, new_column_name='# patients put on ventilator - Not known')
            self.statsDf['% patients put on ventilator - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Not known']/(x['is_ich_cvt_patients'] - x['tmp'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['tmp']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=1, new_column_name='# patients put on ventilator - Yes')
            self.statsDf['% patients put on ventilator - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - Yes']/(x['is_ich_cvt_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="VENTILATOR", value=2, new_column_name='# patients put on ventilator - No')
            self.statsDf['% patients put on ventilator - No'] = self.statsDf.apply(lambda x: round(((x['# patients put on ventilator - No']/(x['is_ich_cvt_patients'] - x['tmp'] - x['# patients put on ventilator - Not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['tmp'] - x['# patients put on ventilator - Not known']) > 0 else 0, axis=1)
            self.statsDf.drop(['tmp'], inplace=True, axis=1)

        #############################
        # RECANALIZATION PROCEDURES #
        #############################
        self.tmp = isch.groupby(['Protocol ID', 'RECANALIZATION_PROCEDURES']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=1, new_column_name='# recanalization procedures - Not done')
        self.statsDf['% recanalization procedures - Not done'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Not done']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=2, new_column_name='# recanalization procedures - IV tPa')
        self.statsDf['% recanalization procedures - IV tPa'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=3, new_column_name='# recanalization procedures - IV tPa + endovascular treatment')
        self.statsDf['% recanalization procedures - IV tPa + endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa + endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=4, new_column_name='# recanalization procedures - Endovascular treatment alone')
        self.statsDf['% recanalization procedures - Endovascular treatment alone'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Endovascular treatment alone']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=5, new_column_name='# recanalization procedures - IV tPa + referred to another centre for endovascular treatment')
        self.statsDf['% recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=6, new_column_name='# recanalization procedures - Referred to another centre for endovascular treatment')
        self.statsDf['% recanalization procedures - Referred to another centre for endovascular treatment'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred to another centre for endovascular treatment']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=7, new_column_name='# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre')
        self.statsDf['% recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=8, new_column_name='# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre')
        self.statsDf['% recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="RECANALIZATION_PROCEDURES", value=9, new_column_name='# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre')
        self.statsDf['% recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] = self.statsDf.apply(lambda x: round(((x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        # Get recanalization procedure differently for CZ, they are taking the possible values differently
        if country_code == 'CZ':
            # self.statsDf['# patients recanalized'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] +  x['# recanalization procedures - Endovascular treatment alone']  + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'], axis=1)
            recanalized_df = isch.loc[isch['IVT_DONE'].isin([1]) | isch['TBY_DONE'].isin([1])]
            self.statsDf['# patients recanalized'] = self._count_patients(dataframe=recanalized_df)

            recanalized_denominator_df = isch.loc[isch['IVT_DONE'].isin([1]) | isch['TBY_DONE'].isin([1]) | isch['RECANALIZATION_PROCEDURES'].isin([1])]
            self.statsDf['denominator'] =self._count_patients(dataframe=recanalized_denominator_df)

            #self.statsDf['# patients recanalized'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] +  x['# recanalization procedures - Endovascular treatment alone'], axis=1)

            #self.statsDf['% patients recanalized'] = self.statsDf.apply(lambda x: round(((x['# patients recanalized']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']) > 0 else 0, axis=1)

            #self.statsDf['% patients recanalized'] = self.statsDf.apply(lambda x: round(((x['# patients recanalized']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']) > 0 else 0, axis=1)
            self.statsDf['% patients recanalized'] = self.statsDf.apply(lambda x: round(((x['# patients recanalized']/x['denominator']) * 100), 2) if x['denominator'] > 0 else 0, axis=1)
            self.statsDf.drop(['denominator'], inplace=True, axis=1)
        else:
            self.statsDf['# patients recanalized'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] +  x['# recanalization procedures - Endovascular treatment alone'], axis=1)

            self.statsDf['% patients recanalized'] = self.statsDf.apply(lambda x: round(((x['# patients recanalized']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']) > 0 else 0, axis=1)

        ##############
        # MEDIAN DTN #
        ##############
        def _median_confidence_interval(data, confidence=0.95):
            """ The function calculating median confidence interval. 

            :param confidence: the value of confidence interval
            :type confidence: int/float
            :returns: rv.median(), rv.interval(confidence)
            """
            a = np.array(data)
            w = a + 1

            # create custom discrete random variable from data set
            rv = st.rv_discrete(values=(data, w/w.sum()))

            return rv.median(), rv.interval(confidence)
            
        def _mean_confidence_interval(data, confidence=0.95):
            """ The function calculating mean confidence interval. 

            :param confidence: the value of confidence interval
            :type confidence: int/float
            :returns: m, m-h, m+h
            """

            n = len(data)
            m = mean(data)
            std_err = sem(data)
            h = std_err * t.ppf((1 + confidence) / 2, n - 1)
            return m, m-h, m+h

        if country_code == 'CZ':
            self.tmp = isch.groupby(['Protocol ID', 'IVT_DONE']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="IVT_DONE", value=1, new_column_name='# IV tPa')
            self.statsDf['% IV tPa'] = self.statsDf.apply(lambda x: round(((x['# IV tPa']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
            
            # Create temporary dataframe with the patients who has been treated with thrombolysis
            recanalization_procedure_iv_tpa = isch[isch['IVT_DONE'].isin([1])].copy()
            recanalization_procedure_iv_tpa.fillna(0, inplace=True)
            # Create one column with times of door to thrombolysis 
            thrombolysis = recanalization_procedure_iv_tpa[(recanalization_procedure_iv_tpa['IVTPA'] > 0) & (recanalization_procedure_iv_tpa['IVTPA'] <= 400)].copy()

            tmp = thrombolysis.groupby(['Protocol ID']).IVTPA.agg(['median']).rename(columns={'median': 'Median DTN (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)

        else:
            self.statsDf.loc[:, '# IV tPa'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)
            self.statsDf['% IV tPa'] = self.statsDf.apply(lambda x: round(((x['# IV tPa']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

            # Create temporary dataframe with the patients who has been treated with thrombolysis
            recanalization_procedure_iv_tpa = isch[isch['RECANALIZATION_PROCEDURES'].isin([2, 3, 5])].copy()
            recanalization_procedure_iv_tpa.fillna(0, inplace=True)
            # Create one column with times of door to thrombolysis 
            recanalization_procedure_iv_tpa['IVTPA'] = recanalization_procedure_iv_tpa['IVT_ONLY_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_ONLY_NEEDLE_TIME_MIN'] + recanalization_procedure_iv_tpa['IVT_TBY_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_TBY_NEEDLE_TIME_MIN'] + recanalization_procedure_iv_tpa['IVT_TBY_REFER_NEEDLE_TIME'] + recanalization_procedure_iv_tpa['IVT_TBY_REFER_NEEDLE_TIME_MIN']

        
        # sites_ids = recanalization_procedure_iv_tpa['Protocol ID'].tolist()
        # sites_ids = set(sites_ids)
        # interval_vals = {}		
        # for idx, val in enumerate(sites_ids): 
        #     meanv, lbound, ubound = _mean_confidence_interval(recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['Protocol ID'] == val]['IVTPA'].tolist())
        #     medianv, interval_median = _median_confidence_interval(recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['Protocol ID'] == val]['IVTPA'].tolist())
        #     interval_vals[str(idx)] = [val, "({0:.2f},{1:.2f})".format(lbound, ubound), "{0}".format(interval_median)]

        #     #interval_vals.append("{0}: ({1}-{2})".format(i, lowb, upb))
        # #print(interval_vals)
        # interval_vals_df = pd.DataFrame.from_dict(interval_vals, orient='index', columns=['Protocol ID', 'Confidence interval DTN (Mean)', 'Confidence interval DTN (Median)'])
        
            tmp = recanalization_procedure_iv_tpa.groupby(['Protocol ID']).IVTPA.agg(['median']).rename(columns={'median': 'Median DTN (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)

        # self.statsDf = self.statsDf.merge(interval_vals_df, how='outer')
        
        ##############
        # MEDIAN DTG #
        ##############
        # Seperate calculation of TBY for CZ
        if country_code == 'CZ':
            self.tmp = isch.groupby(['Protocol ID', 'TBY_DONE']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="TBY_DONE", value=1, new_column_name='# TBY')
            self.statsDf['% TBY'] = self.statsDf.apply(lambda x: round(((x['# TBY']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
            
            # Create temporary dataframe with the patients who has been treated with thrombolysis
            recanalization_procedure_tby_dtg = isch[isch['TBY_DONE'].isin([1])].copy()
            recanalization_procedure_tby_dtg.fillna(0, inplace=True)
            # Create one column with times of door to thrombolysis 
            thrombectomy = recanalization_procedure_tby_dtg[(recanalization_procedure_tby_dtg['TBY'] > 0) & (recanalization_procedure_tby_dtg['TBY'] <= 700)].copy()

            tmp = thrombectomy.groupby(['Protocol ID']).TBY.agg(['median']).rename(columns={'median': 'Median DTG (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)

            # self.statsDf.loc[:, '# TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'], axis=1)
            """
            self.statsDf.loc[:, '# TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + endovascular treatment'], axis=1)
            self.statsDf['% TBY'] = self.statsDf.apply(lambda x: round(((x['# TBY']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

            # Create temporary dataframe with the patients who has been treated with thrombectomy
            # recanalization_procedure_tby_dtg = isch[isch['RECANALIZATION_PROCEDURES'].isin([4, 3, 6, 7, 8])].copy()
            recanalization_procedure_tby_dtg = isch[isch['RECANALIZATION_PROCEDURES'].isin([4, 3])].copy()
            recanalization_procedure_tby_dtg.fillna(0, inplace=True)

            # Get IVTPA in minutes
            # recanalization_procedure_tby_dtg['TBY'] = recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_TIME_MIN'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME_MIN'] + recanalization_procedure_tby_dtg['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'] + recanalization_procedure_tby_dtg['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN']
            recanalization_procedure_tby_dtg['TBY'] = recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_TIME_MIN'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME_MIN']
            """
           
            # sites_ids = recanalization_procedure_tby_dtg['Protocol ID'].tolist()
            # sites_ids = set(sites_ids)
            # interval_vals = {}		
            # for idx, val in enumerate(sites_ids): 
            #     meanv, lbound, ubound = _mean_confidence_interval(recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['Protocol ID'] == val]['TBY'].tolist())
            #     medianv, interval_median = _median_confidence_interval(recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['Protocol ID'] == val]['TBY'].tolist())
            #     interval_vals[str(idx)] = [val, "({0:.2f}-{1:.2f})".format(lbound, ubound), "{0}".format(interval_median)]

            # interval_vals_df = pd.DataFrame.from_dict(interval_vals, orient='index', columns=['Protocol ID', 'Confidence interval DTG (Mean)', 'Confidence interval DTG (Median)'])
            
            # recanalization_procedure_tby['TBY'] = recanalization_procedure_tby.loc[:, ['TBY_ONLY_GROIN_PUNCTURE_TIME', 'TBY_ONLY_GROIN_PUNCTURE_TIME_MIN', 'IVT_TBY_GROIN_TIME', 'IVT_TBY_GROIN_TIME_MIN']].sum(1).reset_index()[0].tolist()
        else:
            self.statsDf.loc[:, '# TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + endovascular treatment'], axis=1)
            self.statsDf['% TBY'] = self.statsDf.apply(lambda x: round(((x['# TBY']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
            # Create temporary dataframe with the patients who has been treated with thrombectomy
            recanalization_procedure_tby_dtg = isch[isch['RECANALIZATION_PROCEDURES'].isin([4, 3])].copy()
            recanalization_procedure_tby_dtg.fillna(0, inplace=True)
            # Create one column with times of door to thrombectomy 
            recanalization_procedure_tby_dtg['TBY'] = recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_PUNCTURE_TIME'] + recanalization_procedure_tby_dtg['TBY_ONLY_GROIN_TIME_MIN'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME'] + recanalization_procedure_tby_dtg['IVT_TBY_GROIN_TIME_MIN']

            # sites_ids = recanalization_procedure_tby_dtg['Protocol ID'].tolist()
            # sites_ids = set(sites_ids)
            # interval_vals = {}		
            # for idx, val in enumerate(sites_ids): 
            #     meanv, lbound, ubound = _mean_confidence_interval(recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['Protocol ID'] == val]['IVTPA'].tolist())
            #     medianv, interval_median = _median_confidence_interval(recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['Protocol ID'] == val]['IVTPA'].tolist())
            #     interval_vals[str(idx)] = [val, "({0:.2f}-{1:.2f})".format(lbound, ubound), "{0}".format(interval_median)]
            
            # interval_vals_df = pd.DataFrame.from_dict(interval_vals, orient='index', columns=['Protocol ID', 'Confidence interval DTG (Mean)', 'Confidence interval DTG (Median)'])
            
            # recanalization_procedure_tby['TBY'] = recanalization_procedure_tby.loc[:, ['TBY_ONLY_GROIN_PUNCTURE_TIME', 'TBY_ONLY_GROIN_PUNCTURE_TIME_MIN', 'IVT_TBY_GROIN_TIME', 'IVT_TBY_GROIN_TIME_MIN']].sum(1).reset_index()[0].tolist()

            tmp = recanalization_procedure_tby_dtg.groupby(['Protocol ID']).TBY.agg(['median']).rename(columns={'median': 'Median DTG (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)

        # self.statsDf = self.statsDf.merge(interval_vals_df, how='outer')

        ###############
        # MEDIAN DIDO #
        ###############
        if country_code == 'CZ':
            # self.statsDf.loc[:, '# DIDO TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment'], axis=1)
            self.statsDf.loc[:, '# DIDO TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'], axis=1)

            # self.statsDf['% DIDO TBY'] = self.statsDf.apply(lambda x: round(((x['# DIDO TBY']/(x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done']) > 0 else 0, axis=1)
            
            # Get only patients recanalized TBY
            # recanalization_procedure_tby_dido = isch[isch['RECANALIZATION_PROCEDURES'].isin([5, 6, 7, 8])].copy()

            # For CZ remove referred for endovascular treatment from DIDO time because they are taking it as the patient was referred to them for TBY
            # recanalization_procedure_tby_dido = isch[isch['RECANALIZATION_PROCEDURES'].isin([5, 6])].copy()
            
            # Create temporary dataframe with the patients who has been transferred for recanalization procedures
            recanalization_procedure_tby_dido = isch[isch['RECANALIZATION_PROCEDURES'].isin([5, 6, 7, 8])].copy()
            recanalization_procedure_tby_dido.fillna(0, inplace=True)

            # Get DIDO in minutes
            # recanalization_procedure_tby_dido['DIDO'] = recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME_MIN']
            # recanalization_procedure_tby_dido['DIDO'] = recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME_MIN']

            # Create one column with times of door-in door-out time 
            recanalization_procedure_tby_dido['DIDO'] = recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME_MIN']

            tmp = recanalization_procedure_tby_dido.groupby(['Protocol ID']).DIDO.agg(['median']).rename(columns={'median': 'Median TBY DIDO (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)
        else:
            self.statsDf.loc[:, '# DIDO TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment'] + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'], axis=1)
            # self.statsDf['% DIDO TBY'] = self.statsDf.apply(lambda x: round(((x['# DIDO TBY']/(x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Not done']) > 0 else 0, axis=1)

            # Create temporary dataframe with the patients who has been transferred for recanalization procedures
            recanalization_procedure_tby_dido = isch[isch['RECANALIZATION_PROCEDURES'].isin([5, 6, 7, 8])].copy()
            recanalization_procedure_tby_dido.fillna(0, inplace=True)

            # Create one column with times of door-in door-out time 
            recanalization_procedure_tby_dido['DIDO'] = recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['IVT_TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_ALL_DIDO_TIME_MIN'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME'] + recanalization_procedure_tby_dido['TBY_REFER_LIM_DIDO_TIME_MIN']

            tmp = recanalization_procedure_tby_dido.groupby(['Protocol ID']).DIDO.agg(['median']).rename(columns={'median': 'Median TBY DIDO (minutes)'}).reset_index()
            self.statsDf = self.statsDf.merge(tmp, how='outer')
            self.statsDf.fillna(0, inplace=True)
        

        #######################
        # DYPSHAGIA SCREENING #
        #######################
        # For CZ exclude CVT from the calculation 
        # tag::dysphagia_screening[]
        if country_code == 'CZ':
            is_ich_not_referred = is_ich.loc[~(is_ich['crf_parent_name'].isin(['F_RESQ_IVT_TBY_CZ_4']) & is_ich['RECANALIZATION_PROCEDURES'].isin([5,6]))].copy()
            self.statsDf['is_ich_not_referred_patients'] = self._count_patients(dataframe=is_ich_not_referred) 
            
            self.tmp = is_ich_not_referred.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=6, new_column_name='# dysphagia screening - not known')
            self.statsDf['% dysphagia screening - not known'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - not known']/x['is_ich_not_referred_patients']) * 100), 2) if x['is_ich_not_referred_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=1, new_column_name='# dysphagia screening - Guss test')
            self.statsDf['% dysphagia screening - Guss test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Guss test']/(x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=2, new_column_name='# dysphagia screening - Other test')
            self.statsDf['% dysphagia screening - Other test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Other test']/(x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=3, new_column_name='# dysphagia screening - Another centre')
            self.statsDf['% dysphagia screening - Another centre'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Another centre']/(x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=4, new_column_name='# dysphagia screening - Not done')
            self.statsDf['% dysphagia screening - Not done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Not done']/(x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=5, new_column_name='# dysphagia screening - Unable to test')
            self.statsDf['% dysphagia screening - Unable to test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Unable to test']/(x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_not_referred_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            # self.statsDf['# dysphagia screening done'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test'] + self.statsDf['# dysphagia screening - Another centre']
            self.statsDf['# dysphagia screening done'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test']
            # self.statsDf['% dysphagia screening done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening done']/(x['is_ich_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf['% dysphagia screening done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening done']/(x['# dysphagia screening done'] + x['# dysphagia screening - Not done'])) * 100), 2) if (x['# dysphagia screening done'] + x['# dysphagia screening - Not done']) > 0 else 0, axis=1)
        else:
            self.tmp = is_ich_cvt.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=6, new_column_name='# dysphagia screening - not known')
            self.statsDf['% dysphagia screening - not known'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - not known']/x['is_ich_cvt_patients']) * 100), 2) if x['is_ich_cvt_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=1, new_column_name='# dysphagia screening - Guss test')
            self.statsDf['% dysphagia screening - Guss test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Guss test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=2, new_column_name='# dysphagia screening - Other test')
            self.statsDf['% dysphagia screening - Other test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Other test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=3, new_column_name='# dysphagia screening - Another centre')
            self.statsDf['% dysphagia screening - Another centre'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Another centre']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=4, new_column_name='# dysphagia screening - Not done')
            self.statsDf['% dysphagia screening - Not done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Not done']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=5, new_column_name='# dysphagia screening - Unable to test')
            self.statsDf['% dysphagia screening - Unable to test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Unable to test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
            self.statsDf['# dysphagia screening done'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test'] + self.statsDf['# dysphagia screening - Another centre']
            self.statsDf['% dysphagia screening done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening done']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)
        # end::dysphagia_screening[]

        ############################
        # DYPSHAGIA SCREENING TIME #
        ############################
        self.tmp = self.df.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING_TIME']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING_TIME", value=1, new_column_name='# dysphagia screening time - Within first 24 hours')
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING_TIME", value=2, new_column_name='# dysphagia screening time - After first 24 hours')
        self.statsDf['% dysphagia screening time - Within first 24 hours'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening time - Within first 24 hours']/(x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours'])) * 100), 2) if (x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours']) > 0 else 0, axis=1)
        self.statsDf['% dysphagia screening time - After first 24 hours'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening time - After first 24 hours']/(x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours'])) * 100), 2) if (x['# dysphagia screening time - Within first 24 hours'] + x['# dysphagia screening time - After first 24 hours']) > 0 else 0, axis=1)

        ###################
        # HEMICRANIECTOMY #
        ###################
        self.tmp = isch.groupby(['Protocol ID', 'HEMICRANIECTOMY']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=1, new_column_name='# hemicraniectomy - Yes')
        self.statsDf['% hemicraniectomy - Yes'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - Yes']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=2, new_column_name='# hemicraniectomy - No')
        self.statsDf['% hemicraniectomy - No'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - No']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="HEMICRANIECTOMY", value=3, new_column_name='# hemicraniectomy - Referred to another centre')
        self.statsDf['% hemicraniectomy - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# hemicraniectomy - Referred to another centre']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        ################
        # NEUROSURGERY #
        ################
        self.tmp = ich.groupby(['Protocol ID', 'NEUROSURGERY']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=3, new_column_name='# neurosurgery - Not known')
        self.statsDf['% neurosurgery - Not known'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - Not known']/x['ich_patients']) * 100), 2) if x['ich_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=1, new_column_name='# neurosurgery - Yes')
        self.statsDf['% neurosurgery - Yes'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - Yes']/(x['ich_patients'] - x['# neurosurgery - Not known'])) * 100), 2) if (x['ich_patients'] - x['# neurosurgery - Not known']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY", value=2, new_column_name='# neurosurgery - No')
        self.statsDf['% neurosurgery - No'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery - No']/(x['ich_patients'] - x['# neurosurgery - Not known'])) * 100), 2) if (x['ich_patients'] - x['# neurosurgery - Not known']) > 0 else 0, axis=1)

        #####################
        # NEUROSURGERY TYPE #
        #####################
        # Create temporary dataframe of patients who have undergone neurosurgery 
        neurosurgery = ich[ich['NEUROSURGERY'].isin([1])].copy()

        if neurosurgery.empty:
            # If no data available set 0 to all variables
            self.statsDf['neurosurgery_patients'] = 0
            self.statsDf['# neurosurgery type - intracranial hematoma evacuation'] = 0
            self.statsDf['% neurosurgery type - intracranial hematoma evacuation'] = 0
            self.statsDf['# neurosurgery type - external ventricular drainage'] = 0
            self.statsDf['% neurosurgery type - external ventricular drainage'] = 0
            self.statsDf['# neurosurgery type - decompressive craniectomy'] = 0
            self.statsDf['% neurosurgery type - decompressive craniectomy'] = 0
            self.statsDf['# neurosurgery type - Referred to another centre'] = 0
            self.statsDf['% neurosurgery type - Referred to another centre'] = 0
        else:
            self.tmp = neurosurgery.groupby(['Protocol ID', 'NEUROSURGERY_TYPE']).size().to_frame('count').reset_index()
            self.statsDf['neurosurgery_patients'] = self._count_patients(dataframe=neurosurgery)
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=1, new_column_name='# neurosurgery type - intracranial hematoma evacuation')
            self.statsDf['% neurosurgery type - intracranial hematoma evacuation'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - intracranial hematoma evacuation']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=2, new_column_name='# neurosurgery type - external ventricular drainage')
            self.statsDf['% neurosurgery type - external ventricular drainage'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - external ventricular drainage']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=3, new_column_name='# neurosurgery type - decompressive craniectomy')
            self.statsDf['% neurosurgery type - decompressive craniectomy'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - decompressive craniectomy']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)
            self.statsDf = self._get_values_for_factors(column_name="NEUROSURGERY_TYPE", value=4, new_column_name='# neurosurgery type - Referred to another centre')
            self.statsDf['% neurosurgery type - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# neurosurgery type - Referred to another centre']/x['neurosurgery_patients']) * 100), 2) if x['neurosurgery_patients'] > 0 else 0, axis=1)

        ###################
        # BLEEDING REASON #
        ###################
        self.tmp = ich.groupby(['Protocol ID', 'BLEEDING_REASON']).size().to_frame('count').reset_index()
        self.tmp['BLEEDING_REASON'] = self.tmp['BLEEDING_REASON'].astype(str)
        # Get number of patients entered in older form
        self.statsDf = self._get_values_for_factors(column_name="BLEEDING_REASON", value='-999', new_column_name='tmp')
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value='1', new_column_name='# bleeding reason - arterial hypertension')
        self.statsDf['% bleeding reason - arterial hypertension'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - arterial hypertension']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="2", new_column_name='# bleeding reason - aneurysm')
        self.statsDf['% bleeding reason - aneurysm'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - aneurysm']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="3", new_column_name='# bleeding reason - arterio-venous malformation')
        self.statsDf['% bleeding reason - arterio-venous malformation'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - arterio-venous malformation']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="4", new_column_name='# bleeding reason - anticoagulation therapy')
        self.statsDf['% bleeding reason - anticoagulation therapy'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - anticoagulation therapy']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="5", new_column_name='# bleeding reason - amyloid angiopathy')
        self.statsDf['% bleeding reason - amyloid angiopathy'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - amyloid angiopathy']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value="6", new_column_name='# bleeding reason - Other')
        self.statsDf['% bleeding reason - Other'] = self.statsDf.apply(lambda x: round(((x['# bleeding reason - Other']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)

        ### DATA NORMALIZATION
        norm_tmp = self.statsDf[['% bleeding reason - arterial hypertension', '% bleeding reason - aneurysm', '% bleeding reason - arterio-venous malformation', '% bleeding reason - anticoagulation therapy', '% bleeding reason - amyloid angiopathy', '% bleeding reason - Other']].copy()
        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)
        self.statsDf['bleeding_arterial_hypertension_perc_norm'] = ((norm_tmp['% bleeding reason - arterial hypertension']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['bleeding_aneurysm_perc_norm'] = ((norm_tmp['% bleeding reason - aneurysm']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['bleeding_arterio_venous_malformation_perc_norm'] = ((norm_tmp['% bleeding reason - arterio-venous malformation']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['bleeding_anticoagulation_therapy_perc_norm'] = ((norm_tmp['% bleeding reason - anticoagulation therapy']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['bleeding_amyloid_angiopathy_perc_norm'] = ((norm_tmp['% bleeding reason - amyloid angiopathy']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['bleeding_other_perc_norm'] = ((norm_tmp['% bleeding reason - Other']/norm_tmp['rowsums']) * 100).round(decimals=2)

        # MORE THAN ONE POSIBILITY
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_REASON", value=",", new_column_name='# bleeding reason - more than one')
        self.statsDf['% bleeding reason - more than one'] =  self.statsDf.apply(lambda x: round(((x['# bleeding reason - more than one']/(x['ich_patients'] - x['tmp'])) * 100), 2) if (x['ich_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ###################
        # BLEEDING SOURCE #
        ###################
        self.tmp = sah.groupby(['Protocol ID', 'BLEEDING_SOURCE']).size().to_frame('count').reset_index()
        self.tmp['BLEEDING_SOURCE'] = self.tmp['BLEEDING_SOURCE'].astype(str)
        # Get number of patients entered in older form
        # self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value='-999', new_column_name='tmp')
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_SOURCE", value='-999', new_column_name='tmp')
        # self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value='1', new_column_name='# bleeding source - Known')
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_SOURCE", value='1', new_column_name='# bleeding source - Known')
        self.statsDf['% bleeding source - Known'] = self.statsDf.apply(lambda x: round(((x['# bleeding source - Known']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1)
        # self.statsDf = self._get_values_for_factors(column_name="BLEEDING_SOURCE", value='2', new_column_name='# bleeding source - Not known')
        self.statsDf = self._get_values_for_factors_containing(column_name="BLEEDING_SOURCE", value='2', new_column_name='# bleeding source - Not known')
        self.statsDf['% bleeding source - Not known'] = self.statsDf.apply(lambda x: round(((x['# bleeding source - Not known']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1)
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ################
        # INTERVENTION #
        ################
        self.tmp = sah.groupby(['Protocol ID', 'INTERVENTION']).size().to_frame('count').reset_index()
        self.tmp['INTERVENTION'] = self.tmp['INTERVENTION'].astype(str)
        # Get number of patients entered in older form
        self.statsDf = self._get_values_for_factors(column_name="INTERVENTION", value=-999, new_column_name='tmp')
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="1", new_column_name='# intervention - endovascular (coiling)')
        self.statsDf['% intervention - endovascular (coiling)'] = self.statsDf.apply(lambda x: round(((x['# intervention - endovascular (coiling)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="2", new_column_name='# intervention - neurosurgical (clipping)')
        self.statsDf['% intervention - neurosurgical (clipping)'] = self.statsDf.apply(lambda x: round(((x['# intervention - neurosurgical (clipping)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="3", new_column_name='# intervention - Other neurosurgical treatment (decompression, drainage)')
        self.statsDf['% intervention - Other neurosurgical treatment (decompression, drainage)'] = self.statsDf.apply(lambda x: round(((x['# intervention - Other neurosurgical treatment (decompression, drainage)']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="4", new_column_name='# intervention - Referred to another hospital for intervention')
        self.statsDf['% intervention - Referred to another hospital for intervention'] = self.statsDf.apply(lambda x: round(((x['# intervention - Referred to another hospital for intervention']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 
        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value="5|6", new_column_name='# intervention - None / no intervention')
        self.statsDf['% intervention - None / no intervention'] = self.statsDf.apply(lambda x: round(((x['# intervention - None / no intervention']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 

        ### DATA NORMALIZATION
        norm_tmp = self.statsDf[['% intervention - endovascular (coiling)', '% intervention - neurosurgical (clipping)', '% intervention - Other neurosurgical treatment (decompression, drainage)', '% intervention - Referred to another hospital for intervention', '% intervention - None / no intervention']].copy()
        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)
        self.statsDf['intervention_endovascular_perc_norm'] = ((norm_tmp['% intervention - endovascular (coiling)']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['intervention_neurosurgical_perc_norm'] = ((norm_tmp['% intervention - neurosurgical (clipping)']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['intervention_other_perc_norm'] = ((norm_tmp['% intervention - Other neurosurgical treatment (decompression, drainage)']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['intervention_referred_perc_norm'] = ((norm_tmp['% intervention - Referred to another hospital for intervention']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['intervention_none_perc_norm'] = ((norm_tmp['% intervention - None / no intervention']/norm_tmp['rowsums']) * 100).round(decimals=2)

        self.statsDf = self._get_values_for_factors_containing(column_name="INTERVENTION", value=",", new_column_name='# intervention - more than one')
        self.statsDf['% intervention - more than one'] = self.statsDf.apply(lambda x: round(((x['# intervention - more than one']/(x['sah_patients'] - x['tmp'])) * 100), 2) if (x['sah_patients'] - x['tmp']) > 0 else 0, axis=1) 
        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ################
        # VT TREATMENT #
        ################
        if ('VT_TREATMENT' not in cvt.columns):
            cvt['VT_TREATMENT'] = np.nan
            
        self.tmp = cvt.groupby(['Protocol ID', 'VT_TREATMENT']).size().to_frame('count').reset_index()
        self.tmp[['VT_TREATMENT']] = self.tmp[['VT_TREATMENT']].astype(str)
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="1", new_column_name='# VT treatment - anticoagulation')
        self.statsDf['% VT treatment - anticoagulation'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - anticoagulation']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="2", new_column_name='# VT treatment - thrombectomy')
        self.statsDf['% VT treatment - thrombectomy'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - thrombectomy']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="3", new_column_name='# VT treatment - local thrombolysis')
        self.statsDf['% VT treatment - local thrombolysis'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - local thrombolysis']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value="4", new_column_name='# VT treatment - local neurological treatment')
        self.statsDf['% VT treatment - local neurological treatment'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - local neurological treatment']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)
        self.statsDf = self._get_values_for_factors_containing(column_name="VT_TREATMENT", value=",", new_column_name='# VT treatment - more than one treatment')
        self.statsDf['% VT treatment - more than one treatment'] = self.statsDf.apply(lambda x: round(((x['# VT treatment - more than one treatment']/x['cvt_patients']) * 100), 2) if x['cvt_patients'] > 0 else 0, axis=1)

        ### DATA NORMALIZATION
        norm_tmp = self.statsDf[['% VT treatment - anticoagulation', '% VT treatment - thrombectomy', '% VT treatment - local thrombolysis', '% VT treatment - local neurological treatment']].copy()
        norm_tmp.loc[:, 'rowsums'] = norm_tmp.sum(axis=1)
        self.statsDf['vt_treatment_anticoagulation_perc_norm'] = ((norm_tmp['% VT treatment - anticoagulation']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['vt_treatment_thrombectomy_perc_norm'] = ((norm_tmp['% VT treatment - thrombectomy']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['vt_treatment_local_thrombolysis_perc_norm'] = ((norm_tmp['% VT treatment - local thrombolysis']/norm_tmp['rowsums']) * 100).round(decimals=2)
        self.statsDf['vt_treatment_local_neurological_treatment_perc_norm'] = ((norm_tmp['% VT treatment - local neurological treatment']/norm_tmp['rowsums']) * 100).round(decimals=2)

        ########
        # AFIB #
        ########
        # tag::afib[]
        if country_code == 'CZ':
            not_reffered = is_tia.loc[~(is_tia['crf_parent_name'].isin(['F_RESQ_IVT_TBY_CZ_4']) & is_tia['RECANALIZATION_PROCEDURES'].isin([5,6,8]))].copy()
            self.statsDf['not_reffered_patients'] = self._count_patients(dataframe=not_reffered) 

            # Create dataframe with the patients referred to another hospital
            reffered = is_tia[is_tia['RECANALIZATION_PROCEDURES'].isin([5,6,8])].copy()
            self.statsDf['reffered_patients'] = self._count_patients(dataframe=reffered)

            self.tmp = not_reffered.groupby(['Protocol ID', 'AFIB_FLUTTER']).size().to_frame('count').reset_index()
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=1, new_column_name='# afib/flutter - Known')
            self.statsDf['% afib/flutter - Known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=2, new_column_name='# afib/flutter - Newly-detected at admission')
            self.statsDf['% afib/flutter - Newly-detected at admission'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Newly-detected at admission']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=3, new_column_name='# afib/flutter - Detected during hospitalization')
            self.statsDf['% afib/flutter - Detected during hospitalization'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Detected during hospitalization']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=4, new_column_name='# afib/flutter - Not detected')
            self.statsDf['% afib/flutter - Not detected'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not detected']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=5, new_column_name='# afib/flutter - Not known')
            self.statsDf['% afib/flutter - Not known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)

            self.statsDf['afib_flutter_detected_only'] = self.statsDf['# afib/flutter - Newly-detected at admission'] + self.statsDf['# afib/flutter - Detected during hospitalization']
            self.statsDf['% patients detected for aFib'] = self.statsDf.apply(lambda x: round(((x['afib_flutter_detected_only']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 

        else:
            not_reffered = is_tia[~is_tia['RECANALIZATION_PROCEDURES'].isin([7])].copy()
            self.statsDf['not_reffered_patients'] = self._count_patients(dataframe=not_reffered)

            # Create dataframe with the patients referred to another hospital
            reffered = is_tia[is_tia['RECANALIZATION_PROCEDURES'].isin([7])].copy()
            self.statsDf['reffered_patients'] = self._count_patients(dataframe=reffered)

            self.tmp = not_reffered.groupby(['Protocol ID', 'AFIB_FLUTTER']).size().to_frame('count').reset_index()
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=1, new_column_name='# afib/flutter - Known')
            self.statsDf['% afib/flutter - Known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=2, new_column_name='# afib/flutter - Newly-detected at admission')
            self.statsDf['% afib/flutter - Newly-detected at admission'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Newly-detected at admission']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=3, new_column_name='# afib/flutter - Detected during hospitalization')
            self.statsDf['% afib/flutter - Detected during hospitalization'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Detected during hospitalization']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=4, new_column_name='# afib/flutter - Not detected')
            self.statsDf['% afib/flutter - Not detected'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not detected']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_FLUTTER", value=5, new_column_name='# afib/flutter - Not known')
            self.statsDf['% afib/flutter - Not known'] = self.statsDf.apply(lambda x: round(((x['# afib/flutter - Not known']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1)

            self.statsDf['afib_flutter_detected_only'] = self.statsDf['# afib/flutter - Newly-detected at admission'] + self.statsDf['# afib/flutter - Detected during hospitalization']
            self.statsDf['% patients detected for aFib'] = self.statsDf.apply(lambda x: round(((x['afib_flutter_detected_only']/(x['is_tia_patients'] - x['reffered_patients'])) * 100), 2) if (x['is_tia_patients'] - x['reffered_patients']) > 0 else 0, axis=1) 
        # end::afib[]

        #########################
        # AFIB DETECTION METHOD #
        #########################
        if country_code == 'CZ':
            afib_detected_during_hospitalization = not_reffered[not_reffered['AFIB_FLUTTER'].isin([3])].copy()
            self.statsDf['afib_detected_during_hospitalization_patients'] = self._count_patients(dataframe=afib_detected_during_hospitalization)
            afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'] = afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'].astype(str) # Convert values to string
            self.tmp = afib_detected_during_hospitalization.groupby(['Protocol ID', 'AFIB_DETECTION_METHOD']).size().to_frame('count').reset_index()
            
            self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="1", new_column_name='# afib detection method - Telemetry with monitor allowing automatic detection of aFib')
            self.statsDf['% afib detection method - Telemetry with monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry with monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="2", new_column_name='# afib detection method - Telemetry without monitor allowing automatic detection of aFib')
            self.statsDf['% afib detection method - Telemetry without monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry without monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="3", new_column_name='# afib detection method - Holter-type monitoring')
            self.statsDf['% afib detection method - Holter-type monitoring'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Holter-type monitoring']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="4", new_column_name='# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib')
            self.statsDf['% afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors_containing(column_name="AFIB_DETECTION_METHOD", value="5", new_column_name='# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib')
            self.statsDf['% afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
        else:
            afib_detected_during_hospitalization = not_reffered[not_reffered['AFIB_FLUTTER'].isin([3])].copy()
            self.statsDf['afib_detected_during_hospitalization_patients'] = self._count_patients(dataframe=afib_detected_during_hospitalization)
            afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'] = afib_detected_during_hospitalization['AFIB_DETECTION_METHOD'].astype(str)
            self.tmp = afib_detected_during_hospitalization.groupby(['Protocol ID', 'AFIB_DETECTION_METHOD']).size().to_frame('count').reset_index()
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_DETECTION_METHOD", value=1, new_column_name='# afib detection method - Telemetry with monitor allowing automatic detection of aFib')
            self.statsDf['% afib detection method - Telemetry with monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry with monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_DETECTION_METHOD", value=2, new_column_name='# afib detection method - Telemetry without monitor allowing automatic detection of aFib')
            self.statsDf['% afib detection method - Telemetry without monitor allowing automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Telemetry without monitor allowing automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_DETECTION_METHOD", value=3, new_column_name='# afib detection method - Holter-type monitoring')
            self.statsDf['% afib detection method - Holter-type monitoring'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - Holter-type monitoring']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_DETECTION_METHOD", value=4, new_column_name='# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib')
            self.statsDf['% afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed with automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="AFIB_DETECTION_METHOD", value=5, new_column_name='# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib')
            self.statsDf['% afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib'] = self.statsDf.apply(lambda x: round(((x['# afib detection method - EKG monitoring in an ICU bed without automatic detection of aFib']/x['afib_detected_during_hospitalization_patients']) * 100), 2) if x['afib_detected_during_hospitalization_patients'] > 0 else 0, axis=1)

        ###############################
        # AFIB OTHER DETECTION METHOD #
        ###############################
        afib_not_detected_or_not_known = not_reffered[not_reffered['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_not_detected_or_not_known_patients'] = self._count_patients(dataframe=afib_not_detected_or_not_known)
        self.tmp = afib_not_detected_or_not_known.groupby(['Protocol ID', 'AFIB_OTHER_RECS']).size().to_frame('count').reset_index()
        
        self.statsDf = self._get_values_for_factors(column_name="AFIB_OTHER_RECS", value=1, new_column_name='# other afib detection method - Yes')
        self.statsDf['% other afib detection method - Yes'] = self.statsDf.apply(lambda x: round(((x['# other afib detection method - Yes']/x['afib_not_detected_or_not_known_patients']) * 100), 2) if x['afib_not_detected_or_not_known_patients'] > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="AFIB_OTHER_RECS", value=2, new_column_name='# other afib detection method - Not detected or not known')
        self.statsDf['% other afib detection method - Not detected or not known'] = self.statsDf.apply(lambda x: round(((x['# other afib detection method - Not detected or not known']/x['afib_not_detected_or_not_known_patients']) * 100), 2) if x['afib_not_detected_or_not_known_patients'] > 0 else 0, axis=1)

        
        ############################
        # CAROTID ARTERIES IMAGING #
        ############################
        if country_code == 'CZ':
            if (self.period.startswith('Q1') and self.period.endswith('2019')):
                self.statsDf.loc[:, '# carotid arteries imaging - Not known'] = 'N/A'
                self.statsDf.loc[:, '% carotid arteries imaging - Not known'] = 'N/A'
                self.statsDf.loc[:, '# carotid arteries imaging - Yes'] = 'N/A'
                self.statsDf.loc[:, '% carotid arteries imaging - Yes'] = 'N/A'
                self.statsDf.loc[:, '# carotid arteries imaging - No'] = 'N/A'
                self.statsDf.loc[:, '% carotid arteries imaging - No'] = 'N/A'
            elif ((self.period.startswith('Q2') or self.period.startswith('H1')) and self.period.endswith('2019')):
                date1 = date(2019, 7, 19)
                date2 = date(2019, 8, 31)
                obj = FilterDataset(df=self.raw_data, country='CZ', date1=date1, date2=date2)
                cz_df = obj.fdf.copy()
                if (country):
                    country_df = cz_df.copy()
                    #self.country_name = pytz.country_names[country_code]
                    # country['Protocol ID'] = self.country_name
                    #country['Site Name'] = self.country_name
                    country_df['Protocol ID'] = country_df['Country']
                    country_df['Site Name'] = country_df['Country']
                    
                    cz_df = pd.concat([cz_df, country_df])

                cz_df_is_tia = cz_df.loc[cz_df['STROKE_TYPE'].isin([1,3])].copy()
                self.statsDf['cz_df_is_tia_pts'] = self._count_patients(dataframe=cz_df_is_tia)

                self.tmp = cz_df_is_tia.groupby(['Protocol ID', 'CAROTID_ARTERIES_IMAGING']).size().to_frame('count').reset_index()
      
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=3, new_column_name='# carotid arteries imaging - Not known')
                self.statsDf['% carotid arteries imaging - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Not known']/x['cz_df_is_tia_pts']) * 100), 2) if x['cz_df_is_tia_pts'] > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=1, new_column_name='# carotid arteries imaging - Yes')
                self.statsDf['% carotid arteries imaging - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Yes']/(x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=2, new_column_name='# carotid arteries imaging - No')
                self.statsDf['% carotid arteries imaging - No'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - No']/(x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
            elif (self.period == '2019'):
                date1 = date(2019, 7, 19)
                date2 = date(2019, 12, 31)
                obj = FilterDataset(df=self.raw_data, country='CZ', date1=date1, date2=date2)
                cz_df = obj.fdf.copy()
                if (country):
                    country_df = cz_df.copy()
                    #self.country_name = pytz.country_names[country_code]
                    # country['Protocol ID'] = self.country_name
                    #country['Site Name'] = self.country_name
                    country_df['Protocol ID'] = country_df['Country']
                    country_df['Site Name'] = country_df['Country']
                    
                    cz_df = pd.concat([cz_df, country_df])

                cz_df_is_tia = cz_df.loc[cz_df['STROKE_TYPE'].isin([1,3])].copy()
                self.statsDf['cz_df_is_tia_pts'] = self._count_patients(dataframe=cz_df_is_tia)

                self.tmp = cz_df_is_tia.groupby(['Protocol ID', 'CAROTID_ARTERIES_IMAGING']).size().to_frame('count').reset_index()
      
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=3, new_column_name='# carotid arteries imaging - Not known')
                self.statsDf['% carotid arteries imaging - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Not known']/x['cz_df_is_tia_pts']) * 100), 2) if x['cz_df_is_tia_pts'] > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=1, new_column_name='# carotid arteries imaging - Yes')
                self.statsDf['% carotid arteries imaging - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Yes']/(x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=2, new_column_name='# carotid arteries imaging - No')
                self.statsDf['% carotid arteries imaging - No'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - No']/(x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['cz_df_is_tia_pts'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
            else:
                self.tmp = is_tia.groupby(['Protocol ID', 'CAROTID_ARTERIES_IMAGING']).size().to_frame('count').reset_index()
        
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=3, new_column_name='# carotid arteries imaging - Not known')
                self.statsDf['% carotid arteries imaging - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=1, new_column_name='# carotid arteries imaging - Yes')
                self.statsDf['% carotid arteries imaging - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Yes']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
                
                self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=2, new_column_name='# carotid arteries imaging - No')
                self.statsDf['% carotid arteries imaging - No'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - No']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)

            if 'cz_df_is_tia_pts' in self.statsDf.columns:
                self.statsDf.drop(['cz_df_is_tia_pts'], inplace=True, axis=1)
        else:
            self.tmp = is_tia.groupby(['Protocol ID', 'CAROTID_ARTERIES_IMAGING']).size().to_frame('count').reset_index()
        
            self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=3, new_column_name='# carotid arteries imaging - Not known')
            self.statsDf['% carotid arteries imaging - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=1, new_column_name='# carotid arteries imaging - Yes')
            self.statsDf['% carotid arteries imaging - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - Yes']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="CAROTID_ARTERIES_IMAGING", value=2, new_column_name='# carotid arteries imaging - No')
            self.statsDf['% carotid arteries imaging - No'] = self.statsDf.apply(lambda x: round(((x['# carotid arteries imaging - No']/(x['is_tia_patients'] - x['# carotid arteries imaging - Not known'])) * 100), 2) if (x['is_tia_patients'] - x['# carotid arteries imaging - Not known']) > 0 else 0, axis=1)

        ############################
        # ANTITHROMBOTICS WITH CVT #
        ############################
        # Create dataframe with dead patients excluded
        antithrombotics_with_cvt = is_tia_cvt[~is_tia_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['antithrombotics_patients_with_cvt'] = self._count_patients(dataframe=antithrombotics_with_cvt)
        
        ischemic_transient_cerebral_dead = is_tia_cvt[is_tia_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['ischemic_transient_cerebral_dead_patients'] = self._count_patients(dataframe=ischemic_transient_cerebral_dead)
        self.tmp = antithrombotics_with_cvt.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients receiving antiplatelets with CVT')
        self.statsDf['% patients receiving antiplatelets with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving antiplatelets with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist with CVT')
        self.statsDf['% patients receiving Vit. K antagonist with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran with CVT')
        self.statsDf['% patients receiving dabigatran with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban with CVT')
        self.statsDf['% patients receiving rivaroxaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban with CVT')
        self.statsDf['% patients receiving apixaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban with CVT')
        self.statsDf['% patients receiving edoxaban with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose with CVT')
        self.statsDf['% patients receiving LMWH or heparin in prophylactic dose with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose with CVT')
        self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=9, new_column_name='# patients not prescribed antithrombotics, but recommended with CVT')
        self.statsDf['% patients not prescribed antithrombotics, but recommended with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients not prescribed antithrombotics, but recommended with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=10, new_column_name='# patients neither receiving antithrombotics nor recommended with CVT')
        self.statsDf['% patients neither receiving antithrombotics nor recommended with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients neither receiving antithrombotics nor recommended with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        ## ANTITHROMBOTICS - PATIENTS PRESCRIBED + RECOMMENDED
        self.statsDf.loc[:, '# patients prescribed antithrombotics with CVT'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets with CVT'] + x['# patients receiving Vit. K antagonist with CVT'] + x['# patients receiving dabigatran with CVT'] + x['# patients receiving rivaroxaban with CVT'] + x['# patients receiving apixaban with CVT'] + x['# patients receiving edoxaban with CVT'] + x['# patients receiving LMWH or heparin in prophylactic dose with CVT'] + x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT'], axis=1)

        # self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0 else 0, axis=1)
        self.statsDf['% patients prescribed antithrombotics with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with CVT']/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients']) > 0 else 0, axis=1)

        self.statsDf.loc[:, '# patients prescribed or recommended antithrombotics with CVT'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets with CVT'] + x['# patients receiving Vit. K antagonist with CVT'] + x['# patients receiving dabigatran with CVT'] + x['# patients receiving rivaroxaban with CVT'] + x['# patients receiving apixaban with CVT'] + x['# patients receiving edoxaban with CVT'] + x['# patients receiving LMWH or heparin in prophylactic dose with CVT'] + x['# patients receiving LMWH or heparin in full anticoagulant dose with CVT'] + x['# patients not prescribed antithrombotics, but recommended with CVT'], axis=1)

        self.statsDf['% patients prescribed or recommended antithrombotics with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics with CVT'] - x['ischemic_transient_cerebral_dead_patients'])/(x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended with CVT'])) * 100, 2) if ((x['is_tia_cvt_patients'] - x['ischemic_transient_cerebral_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended with CVT']) > 0) else 0, axis=1)

        self.statsDf.fillna(0, inplace=True)

        ###########################################
        # ANTIPLATELETS - PRESCRIBED WITHOUT AFIB #
        ###########################################
        afib_flutter_not_detected_or_not_known_with_cvt = is_tia_cvt[is_tia_cvt['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_with_cvt)

        afib_flutter_not_detected_or_not_known_with_cvt_dead = afib_flutter_not_detected_or_not_known_with_cvt[afib_flutter_not_detected_or_not_known_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_with_cvt_dead)

        prescribed_antiplatelets_no_afib_with_cvt = afib_flutter_not_detected_or_not_known_with_cvt[afib_flutter_not_detected_or_not_known_with_cvt['ANTITHROMBOTICS'].isin([1])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_patients_with_cvt'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_with_cvt)

        prescribed_antiplatelets_no_afib_dead_with_cvt = prescribed_antiplatelets_no_afib_with_cvt[prescribed_antiplatelets_no_afib_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_dead_patients_with_cvt'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_dead_with_cvt)

        self.tmp = afib_flutter_not_detected_or_not_known_with_cvt.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed antiplatelets without aFib with CVT')
        self.statsDf['% patients prescribed antiplatelets without aFib with CVT'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed antiplatelets without aFib with CVT'] - x['prescribed_antiplatelets_no_afib_dead_patients_with_cvt'])/(x['afib_flutter_not_detected_or_not_known_patients_with_cvt'] - x['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt'])) * 100, 2) if ((x['afib_flutter_not_detected_or_not_known_patients_with_cvt'] - x['afib_flutter_not_detected_or_not_known_dead_patients_with_cvt']) > 0) else 0, axis=1)

        #########################################
        # ANTICOAGULANTS - PRESCRIBED WITH AFIB #
        #########################################       
        afib_flutter_detected_with_cvt = is_tia_cvt[is_tia_cvt['AFIB_FLUTTER'].isin([1, 2, 3])].copy()
        self.statsDf['afib_flutter_detected_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_detected_with_cvt)

        anticoagulants_prescribed_with_cvt = afib_flutter_detected_with_cvt[~afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([1, 10, 9]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed anticoagulants with aFib with CVT'] = self._count_patients(dataframe=anticoagulants_prescribed_with_cvt)
        
        anticoagulants_recommended_with_cvt = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9])].copy()
        self.statsDf['anticoagulants_recommended_patients_with_cvt'] = self._count_patients(dataframe=anticoagulants_recommended_with_cvt)

        afib_flutter_detected_dead_with = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_dead_patients_with_cvt'] = self._count_patients(dataframe=afib_flutter_detected_dead_with)

        self.statsDf['% patients prescribed anticoagulants with aFib with CVT'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed anticoagulants with aFib with CVT']/(x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'])) * 100), 2) if (x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt']) > 0 else 0, axis=1)

        ##########################################
        # ANTITHROMBOTICS - PRESCRIBED WITH AFIB #
        ##########################################
        antithrombotics_prescribed_with_cvt = afib_flutter_detected_with_cvt[~afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9, 10]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed antithrombotics with aFib with CVT'] = self._count_patients(dataframe=antithrombotics_prescribed_with_cvt)

        recommended_antithrombotics_with_afib_alive_with_cvt = afib_flutter_detected_with_cvt[afib_flutter_detected_with_cvt['ANTITHROMBOTICS'].isin([9]) & ~afib_flutter_detected_with_cvt['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['recommended_antithrombotics_with_afib_alive_patients_with_cvt'] = self._count_patients(dataframe=recommended_antithrombotics_with_afib_alive_with_cvt)

        self.statsDf['% patients prescribed antithrombotics with aFib with CVT'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with aFib with CVT']/(x['afib_flutter_detected_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'] - x['recommended_antithrombotics_with_afib_alive_patients_with_cvt'])) * 100), 2) if (x['afib_flutter_detected_dead_patients_with_cvt'] - x['afib_flutter_detected_dead_patients_with_cvt'] - x['recommended_antithrombotics_with_afib_alive_patients_with_cvt']) > 0 else 0, axis=1)
        
        
        ###############################
        # ANTITHROMBOTICS WITHOUT CVT #
        ###############################
        antithrombotics = is_tia[~is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['antithrombotics_patients'] = self._count_patients(dataframe=antithrombotics)

        ischemic_transient_dead = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['ischemic_transient_dead_patients'] = self._count_patients(dataframe=ischemic_transient_dead)

        ischemic_transient_dead_prescribed = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([5]) & ~is_tia['ANTITHROMBOTICS'].isin([10])].copy()
        self.statsDf['ischemic_transient_dead_patients_prescribed'] = self._count_patients(dataframe=ischemic_transient_dead_prescribed)
        
        self.tmp = antithrombotics.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients receiving antiplatelets')
        self.statsDf['% patients receiving antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# patients receiving antiplatelets']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist')
        # self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran')
        # self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban')
        # self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban')
        # self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban')
        # self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')
        # self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')
        # self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=9, new_column_name='# patients not prescribed antithrombotics, but recommended')
        self.statsDf['% patients not prescribed antithrombotics, but recommended'] = self.statsDf.apply(lambda x: round(((x['# patients not prescribed antithrombotics, but recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=10, new_column_name='# patients neither receiving antithrombotics nor recommended')
        self.statsDf['% patients neither receiving antithrombotics nor recommended'] = self.statsDf.apply(lambda x: round(((x['# patients neither receiving antithrombotics nor recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        ## ANTITHROMBOTICS - PATIENTS PRESCRIBED + RECOMMENDED
        self.statsDf.loc[:, '# patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets'] + x['# patients receiving Vit. K antagonist'] + x['# patients receiving dabigatran'] + x['# patients receiving rivaroxaban'] + x['# patients receiving apixaban'] + x['# patients receiving edoxaban'] + x['# patients receiving LMWH or heparin in prophylactic dose'] + x['# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1)

        # self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0 else 0, axis=1)
        self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.statsDf.loc[:, '# patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets'] + x['# patients receiving Vit. K antagonist'] + x['# patients receiving dabigatran'] + x['# patients receiving rivaroxaban'] + x['# patients receiving apixaban'] + x['# patients receiving edoxaban'] + x['# patients receiving LMWH or heparin in prophylactic dose'] + x['# patients receiving LMWH or heparin in full anticoagulant dose'] + x['# patients not prescribed antithrombotics, but recommended'], axis=1)

        # From patients prescribed or recommended antithrombotics remove patient who had prescribed antithrombotics and were dead (nominator)
        # self.statsDf['% patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics'] - x['ischemic_transient_dead_patients_prescribed'])/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100, 2) if ((x['is_tia_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0) else 0, axis=1)
        self.statsDf['% patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics'] - x['ischemic_transient_dead_patients_prescribed'])/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100, 2) if ((x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0) else 0, axis=1)
        
        # Drop the redundant columns
        self.statsDf.drop(['# patients receiving Vit. K antagonist', '# patients receiving dabigatran', '# patients receiving rivaroxaban', '# patients receiving apixaban', '# patients receiving edoxaban', '# patients receiving LMWH or heparin in prophylactic dose','# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1, inplace=True)

        self.statsDf.fillna(0, inplace=True)

        ###########################################
        # ANTIPLATELETS - PRESCRIBED WITHOUT AFIB #
        ###########################################
        afib_flutter_not_detected_or_not_known = is_tia[is_tia['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known)

        afib_flutter_not_detected_or_not_known_dead = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_dead_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_dead)

        prescribed_antiplatelets_no_afib = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['ANTITHROMBOTICS'].isin([1])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib)

        prescribed_antiplatelets_no_afib_dead = prescribed_antiplatelets_no_afib[prescribed_antiplatelets_no_afib['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_dead_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_dead)

        self.tmp = afib_flutter_not_detected_or_not_known.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed antiplatelets without aFib')
        self.statsDf['% patients prescribed antiplatelets without aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed antiplatelets without aFib'] - x['prescribed_antiplatelets_no_afib_dead_patients'])/(x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients'])) * 100, 2) if ((x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients']) > 0) else 0, axis=1)

        #########################################
        # ANTICOAGULANTS - PRESCRIBED WITH AFIB #
        #########################################
        afib_flutter_detected = is_tia[is_tia['AFIB_FLUTTER'].isin([1, 2, 3])].copy()
        self.statsDf['afib_flutter_detected_patients'] = self._count_patients(dataframe=afib_flutter_detected)

        afib_flutter_detected_not_dead = afib_flutter_detected[~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_patients_not_dead'] = self._count_patients(dataframe=afib_flutter_detected_not_dead)

        anticoagulants_prescribed = afib_flutter_detected[~afib_flutter_detected['ANTITHROMBOTICS'].isin([1, 10, 9]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed anticoagulants with aFib'] = self._count_patients(dataframe=anticoagulants_prescribed)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
        
        # Additional calculation 
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=2, new_column_name='# patients receiving Vit. K antagonist')
        # self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['# patients prescribed anticoagulants with aFib']) * 100), 2) if x['# patients prescribed anticoagulants with aFib'] > 0 else 0, axis=1)
        self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=3, new_column_name='# patients receiving dabigatran')
        self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=4, new_column_name='# patients receiving rivaroxaban')
        self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=5, new_column_name='# patients receiving apixaban')
        self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=6, new_column_name='# patients receiving edoxaban')
        self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=7, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')
        self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=8, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')
        self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)
        
        anticoagulants_recommended = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9])].copy()
        self.statsDf['anticoagulants_recommended_patients'] = self._count_patients(dataframe=anticoagulants_recommended)

        afib_flutter_detected_dead = afib_flutter_detected[afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_dead_patients'] = self._count_patients(dataframe=afib_flutter_detected_dead)

        self.statsDf['% patients prescribed anticoagulants with aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed anticoagulants with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)

        ##########################################
        # ANTITHROMBOTICS - PRESCRIBED WITH AFIB #
        ##########################################
        antithrombotics_prescribed = afib_flutter_detected[~afib_flutter_detected['ANTITHROMBOTICS'].isin([9, 10]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed antithrombotics with aFib'] = self._count_patients(dataframe=antithrombotics_prescribed)

        recommended_antithrombotics_with_afib_alive = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['recommended_antithrombotics_with_afib_alive_patients'] = self._count_patients(dataframe=recommended_antithrombotics_with_afib_alive)

        self.statsDf['% patients prescribed antithrombotics with aFib'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients']) > 0 else 0, axis=1)
    

        ###########
        # STATINS #
        ###########
        # For CZ only patients discharged home included
        if country_code == 'CZ':
            is_tia_discharged_home = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([1])].copy()
            self.statsDf['is_tia_discharged_home_patients'] = self._count_patients(dataframe=is_tia_discharged_home)
            
            self.tmp = is_tia_discharged_home.groupby(['Protocol ID', 'STATIN']).size().to_frame('count').reset_index()
            
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=1, new_column_name='# patients prescribed statins - Yes')
            self.statsDf['% patients prescribed statins - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Yes']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=2, new_column_name='# patients prescribed statins - No')
            self.statsDf['% patients prescribed statins - No'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - No']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=3, new_column_name='# patients prescribed statins - Not known')
            self.statsDf['% patients prescribed statins - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Not known']/x['is_tia_discharged_home_patients']) * 100), 2) if x['is_tia_discharged_home_patients'] > 0 else 0, axis=1)
        else:
            self.tmp = is_tia.groupby(['Protocol ID', 'STATIN']).size().to_frame('count').reset_index()
           
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=1, new_column_name='# patients prescribed statins - Yes')
            self.statsDf['% patients prescribed statins - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Yes']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=2, new_column_name='# patients prescribed statins - No')
            self.statsDf['% patients prescribed statins - No'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="STATIN", value=3, new_column_name='# patients prescribed statins - Not known')
            self.statsDf['% patients prescribed statins - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        ####################
        # CAROTID STENOSIS #
        ####################
        self.tmp = is_tia.groupby(['Protocol ID', 'CAROTID_STENOSIS']).size().to_frame('count').reset_index()
        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=1, new_column_name='# carotid stenosis - 50%-70%')
        self.statsDf['% carotid stenosis - 50%-70%'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - 50%-70%']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=2, new_column_name='# carotid stenosis - >70%')
        self.statsDf['% carotid stenosis - >70%'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - >70%']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=3, new_column_name='# carotid stenosis - No')
        self.statsDf['% carotid stenosis - No'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS", value=4, new_column_name='# carotid stenosis - Not known')
        self.statsDf['% carotid stenosis - Not known'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        ##############################
        # CAROTID STENOSIS FOLLOW-UP #
        ##############################
        # Create temporary dataframe if carotid stenosis was 50-70% or > 70%
        carotid_stenosis = is_tia[is_tia['CAROTID_STENOSIS'].isin([1, 2])] 

        self.tmp = carotid_stenosis.groupby(['Protocol ID', 'CAROTID_STENOSIS_FOLLOWUP']).size().to_frame('count').reset_index()

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=1, new_column_name='# carotid stenosis followup - Yes')
        self.statsDf['% carotid stenosis followup - Yes'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Yes']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=2, new_column_name='# carotid stenosis followup - No')
        self.statsDf['% carotid stenosis followup - No'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=3, new_column_name='# carotid stenosis followup - No, but planned later')
        self.statsDf['% carotid stenosis followup - No, but planned later'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - No, but planned later']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Create temporary dataframe if carotid stenosis was followed up or planned to follow up later
        carotid_stenosis_followup = carotid_stenosis[carotid_stenosis['CAROTID_STENOSIS_FOLLOWUP'].isin([1, 3])].copy()

        self.statsDf['# carotid stenosis followup - Yes, but planned'] = self._count_patients(dataframe=carotid_stenosis_followup)
        self.statsDf['% carotid stenosis followup - Yes, but planned'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Yes, but planned']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="CAROTID_STENOSIS_FOLLOWUP", value=4, new_column_name='# carotid stenosis followup - Referred to another centre')
        self.statsDf['% carotid stenosis followup - Referred to another centre'] = self.statsDf.apply(lambda x: round(((x['# carotid stenosis followup - Referred to another centre']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        #####################
        # ANTIHYPERTENSIVES #
        #####################
        # tag::antihypertensive[]
        if country_code == 'CZ':
            # filter patients with recanaliztion procedure 8 and form CZ_4 (antihypertensive not shown in the new version)
            discharge_subset_alive_not_returned_back = discharge_subset_alive.loc[~(discharge_subset_alive['crf_parent_name'].isin(['F_RESQ_IVT_TBY_CZ_4']) & discharge_subset_alive['RECANALIZATION_PROCEDURES'].isin([5,6,8]))].copy()
            self.statsDf['discharge_subset_alive_not_returned_back_patients'] = self._count_patients(dataframe=discharge_subset_alive_not_returned_back) 

            self.tmp = discharge_subset_alive_not_returned_back.groupby(['Protocol ID', 'ANTIHYPERTENSIVE']).size().to_frame('count').reset_index()

            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=3, new_column_name='# prescribed antihypertensives - Not known')
            self.statsDf['% prescribed antihypertensives - Not known'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Not known']/x['discharge_subset_alive_not_returned_back_patients']) * 100), 2) if x['discharge_subset_alive_not_returned_back_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=1, new_column_name='# prescribed antihypertensives - Yes')
            self.statsDf['% prescribed antihypertensives - Yes'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Yes']/(x['discharge_subset_alive_not_returned_back_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=2, new_column_name='# prescribed antihypertensives - No')
            self.statsDf['% prescribed antihypertensives - No'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - No']/(x['discharge_subset_alive_not_returned_back_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)

        else:
            self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'ANTIHYPERTENSIVE']).size().to_frame('count').reset_index()

            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=3, new_column_name='# prescribed antihypertensives - Not known')
            self.statsDf['% prescribed antihypertensives - Not known'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Not known']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=1, new_column_name='# prescribed antihypertensives - Yes')
            self.statsDf['% prescribed antihypertensives - Yes'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - Yes']/(x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)
            
            self.statsDf = self._get_values_for_factors(column_name="ANTIHYPERTENSIVE", value=2, new_column_name='# prescribed antihypertensives - No')
            self.statsDf['% prescribed antihypertensives - No'] = self.statsDf.apply(lambda x: round(((x['# prescribed antihypertensives - No']/(x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['# prescribed antihypertensives - Not known']) > 0 else 0, axis=1)
        # end::antihypertensive[]


        #####################
        # SMOKING CESSATION #
        #####################
        # tag::smoking[]
        if country_code == 'CZ':
            print('Im here')
            self.tmp = discharge_subset_alive_not_returned_back.groupby(['Protocol ID', 'SMOKING_CESSATION']).size().to_frame('count').reset_index()

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=3, new_column_name='# recommended to a smoking cessation program - not a smoker')
            self.statsDf['% recommended to a smoking cessation program - not a smoker'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - not a smoker']/x['discharge_subset_alive_not_returned_back_patients']) * 100), 2) if x['discharge_subset_alive_not_returned_back_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=1, new_column_name='# recommended to a smoking cessation program - Yes')
            self.statsDf['% recommended to a smoking cessation program - Yes'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - Yes']/x['discharge_subset_alive_not_returned_back_patients']) * 100), 2) if x['discharge_subset_alive_not_returned_back_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=2, new_column_name='# recommended to a smoking cessation program - No')
            self.statsDf['% recommended to a smoking cessation program - No'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - No']/x['discharge_subset_alive_not_returned_back_patients']) * 100), 2) if x['discharge_subset_alive_not_returned_back_patients'] > 0 else 0, axis=1)

        else:
            self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'SMOKING_CESSATION']).size().to_frame('count').reset_index()

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=3, new_column_name='# recommended to a smoking cessation program - not a smoker')
            self.statsDf['% recommended to a smoking cessation program - not a smoker'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - not a smoker']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=1, new_column_name='# recommended to a smoking cessation program - Yes')
            self.statsDf['% recommended to a smoking cessation program - Yes'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - Yes']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="SMOKING_CESSATION", value=2, new_column_name='# recommended to a smoking cessation program - No')
            self.statsDf['% recommended to a smoking cessation program - No'] = self.statsDf.apply(lambda x: round(((x['# recommended to a smoking cessation program - No']/x['discharge_subset_alive_patients']) * 100), 2) if x['discharge_subset_alive_patients'] > 0 else 0, axis=1)
        # end::smoking[]


        ##########################
        # CEREBROVASCULAR EXPERT #
        ##########################
        # tag::cerebrovascular_expert[]
        if country_code == 'CZ':
            self.tmp = discharge_subset_alive_not_returned_back.groupby(['Protocol ID', 'CEREBROVASCULAR_EXPERT']).size().to_frame('count').reset_index()

            # Claculate number of patients entered to the old form
            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=-999, new_column_name='tmp')

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=1, new_column_name='# recommended to a cerebrovascular expert - Recommended, and appointment was made')
            self.statsDf['% recommended to a cerebrovascular expert - Recommended, and appointment was made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, and appointment was made']/(x['discharge_subset_alive_not_returned_back_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=2, new_column_name='# recommended to a cerebrovascular expert - Recommended, but appointment was not made')
            self.statsDf['% recommended to a cerebrovascular expert - Recommended, but appointment was not made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made']/(x['discharge_subset_alive_not_returned_back_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf.loc[:, '# recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: x['# recommended to a cerebrovascular expert - Recommended, and appointment was made'] + x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made'], axis=1)
            self.statsDf['% recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended']/(x['discharge_subset_alive_not_returned_back_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=3, new_column_name='# recommended to a cerebrovascular expert - Not recommended')
            self.statsDf['% recommended to a cerebrovascular expert - Not recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Not recommended']/(x['discharge_subset_alive_not_returned_back_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_not_returned_back_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf.drop(['tmp'], inplace=True, axis=1)

        else:
            self.tmp = discharge_subset_alive.groupby(['Protocol ID', 'CEREBROVASCULAR_EXPERT']).size().to_frame('count').reset_index()

            # Claculate number of patients entered to the old form
            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=-999, new_column_name='tmp')

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=1, new_column_name='# recommended to a cerebrovascular expert - Recommended, and appointment was made')
            self.statsDf['% recommended to a cerebrovascular expert - Recommended, and appointment was made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, and appointment was made']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=2, new_column_name='# recommended to a cerebrovascular expert - Recommended, but appointment was not made')
            self.statsDf['% recommended to a cerebrovascular expert - Recommended, but appointment was not made'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf.loc[:, '# recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: x['# recommended to a cerebrovascular expert - Recommended, and appointment was made'] + x['# recommended to a cerebrovascular expert - Recommended, but appointment was not made'], axis=1)
            self.statsDf['% recommended to a cerebrovascular expert - Recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Recommended']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf = self._get_values_for_factors(column_name="CEREBROVASCULAR_EXPERT", value=3, new_column_name='# recommended to a cerebrovascular expert - Not recommended')
            self.statsDf['% recommended to a cerebrovascular expert - Not recommended'] = self.statsDf.apply(lambda x: round(((x['# recommended to a cerebrovascular expert - Not recommended']/(x['discharge_subset_alive_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_alive_patients'] - x['tmp']) > 0 else 0, axis=1)

            self.statsDf.drop(['tmp'], inplace=True, axis=1)
        # end::cerebrovascular_expert[]
        
        #########################
        # DISCHARGE DESTINATION #
        #########################
        self.tmp = discharge_subset.groupby(['Protocol ID', 'DISCHARGE_DESTINATION']).size().to_frame('count').reset_index()

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=1, new_column_name='# discharge destination - Home')
        self.statsDf['% discharge destination - Home'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Home']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=2, new_column_name='# discharge destination - Transferred within the same centre')
        self.statsDf['% discharge destination - Transferred within the same centre'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Transferred within the same centre']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=3, new_column_name='# discharge destination - Transferred to another centre')
        self.statsDf['% discharge destination - Transferred to another centre'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Transferred to another centre']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=4, new_column_name='# discharge destination - Social care facility')
        self.statsDf['% discharge destination - Social care facility'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Social care facility']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_DESTINATION", value=5, new_column_name='# discharge destination - Dead')
        self.statsDf['% discharge destination - Dead'] = self.statsDf.apply(lambda x: round(((x['# discharge destination - Dead']/x['discharge_subset_patients']) * 100), 2) if x['discharge_subset_patients'] > 0 else 0, axis=1)

        #######################################
        # DISCHARGE DESTINATION - SAME CENTRE #
        #######################################
        discharge_subset_same_centre = discharge_subset[discharge_subset['DISCHARGE_DESTINATION'].isin([2])].copy()
        self.statsDf['discharge_subset_same_centre_patients'] = self._count_patients(dataframe=discharge_subset_same_centre)

        self.tmp = discharge_subset_same_centre.groupby(['Protocol ID', 'DISCHARGE_SAME_FACILITY']).size().to_frame('count').reset_index()

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=1, new_column_name='# transferred within the same centre - Acute rehabilitation')
        self.statsDf['% transferred within the same centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Acute rehabilitation']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=2, new_column_name='# transferred within the same centre - Post-care bed')
        self.statsDf['% transferred within the same centre - Post-care bed'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Post-care bed']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_SAME_FACILITY", value=3, new_column_name='# transferred within the same centre - Another department')

        self.statsDf['% transferred within the same centre - Another department'] = self.statsDf.apply(lambda x: round(((x['# transferred within the same centre - Another department']/x['discharge_subset_same_centre_patients']) * 100), 2) if x['discharge_subset_same_centre_patients'] > 0 else 0, axis=1)

        ############################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY #
        ############################################
        discharge_subset_another_centre = discharge_subset[discharge_subset['DISCHARGE_DESTINATION'].isin([3])].copy()
        self.statsDf['discharge_subset_another_centre_patients'] = self._count_patients(dataframe=discharge_subset_another_centre)

        self.tmp = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY']).size().to_frame('count').reset_index()

        # Calculate number of patients entered to the old form
        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=-999, new_column_name='tmp')

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=1, new_column_name='# transferred to another centre - Stroke centre')
        self.statsDf['% transferred to another centre - Stroke centre'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Stroke centre']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=2, new_column_name='# transferred to another centre - Comprehensive stroke centre')
        self.statsDf['% transferred to another centre - Comprehensive stroke centre'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Comprehensive stroke centre']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf = self._get_values_for_factors(column_name="DISCHARGE_OTHER_FACILITY", value=3, new_column_name='# transferred to another centre - Another hospital')
        self.statsDf['% transferred to another centre - Another hospital'] = self.statsDf.apply(lambda x: round(((x['# transferred to another centre - Another hospital']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        #########################################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY - DEPARTMENT #
        #########################################################
        self.tmp = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O1']).size().to_frame('count').reset_index()
        tmp_o2 = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O2']).size().to_frame('count').reset_index()
        tmp_o3 = discharge_subset_another_centre.groupby(['Protocol ID', 'DISCHARGE_OTHER_FACILITY_O3']).size().to_frame('count').reset_index()

        # Calculate number of patients entered to the old form
        self.statsDf.loc[:, 'tmp'] = 0

        self.statsDf['# department transferred to within another centre - Acute rehabilitation'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=1, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=1, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=1, dataframe=tmp_o3)
        self.statsDf['% department transferred to within another centre - Acute rehabilitation'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Acute rehabilitation']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf['# department transferred to within another centre - Post-care bed'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=2, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=2, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=2, dataframe=tmp_o3)
        self.statsDf['% department transferred to within another centre - Post-care bed'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Post-care bed']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf['# department transferred to within another centre - Neurology'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=3, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=3, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=3, dataframe=tmp_o3)
        self.statsDf['% department transferred to within another centre - Neurology'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Neurology']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf['# department transferred to within another centre - Another department'] = self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O1", value=4, dataframe=self.tmp) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O2", value=4, dataframe=tmp_o2) + self._get_values_only_columns(column_name="DISCHARGE_OTHER_FACILITY_O3", value=4, dataframe=tmp_o3)
        self.statsDf['% department transferred to within another centre - Another department'] = self.statsDf.apply(lambda x: round(((x['# department transferred to within another centre - Another department']/(x['discharge_subset_another_centre_patients'] - x['tmp'])) * 100), 2) if (x['discharge_subset_another_centre_patients'] - x['tmp']) > 0 else 0, axis=1)

        self.statsDf.drop(['tmp'], inplace=True, axis=1)

        ############################################
        # DISCHARGE DESTINATION - ANOTHER FACILITY #
        ############################################
        discharge_subset.fillna(0, inplace=True)
        discharge_subset_mrs = discharge_subset[~discharge_subset['DISCHARGE_MRS'].isin([0])].copy()
        #discharge_subset_mrs['DISCHARGE_MRS'] = discharge_subset_mrs['DISCHARGE_MRS'].astype(float)

        def convert_mrs_on_discharge(x):
            """ The function calculating mRS on discharge. Options: 1 (unknown/derivate), 2 = 0, 3 = 1, 4 = 2, 5 = 3, 6 = 4, 7 = 5, 8 = 6.

            :param x: the mRS value from the dropdown
            :type x: int
            :returns: x -- value converted to score
            """
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
            discharge_subset_mrs['DISCHARGE_MRS_ADJUSTED'] = discharge_subset_mrs.apply(lambda row: convert_mrs_on_discharge(row['DISCHARGE_MRS']), axis=1)
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

        ###########################
        # MEDIAN LAST SEEN NORMAL #
        ###########################
        self.statsDf = self.statsDf.merge(self.df[self.df['LAST_SEEN_NORMAL'] != 0].groupby(['Protocol ID']).LAST_SEEN_NORMAL.agg(['median']).rename(columns={'median': 'Median last seen normal'})['Median last seen normal'].reset_index(), how='outer')
        self.statsDf.fillna(0, inplace=True)

        # ELIGIBLE RECANALIZATION

        wrong_ivtpa = recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] <= 0]

        self.statsDf['wrong_ivtpa'] = self._count_patients(dataframe=wrong_ivtpa)

        self.statsDf.loc[:, '# patients eligible thrombolysis'] = self.statsDf.apply(lambda x: (x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment']) - x['wrong_ivtpa'], axis=1)
        self.statsDf.drop(['wrong_ivtpa'], inplace=True, axis=1)

        wrong_tby = recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['TBY'] <= 0]

        self.statsDf['wrong_tby'] = self._count_patients(dataframe=wrong_tby)

        if country_code == 'CZ':
            self.statsDf.loc[:, '# patients eligible thrombectomy'] = self.statsDf.apply(lambda x: (x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] + x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre']) - x['wrong_tby'], axis=1)
            self.statsDf.drop(['wrong_tby'], inplace=True, axis=1)
        else:
            self.statsDf.loc[:, '# patients eligible thrombectomy'] = self.statsDf.apply(lambda x: (x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone']) - x['wrong_tby'], axis=1)
            self.statsDf.drop(['wrong_tby'], inplace=True, axis=1)

        self.statsDf.loc[:, 'patients_eligible_recanalization'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Not done'] + x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)


        ################
        # ANGEL AWARDS #
        ################
        self.total_patient_column = '# total patients >= {0}'.format(self.patient_limit)
        self.statsDf[self.total_patient_column] = self.statsDf['Total Patients'] >= self.patient_limit

        ## Calculate classic recanalization procedure
        recanalization_procedure_tby_only_dtg =  recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['RECANALIZATION_PROCEDURES'].isin([4])]

        # Create temporary dataframe only with rows where thrombolysis was performed under 60 minute
        recanalization_procedure_iv_tpa_under_60 = recanalization_procedure_iv_tpa.loc[(recanalization_procedure_iv_tpa['IVTPA'] > 0) & (recanalization_procedure_iv_tpa['IVTPA'] <= 60)]
        # Create temporary dataframe only with rows where thrombolysis was performed under 45 minute
        recanalization_procedure_iv_tpa_under_45 = recanalization_procedure_iv_tpa.loc[(recanalization_procedure_iv_tpa['IVTPA'] > 0) & (recanalization_procedure_iv_tpa['IVTPA'] <= 45)]

        recanalization_procedure_tby_only_dtg_under_60 = recanalization_procedure_tby_only_dtg.loc[(recanalization_procedure_tby_only_dtg['TBY'] > 0) & (recanalization_procedure_tby_only_dtg['TBY'] <= 60)]
        self.statsDf['# patients treated with door to recanalization therapy < 60 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_60) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_60)
        self.statsDf['% patients treated with door to recanalization therapy < 60 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 60 minutes']/x['# patients recanalized']) * 100), 2) if x['# patients recanalized'] > 0 else 0, axis=1)

        recanalization_procedure_tby_only_dtg_under_45 = recanalization_procedure_tby_only_dtg.loc[(recanalization_procedure_tby_only_dtg['TBY'] > 0) & (recanalization_procedure_tby_only_dtg['TBY'] <= 45)]
        self.statsDf['# patients treated with door to recanalization therapy < 45 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_45) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_45)
        self.statsDf['% patients treated with door to recanalization therapy < 45 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 45 minutes']/x['# patients recanalized']) * 100), 2) if x['# patients recanalized'] > 0 else 0, axis=1)


        #### DOOR TO THROMBOLYSIS THERAPY - MINUTES ####
        # If thrombectomy done not at all, take the possible lowest award they can get

        

        self.statsDf['# patients treated with door to thrombolysis < 60 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_60)
        self.statsDf['% patients treated with door to thrombolysis < 60 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to thrombolysis < 60 minutes']/x['# patients eligible thrombolysis']) * 100), 2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)

        self.statsDf['# patients treated with door to thrombolysis < 45 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_45)
        self.statsDf['% patients treated with door to thrombolysis < 45 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to thrombolysis < 45 minutes']/x['# patients eligible thrombolysis']) * 100), 2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)

        # Create temporary dataframe only with rows where trombectomy was performed under 90 minutes
        recanalization_procedure_tby_only_dtg_under_120 = recanalization_procedure_tby_dtg.loc[(recanalization_procedure_tby_dtg['TBY'] > 0) & (recanalization_procedure_tby_dtg['TBY'] <= 120)]
        # Create temporary dataframe only with rows where trombectomy was performed under 60 minutes
        recanalization_procedure_tby_only_dtg_under_90 = recanalization_procedure_tby_dtg.loc[(recanalization_procedure_tby_dtg['TBY'] > 0) & (recanalization_procedure_tby_dtg['TBY'] <= 90)]
        
        self.statsDf['# patients treated with door to thrombectomy < 120 minutes'] = self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_120)
        self.statsDf['% patients treated with door to thrombectomy < 120 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to thrombectomy < 120 minutes']/x['# patients eligible thrombectomy']) * 100), 2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)

        self.statsDf['# patients treated with door to thrombectomy < 90 minutes'] = self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_90)
        self.statsDf['% patients treated with door to thrombectomy < 90 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to thrombectomy < 90 minutes']/x['# patients eligible thrombectomy']) * 100), 2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)

        #### RECANALIZATION RATE ####
        self.statsDf['# recanalization rate out of total ischemic incidence'] = self.statsDf['# patients recanalized']
        self.statsDf['% recanalization rate out of total ischemic incidence'] = self.statsDf['% patients recanalized']

        #### CT/MRI ####
        self.statsDf['# suspected stroke patients undergoing CT/MRI'] = self.statsDf['# CT/MRI - performed']
        self.statsDf['% suspected stroke patients undergoing CT/MRI'] = self.statsDf['% CT/MRI - performed']

        #### DYSPHAGIA SCREENING ####
        self.statsDf['# all stroke patients undergoing dysphagia screening'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test']
        self.statsDf['% all stroke patients undergoing dysphagia screening'] = self.statsDf.apply(lambda x: round(((x['# all stroke patients undergoing dysphagia screening']/(x['# all stroke patients undergoing dysphagia screening'] + x['# dysphagia screening - Not done'])) * 100), 2) if (x['# all stroke patients undergoing dysphagia screening'] + x['# dysphagia screening - Not done']) > 0 else 0, axis=1)

        #### ISCHEMIC STROKE + NO AFIB + ANTIPLATELETS ####
        # Exclude patients referred for recanalization procedure
        non_transferred_antiplatelets = antithrombotics[~antithrombotics['RECANALIZATION_PROCEDURES'].isin([5,6])]
        # Get temporary dataframe with patients who have prescribed antithrombotics and ischemic stroke
        antiplatelets = non_transferred_antiplatelets[non_transferred_antiplatelets['STROKE_TYPE'].isin([1])]
        # Filter temporary dataframe and get only patients who have not been detected or not known for aFib flutter. 
        antiplatelets = antiplatelets[antiplatelets['AFIB_FLUTTER'].isin([4, 5])]
        # Get patients who have prescribed antithrombotics 
        except_recommended = antiplatelets[~antiplatelets['ANTITHROMBOTICS'].isin([9])]

        # Get number of patients who have prescribed antithrombotics and ischemic stroke, have not been detected or not known for aFib flutter.
        self.statsDf['except_recommended_patients'] = self._count_patients(dataframe=except_recommended)
        # Get temporary dataframe groupby protocol ID and antithrombotics column
        self.tmp = antiplatelets.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()

        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# ischemic stroke patients discharged with antiplatelets')
        self.statsDf['% ischemic stroke patients discharged with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged with antiplatelets']/x['except_recommended_patients']) * 100), 2) if x['except_recommended_patients'] > 0 else 0, axis=1)

        # discharged home
        antiplatelets_discharged_home = antiplatelets[antiplatelets['DISCHARGE_DESTINATION'].isin([1])]
        
        if (antiplatelets_discharged_home.empty):
            self.tmp = antiplatelets.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
            self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# ischemic stroke patients discharged home with antiplatelets')
            self.statsDf['% ischemic stroke patients discharged home with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged home with antiplatelets']/x['except_recommended_patients']) * 100), 2) if x['except_recommended_patients'] > 0 else 0, axis=1)
            self.statsDf['except_recommended_discharged_home_patients'] = self.statsDf['except_recommended_patients']
        else:
            self.tmp = antiplatelets_discharged_home.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()
            # Get patients who have prescribed antithrombotics 
            except_recommended_discharged_home = except_recommended[except_recommended['DISCHARGE_DESTINATION'].isin([1])]

            # Get number of patients who have prescribed antithrombotics and ischemic stroke, have not been detected or not known for aFib flutter.
            self.statsDf['except_recommended_discharged_home_patients'] = self._count_patients(dataframe=except_recommended_discharged_home)

            self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# ischemic stroke patients discharged home with antiplatelets')
            self.statsDf['% ischemic stroke patients discharged home with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged home with antiplatelets']/x['except_recommended_discharged_home_patients']) * 100), 2) if x['except_recommended_discharged_home_patients'] > 0 else 0, axis=1)

        # Comapre number of ischemic stroke patients discharged with antiplatelets to the discharged home with antiplatelets and select the higher value
        self.statsDf['# ischemic stroke patients discharged (home) with antiplatelets'] = self.statsDf.apply(lambda x: x['# ischemic stroke patients discharged with antiplatelets'] if x['# ischemic stroke patients discharged with antiplatelets'] > x['# ischemic stroke patients discharged home with antiplatelets'] else x['# ischemic stroke patients discharged home with antiplatelets'], axis=1)
        self.statsDf['% ischemic stroke patients discharged (home) with antiplatelets'] = self.statsDf.apply(lambda x: x['% ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['% ischemic stroke patients discharged home with antiplatelets'], axis=1)

        #### ISCHEMIC STROKE + AFIB + ANTICOAGULANTS ####
        self.statsDf['# afib patients discharged with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed)
        # Get temporary dataframe with patients who are not dead with detected aFib flutter and with prescribed antithrombotics or with nothign (ANTITHROMBOTICS = 10)
        afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([1, 9]))]
        # Get afib patients discharged and not dead
        self.statsDf['afib_detected_discharged_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)

        # self.statsDf['% afib patients discharged with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged with anticoagulants']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)
        self.statsDf['% afib patients discharged with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged with anticoagulants']/x['afib_detected_discharged_patients']) * 100), 2) if (x['afib_detected_discharged_patients']) > 0 else 0, axis=1)
        
        # Get temporary dataframe with patients who have prescribed anticoagulats and were discharged home 
        non_trasferred_anticoagulants = anticoagulants_prescribed[~anticoagulants_prescribed['RECANALIZATION_PROCEDURES'].isin([5,6])]
        anticoagulants_prescribed_discharged_home = non_trasferred_anticoagulants[non_trasferred_anticoagulants['DISCHARGE_DESTINATION'].isin([1])]
        # anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([1])]
        # Get temporary dataframe with patients who have been discharge at home with detected aFib flutter and with prescribed antithrombotics
        # afib_detected_discharged_home = afib_flutter_detected[(afib_flutter_detected['DISCHARGE_DESTINATION'].isin([1])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([9]))]
        afib_detected_discharged_home = afib_flutter_detected[(afib_flutter_detected['DISCHARGE_DESTINATION'].isin([1])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([1,9])) & (~afib_flutter_detected['RECANALIZATION_PROCEDURES'].isin([5,6]))]

        # Check if temporary dataframe is empty. If yes, the value is calculated not only for discharged home, but only dead patients are excluded
        if (anticoagulants_prescribed_discharged_home.empty):
            # afib patients discharged home with anticoagulants	
            anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[~anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([5])]
            # Get temporary dataframe with patients who are not dead with detected aFib flutter and with prescribed antithrombotics 
            afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([1,9]))]
            # Get # afib patients discharged home with anticoagulants
            self.statsDf['# afib patients discharged home with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed_discharged_home)
            # Get afib patients discharged and not dead
            self.statsDf['afib_detected_discharged_home_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)
            # Get % afib patients discharge with anticoagulants and not dead
            self.statsDf['% afib patients discharged home with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged home with anticoagulants']/x['afib_detected_discharged_home_patients']) * 100), 2) if x['afib_detected_discharged_home_patients'] > 0 else 0, axis=1)
        else:
            self.statsDf['# afib patients discharged home with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed_discharged_home)
            # Get afib patients discharged home 
            self.statsDf['afib_detected_discharged_home_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)

            self.statsDf['% afib patients discharged home with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged home with anticoagulants']/x['afib_detected_discharged_home_patients']) * 100), 2) if x['afib_detected_discharged_home_patients'] > 0 else 0, axis=1)

        self.statsDf['# afib patients discharged (home) with anticoagulants'] = self.statsDf.apply(lambda x: x['# afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['# afib patients discharged home with anticoagulants'], axis=1)
        self.statsDf['% afib patients discharged (home) with anticoagulants'] = self.statsDf.apply(lambda x: x['% afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['% afib patients discharged home with anticoagulants'], axis=1)

        #### STROKE UNIT ####
        # stroke patients treated in a dedicated stroke unit / ICU
        self.statsDf['# stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['# patients hospitalized in stroke unit / ICU']

        # % stroke patients treated in a dedicated stroke unit / ICU	
        self.statsDf['% stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['% patients hospitalized in stroke unit / ICU']

        # Create temporary dataframe to calculate final award 
        self.angels_awards_tmp = self.statsDf[[self.total_patient_column, '% patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 45 minutes', '% patients treated with door to thrombolysis < 60 minutes', '% patients treated with door to thrombolysis < 45 minutes', '% patients treated with door to thrombectomy < 120 minutes', '% patients treated with door to thrombectomy < 90 minutes', '% recanalization rate out of total ischemic incidence', '% suspected stroke patients undergoing CT/MRI', '% all stroke patients undergoing dysphagia screening', '% ischemic stroke patients discharged (home) with antiplatelets', '% afib patients discharged (home) with anticoagulants', '% stroke patients treated in a dedicated stroke unit / ICU', '# patients eligible thrombectomy', '# patients eligible thrombolysis']]

        #self.angels_awards_tmp = self.statsDf[[self.total_patient_column, '% patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 45 minutes', '% patients treated with door to thrombolysis < 60 minutes', '% patients treated with door to thrombolysis < 45 minutes', '% patients treated with door to thrombectomy < 120 minutes', '% patients treated with door to thrombectomy < 90 minutes', '% recanalization rate out of total ischemic incidence', '% suspected stroke patients undergoing CT/MRI', '% all stroke patients undergoing dysphagia screening', '% ischemic stroke patients discharged (home) with antiplatelets', '% patients prescribed anticoagulants with aFib', '% stroke patients treated in a dedicated stroke unit / ICU', '# patients eligible thrombectomy', '# patients eligible thrombolysis']]

        
        self.statsDf.fillna(0, inplace=True)

        self.angels_awards_tmp.loc[:, 'Proposed Award (old calculation)'] = self.angels_awards_tmp.apply(lambda x: self._get_final_award(x, new_calculation=False), axis=1)
        self.angels_awards_tmp.loc[:, 'Proposed Award'] = self.angels_awards_tmp.apply(lambda x: self._get_final_award(x, new_calculation=True), axis=1)
        
        self.statsDf['Proposed Award (old calculation)'] = self.angels_awards_tmp['Proposed Award (old calculation)']
        self.statsDf['Proposed Award'] = self.angels_awards_tmp['Proposed Award'] 

        self.statsDf.rename(columns={"Protocol ID": "Site ID"}, inplace=True)

        self.statsDf.drop_duplicates(inplace=True)
        
        self.sites = self._get_sites(self.statsDf)

    def _get_final_award(self, x, new_calculation=True):
        """ The function calculating the proposed award. 

        :param x: the row from temporary dataframe
        :type x: pandas series
        :returns: award -- the proposed award
        """
        if x[self.total_patient_column] == False:
            award = "STROKEREADY"
        else:
            if new_calculation:
                thrombolysis_therapy_lt_60min = x['% patients treated with door to thrombolysis < 60 minutes']

                # Calculate award for thrombolysis, if no patients were eligible for thrombolysis and number of total patients was greater than minimum than the award is set to DIAMOND 
                if (float(thrombolysis_therapy_lt_60min) >= 50 and float(thrombolysis_therapy_lt_60min) <= 74.99):
                    award = "GOLD"
                elif (float(thrombolysis_therapy_lt_60min) >= 75):
                    award = "DIAMOND"
                else: 
                    award = "STROKEREADY"

                thrombolysis_therapy_lt_45min = x['% patients treated with door to thrombolysis < 45 minutes']

                if award != "STROKEREADY":
                    if (float(thrombolysis_therapy_lt_45min) <= 49.99):
                        if (award != "GOLD" or award == "DIAMOND"):
                            award = "PLATINUM"
                    elif (float(thrombolysis_therapy_lt_45min) >= 50):
                        if (award != "GOLD"):
                            award = "DIAMOND"
                    else:
                        award = "STROKEREADY"


                # Calculate award for thrombectomy, if no patients were eligible for thrombectomy and number of total patients was greater than minimum than the award is set to the possible proposed award (eg. if in thrombolysis step award was set to GOLD then the award will be GOLD)
                thrombectomy_pts = x['# patients eligible thrombectomy']
                # if thrombectomy_pts != 0:
                if thrombectomy_pts > 3:
                    thrombectomy_therapy_lt_120min = x['% patients treated with door to thrombectomy < 120 minutes']
                    if award != "STROKEREADY":
                        if (float(thrombectomy_therapy_lt_120min) >= 50 and float(thrombectomy_therapy_lt_120min) <= 74.99):
                            if (award == "PLATINUM" or award == "DIAMOND"):
                                award = "GOLD"
                        elif (float(thrombectomy_therapy_lt_120min) >= 75):
                            if (award == "DIAMOND"):
                                award = "DIAMOND"
                        else: 
                            award = "STROKEREADY"

                    thrombectomy_therapy_lt_90min = x['% patients treated with door to thrombectomy < 90 minutes']
                    if award != "STROKEREADY":
                        if (float(thrombectomy_therapy_lt_90min) <= 49.99):
                            if (award != "GOLD" or award == "DIAMOND"):
                                award = "PLATINUM"
                        elif (float(thrombectomy_therapy_lt_90min) >= 50):
                            if (award == "DIAMOND"):
                                award = "DIAMOND"
                        else:
                            award = "STROKEREADY"
            else:
                recan_therapy_lt_60min = x['% patients treated with door to recanalization therapy < 60 minutes']
                if (float(recan_therapy_lt_60min) >= 50 and float(recan_therapy_lt_60min) <= 74.99):
                    award = "GOLD"
                elif (float(recan_therapy_lt_60min) >= 75):
                    award = "DIAMOND"
                else: 
                    award = "STROKEREADY"

                recan_therapy_lt_45min = x['% patients treated with door to recanalization therapy < 45 minutes']
                if award != "STROKEREADY":
                    if (float(recan_therapy_lt_45min) <= 49.99):
                        if (award != "GOLD" or award == "DIAMOND"):
                            award = "PLATINUM"
                    elif (float(recan_therapy_lt_45min) >= 50):
                        if (award != "GOLD"):
                            award = "DIAMOND"
                    else:
                        award = "STROKEREADY"

            recan_rate = x['% recanalization rate out of total ischemic incidence']
            if award != "STROKEREADY":
                if (float(recan_rate) >= 5 and float(recan_rate) <= 14.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(recan_rate) >= 15 and float(recan_rate) <= 24.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(recan_rate) >= 25):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"


            ct_mri = x['% suspected stroke patients undergoing CT/MRI']
            if award != "STROKEREADY":
                if (float(ct_mri) >= 80 and float(ct_mri) <= 84.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(ct_mri) >= 85 and float(ct_mri) <= 89.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(ct_mri) >= 90):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"

            dysphagia_screening = x['% all stroke patients undergoing dysphagia screening']
            if award != "STROKEREADY":
                if (float(dysphagia_screening) >= 80 and float(dysphagia_screening) <= 84.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(dysphagia_screening) >= 85 and float(dysphagia_screening) <= 89.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(dysphagia_screening) >= 90):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"

            discharged_with_antiplatelets_final = x['% ischemic stroke patients discharged (home) with antiplatelets']
            if award != "STROKEREADY":
                if (float(discharged_with_antiplatelets_final) >= 80 and float(discharged_with_antiplatelets_final) <= 84.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(discharged_with_antiplatelets_final) >= 85 and float(discharged_with_antiplatelets_final) <= 89.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(discharged_with_antiplatelets_final) >= 90):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"

            discharged_with_anticoagulants_final = x['% afib patients discharged (home) with anticoagulants']
            if award != "STROKEREADY":
                if (float(discharged_with_anticoagulants_final) >= 80 and float(discharged_with_anticoagulants_final) <= 84.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(discharged_with_anticoagulants_final) >= 85 and float(discharged_with_anticoagulants_final) <= 89.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(discharged_with_anticoagulants_final) >= 90):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"

            stroke_unit = x['% stroke patients treated in a dedicated stroke unit / ICU']
            if award != "STROKEREADY":
                if (float(stroke_unit) <= 0.99):
                    if (award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(stroke_unit) >= 1):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "STROKEREADY"

        return award

    def _count_patients(self, dataframe):
        """ The function calculating the number of patients per site. 

        :param dataframe: the dataframe with the raw data
        :type dataframe: dataframe
        :returns: the column with number of patients
        """
        tmpDf = dataframe.groupby(['Protocol ID']).size().reset_index(name='count_patients')
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.fillna(0, inplace=True)

        return factorDf['count_patients']

    def _get_values_only_columns(self, column_name, value, dataframe):
        """ The function calculating the numbeer of patients per site for the given value from the temporary dataframe. 

        :param column_name: the name of column name the number of patients should be calculated
        :type column_name: str
        :param value: the value for which we would like to get number of patients from the specific column
        :type value: int
        :param dataframe: the dataframe with the raw data
        :type dataframe: pandas dataframe
        :returns: the column with the number of patients
        """

        tmpDf = dataframe[dataframe[column_name] == value].reset_index()[['Protocol ID', 'count']]
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.fillna(0, inplace=True)

        return factorDf['count']

    def _get_values_for_factors(self, column_name, value, new_column_name, df=None):
        """ The function calculating the numbeer of patients per site for the given value from the temporary dataframe. 

        :param column_name: the name of column name the number of patients should be calculated
        :type column_name: str
        :param value: the value for which we would like to get number of patients from the specific column
        :type value: int
        :param new_column_name: to this value will be renamed the created column containing the number of patients
        :type new_column_name: str
        :param df: the dataframe with the raw data
        :type df: pandas dataframe
        :returns: the dataframe with calculated statistics
        """
        # Check if type of column name is type of number, if not convert value into string
        if (self.tmp[column_name].dtype != np.number):
            value = str(value)
        else:
            value = value 

        tmpDf = self.tmp[self.tmp[column_name] == value].reset_index()[['Protocol ID', 'count']]
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.rename(columns={'count': new_column_name}, inplace=True)
        factorDf.fillna(0, inplace=True)

        return factorDf

    def _get_values_for_factors_more_values(self, column_name, value, new_column_name, df=None):
        """ The function calculating the number of patients per site for the given value from the temporary dataframe. 

        :param column_name: the name of column name the number of patients should be calculated
        :type column_name: str
        :param value: the list of values for which we would like to get number of patients from the specific column
        :type value: list
        :param new_column_name: to this value will be renamed the created column containing the number of patients
        :type new_column_name: str
        :param df: the dataframe with the raw data
        :type df: pandas dataframe
        :returns: the dataframe with calculated statistics
        """
        if df is None:
            tmpDf = self.tmp[self.tmp[column_name].isin(value)].reset_index()[['Protocol ID', 'count']]
            tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
            factorDf = self.statsDf.merge(tmpDf, how='outer')
            factorDf.rename(columns={'count': new_column_name}, inplace=True)
            factorDf.fillna(0, inplace=True)
        else:
            tmpDf = df[df[column_name].isin(value)].reset_index()[['Protocol ID', 'count']]
            tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
            factorDf = self.statsDf.merge(tmpDf, how='outer')
            factorDf.rename(columns={'count': new_column_name}, inplace=True)
            factorDf.fillna(0, inplace=True)

        return factorDf

    def _get_values_for_factors_containing(self, column_name, value, new_column_name, df=None):
        """ The function calculating the number of patients per site for the given value from the temporary dataframe. 

        :param column_name: the name of column name the number of patients should be calculated
        :type column_name: str
        :param value: the value of string type for which we would like to get number of patients from the specific column
        :type value: str
        :param new_column_name: to this value will be renamed the created column containing the number of patients
        :type new_column_name: str
        :param df: the dataframe with the raw data
        :type df: pandas dataframe
        :returns: the dataframe with calculated statistics
        """
        
        if df is None:
            tmpDf = self.tmp[self.tmp[column_name].str.contains(value)].reset_index()[['Protocol ID', 'count']]
            tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
            factorDf = self.statsDf.merge(tmpDf, how='outer')
            factorDf.rename(columns={'count': new_column_name}, inplace=True)
            factorDf.fillna(0, inplace=True)
        else:
            tmpDf = df[df[column_name].str.contains(value)].reset_index()[['Protocol ID', 'count']]
            tmpDf = tmpDf.groupby('Protocol ID').sum().reset_index()
            factorDf = self.statsDf.merge(tmpDf, how='outer')
            factorDf.rename(columns={'count': new_column_name}, inplace=True)
            factorDf.fillna(0, inplace=True)

        return factorDf

    def _get_ctmri_delta(self, hosp_time, ct_time):
        """ The function calculating the difference between two times in minutes. 

        :param hosp_time: the time of hospitalization
        :type hosp_time: time
        :param ct_time: the time when CT/MRI was performed
        :type ct_time: time
        :returns: tdelta between two times in minutes
        """

        timeformat = '%H:%M:%S'

        # Check if both time are not None if yes, return 0 else return tdelta
        if hosp_time is None or ct_time is None or pd.isnull(hosp_time) or pd.isnull(ct_time):
            tdeltaMin = 0
        elif hosp_time == 0 or ct_time == 0:
            tdeltaMin = 0
        else:
            if isinstance(ct_time, time) and isinstance(hosp_time, time):
                tdelta = datetime.combine(date.today(), ct_time) - datetime.combine(date.today(), hosp_time)
            elif isinstance(ct_time, time):
                tdelta = datetime.combine(date.today(), ct_time) - datetime.strptime(hosp_time, timeformat)
            elif isinstance(hosp_time, time):
                tdelta = datetime.strptime(ct_time, timeformat) - datetime.strptime(hosp_time, timeformat)
            else:
                tdelta = datetime.strptime(ct_time, timeformat) - datetime.strptime(hosp_time, timeformat)	
            tdeltaMin = tdelta.total_seconds()/60.0

        if tdeltaMin > 60:
            res = 2
        elif tdeltaMin <= 60 and tdeltaMin > 0:
            res = 1
        else:
            res = -2
        return res

    
    def _return_dataset(self):
        """ The function returning dataframe. """

        return self.df

    def _return_stats(self):
        """ The function returning the dataframe with the calculated statistics! 
        
        :returns: the dataframe with the statistics
        """

        return self.statsDf

    def _get_sites(self, df):
        """ The function returning the list of sites in the preprocessed data. 
        
        :returns: the list of sites
        """

        site_ids = df['Site ID'].tolist()
        site_list = list(set(site_ids))

        return site_list

    @property
    def country_name(self):
        return self._country_name










        



        

