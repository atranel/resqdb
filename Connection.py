#### Filename: Connection.py
#### Version: v1.0
#### Author: Marie Jankujova
#### Date: March 4, 2019
#### Description: Connect to database and get atalaia dataframe.

import psycopg2
import sys
import os
import pandas as pd
import logging
from configparser import ConfigParser
from resqdb.CheckData import CheckData
import numpy as np
import time
from multiprocessing import Process, Pool
from threading import Thread
import collections
import datetime
import csv
from dateutil.relativedelta import relativedelta
import json

class Connection():
    """ The class connecting to the database and exporting the data for the Slovakia. 

    :param nprocess: number of processes
    :type nprocess: int
    :param data: the name of data (resq or atalaia)
    :type data: str
    """

    def __init__(self, nprocess=1, data='resq'):

        start = time.time()

        # Create log file in the working folder
        debug = 'debug_' + datetime.datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Connecting to datamix database!')   

        # Get absolute path
        path = os.path.dirname(__file__)
        self.database_ini = os.path.join(path, 'database.ini')

        # Read temporary csv file with CZ report names and Angels Awards report names
        path = os.path.join(os.path.dirname(__file__), 'tmp', 'czech_mapping.json')
        with open(path, 'r', encoding='utf-8') as json_file:
            cz_names_dict = json.load(json_file)

        # Set section
        datamix = 'datamix-backup'
        # datamix = 'datamix'
        # Check which data should be exported
        if data == 'resq':
            # Create empty dictionary
            # self.sqls = ['SELECT * from resq_mix', 'SELECT * from ivttby_mix', 'SELECT * from thailand', 'SELECT * from resq_ivttby_mix']
            self.sqls = ['SELECT * from resq_mix', 'SELECT * from ivttby_mix', 'SELECT * from thailand']
            # List of dataframe names
            self.names = ['resq', 'ivttby', 'thailand']
        elif data == 'atalaia': 
            self.sqls = ['SELECT * from atalaia_mix']
            self.names = []
        elif data == 'qasc':
            self.sqls = ['SELECT * FROM qasc_mix']
            self.names = []
        elif data == 'africa': 
            self.sqls = ['SELECT * FROM africa_mix']
            self.names = []
        
        # Dictionary initialization - db dataframes
        self.dictdb_df = {}
        # Dictioanry initialization - prepared dataframes
        self.dict_df = {}

        if nprocess == 1:
            if data == 'resq':
                for i in range(0, len(self.names)):
                    df_name = self.names[i]
                    self.connect(self.sqls[i], datamix, nprocess, df_name=df_name)
                
                # self.connect(self.sqls[2], datamix, nprocess, df_name='resq_ivttby_mix')
                # self.resq_ivttby_mix = self.dictdb_df['resq_ivttby_mix']
                # self.dictdb_df['resq_ivttby_mix'].to_csv('resq_ivttby_mix.csv', sep=',', index=False)
                # if 'resq_ivttby_mix' in self.dictdb_df.keys():
                #     del self.dictdb_df['resq_ivttby_mix']

                for k, v in self.dictdb_df.items():
                    self.prepare_df(df=v, name=k)

                self.df = pd.DataFrame()
                for i in range(0, len(self.names)):
                    self.df = self.df.append(self.dict_df[self.names[i]], sort=False)
                    logging.info("Connection: {0} dataframe has been appended to the resulting dataframe!".format(self.names[i]))
                # Get all country code in dataframe
                self.countries = self._get_countries(df=self.df)
                # Get preprocessed data
                self.preprocessed_data = self.check_data(df=self.df, nprocess=1)

                self.preprocessed_data['RES-Q reports name'] = self.preprocessed_data.apply(lambda x: cz_names_dict[x['Protocol ID']]['report_name'] if 'Czech Republic' in x['Country'] and x['Protocol ID'] in cz_names_dict.keys() else x['Site Name'], axis=1)
                self.preprocessed_data['ESO Angels name'] = self.preprocessed_data.apply(lambda x: cz_names_dict[x['Protocol ID']]['angels_name'] if 'Czech Republic' in x['Country'] and x['Protocol ID'] in cz_names_dict.keys() else x['Site Name'], axis=1)

                ##############
                # ONSET TIME #
                ##############
                self.preprocessed_data['HOSPITAL_TIME'] = pd.to_datetime(self.preprocessed_data['HOSPITAL_TIME'], format='%H:%M:%S').dt.time
                try:
                    self.preprocessed_data['HOSPITAL_TIMESTAMP'] = self.preprocessed_data.apply(lambda x: datetime.datetime.combine(x['HOSPITAL_DATE'], x['HOSPITAL_TIME']) if not pd.isnull(x['HOSPITAL_TIME']) and not pd.isnull(x['HOSPITAL_DATE']) else None, axis=1)
                    #self.preprocessed_data['HOSPITAL_TIMESTAMP'] = pd.to_datetime(self.preprocessed_data['HOSPITAL_DATE'] + ' ' + self.preprocessed_data['HOSPITAL_TIME'])
                except ValueError as error:
                    logging.error("Error occured when converting hospital date and time into timestamp object - {}.".format(error))
                
                self.preprocessed_data['VISIT_DATE'] = self.preprocessed_data.apply(lambda x: self.fix_date(x['VISIT_DATE'], x['HOSPITAL_DATE']), axis=1)
                self.preprocessed_data['VISIT_TIME'] = pd.to_datetime(self.preprocessed_data['VISIT_TIME'], format='%H:%M:%S').dt.time
            
                try:
                    self.preprocessed_data['VISIT_TIMESTAMP'] = self.preprocessed_data.apply(lambda x: datetime.datetime.combine(x['VISIT_DATE'], x['VISIT_TIME']) if not pd.isnull(x['VISIT_TIME']) and not pd.isnull(x['VISIT_DATE']) else None, axis=1)
                    #self.preprocessed_data['VISIT_TIMESTAMP'] = pd.to_datetime(self.preprocessed_data['VISIT_DATE'] + ' ' + self.preprocessed_data['VISIT_TIME'])
                except ValueError as error:
                    logging.error("Error occured when converting visit date and time into timestamp object - {}.".format(error))			

                # Get difference in minutes between hospitalization and last visit
                self.preprocessed_data['LAST_SEEN_NORMAL'] = self.preprocessed_data.apply(lambda x: self.time_diff(x['VISIT_TIMESTAMP'], x['HOSPITAL_TIMESTAMP']), axis=1)
                self.preprocessed_data['LAST_SEEN_NORMAL'].fillna(0, inplace=True)

                # Create new column to set if patient has stroke in hospital and recanalization procedures were entered in timestamps
                self.preprocessed_data['HOSPITAL_STROKE_IVT_TIMESTAMPS'] = np.nan
                self.preprocessed_data.loc[
                    (self.preprocessed_data['HOSPITAL_STROKE'] == 1) &
                    ((self.preprocessed_data['IVT_ONLY'] == 2) | 
                    (self.preprocessed_data['IVT_TBY'] == 2) | 
                    (self.preprocessed_data['IVT_TBY_REFER'] == 2)),
                    'HOSPITAL_STROKE_IVT_TIMESTAMPS'] = 1
                
                self.preprocessed_data['HOSPITAL_STROKE_TBY_TIMESTAMPS'] = np.nan
                self.preprocessed_data.loc[
                    (self.preprocessed_data['HOSPITAL_STROKE'] == 1) &
                    ((self.preprocessed_data['IVT_TBY'] == 2) |
                    (self.preprocessed_data['TBY_ONLY'] == 2) | 
                    (self.preprocessed_data['TBY_REFER_LIM'] == 2) | 
                    (self.preprocessed_data['TBY_REFER_ALL'] == 2)),
                    'HOSPITAL_STROKE_TBY_TIMESTAMPS'] = 1
        
            elif data == 'atalaia':
                self.connect(self.sqls[0], datamix, nprocess, df_name='atalaia_mix')
                self.atalaiadb_df = self.dictdb_df['atalaia_mix']
                #self.atalaia_preprocessed_data = self.prepare_atalaia_df(self.atalaiadb_df)
                self.atalaia_preprocessed_data = self.atalaiadb_df.copy()
                del self.dictdb_df['atalaia_mix']
            elif data == 'qasc':
                self.__get_qasc_df(datamix, nprocess)
            elif data == 'africa':
                self.__get_africa_df(datamix, nprocess)

        else:
            if data == 'resq':
                threads = []
                for i in range(0, len(self.names)):
                    df_name = self.names[i]
                    process = Thread(target=self.connect(self.sqls[i], datamix, i, df_name=df_name))
                    process.start()
                    threads.append(process)
                # logging.info('The process with id {0} is running.'.format(process))

                process = Thread(target=self.connect(self.sqls[2], datamix, 1, df_name='resq_ivttby_mix'))
                process.start()
                threads.append(process)

                for process in threads:
                    process.join()
            
                end = time.time()
                tdelta = (end-start)/60
                logging.info('The database data were exported in {0} minutes.'.format(tdelta))

                # self.dictdb_df['resq_ivttby_mix'].to_csv('resq_ivttby_mix.csv', sep=',', index=False)
                if 'resq_ivttby_mix' in self.dictdb_df.keys():
                    del self.dictdb_df['resq_ivttby_mix']
                treads = []
                for i in range(0, len(self.names)):
                    df_name = self.names[i]
                    process = Thread(target=self.prepare_df(df=self.dictdb_df[df_name], name=df_name))
                    process.start()
                    threads.append(process)

                for process in threads:
                    process.join()

                end = time.time()
                tdelta = (end-start)/60
                logging.info('The database data were prepared in {0} minutes.'.format(tdelta))

                self.df = pd.DataFrame()
                for i in range(0, len(self.names)):
                    self.df = self.df.append(self.dict_df[self.names[i]], sort=False)
                    logging.info("Connection: {0} dataframe has been appended to the resulting dataframe!.".format(self.names[i]))

                subject_ids = self.df['Subject ID'].tolist()
                duplicates = [item for item, count in collections.Counter(subject_ids).items() if count > 1]

                
                for i in duplicates:
                    duplicates_rows = self.df[(self.df['Subject ID'] == i) & (~pd.isnull(self.df['crf_parent_name']))]
                    set_tmp = set(duplicates_rows['Protocol ID'])
                    if len(set_tmp) == 1:
                        crfs = duplicates_rows['crf_parent_name'].tolist()

                        #print(duplicates_rows[['Subject ID', 'Protocol ID']])
                        for i in crfs:
                            if 'RESQV12' in i:
                                keep_crf = i
                            if 'RESQV20' in i:
                                keep_crf = i
                            if 'IVT_TBY' in i and 'DEVCZ10' not in i:
                                keep_crf = i
                    
                        index = duplicates_rows.index[duplicates_rows['crf_parent_name'] != keep_crf].tolist()
                        self.df.drop(index, inplace=True)

                        #print(duplicates_rows['crf_parent_name'])
                        #print("Keep form: {0}, deleted row: {1}".format(keep_crf, index))
                
                    
                # Get all country code in dataframe
                self.countries = self._get_countries(df=self.df)
                # Cal check data function
                self.preprocessed_data = self.check_data(self.df, nprocess=nprocess)
                #self.preprocessed_data = self.check_data(self.df, nprocess=None)   

                self.preprocessed_data['RES-Q reports name'] = self.preprocessed_data.apply(lambda x: cz_names_dict[x['Protocol ID']]['report_name'] if 'Czech Republic' in x['Country'] and x['Protocol ID'] in cz_names_dict.keys() else x['Site Name'], axis=1)
                self.preprocessed_data['ESO Angels name'] = self.preprocessed_data.apply(lambda x: cz_names_dict[x['Protocol ID']]['angels_name'] if 'Czech Republic' in x['Country'] and x['Protocol ID'] in cz_names_dict.keys() else x['Site Name'], axis=1)

                ##############
                # ONSET TIME #
                ##############
                self.preprocessed_data['HOSPITAL_TIME'] = pd.to_datetime(self.preprocessed_data['HOSPITAL_TIME'], format='%H:%M:%S').dt.time
                try:
                    self.preprocessed_data['HOSPITAL_TIMESTAMP'] = self.preprocessed_data.apply(lambda x: datetime.datetime.combine(x['HOSPITAL_DATE'], x['HOSPITAL_TIME']) if not pd.isnull(x['HOSPITAL_TIME']) and not pd.isnull(x['HOSPITAL_DATE']) else None, axis=1)
                    #self.preprocessed_data['HOSPITAL_TIMESTAMP'] = pd.to_datetime(self.preprocessed_data['HOSPITAL_DATE'] + ' ' + self.preprocessed_data['HOSPITAL_TIME'])
                except ValueError as error:
                    logging.error("Error occured when converting hospital date and time into timestamp object - {}.".format(error))
                
                self.preprocessed_data['VISIT_DATE'] = self.preprocessed_data.apply(lambda x: self.fix_date(x['VISIT_DATE'], x['HOSPITAL_DATE']), axis=1)
                self.preprocessed_data['VISIT_TIME'] = pd.to_datetime(self.preprocessed_data['VISIT_TIME'], format='%H:%M:%S').dt.time
            
                try:
                    self.preprocessed_data['VISIT_TIMESTAMP'] = self.preprocessed_data.apply(lambda x: datetime.datetime.combine(x['VISIT_DATE'], x['VISIT_TIME']) if not pd.isnull(x['VISIT_TIME']) and not pd.isnull(x['VISIT_DATE']) else None, axis=1)
                    #self.preprocessed_data['VISIT_TIMESTAMP'] = pd.to_datetime(self.preprocessed_data['VISIT_DATE'] + ' ' + self.preprocessed_data['VISIT_TIME'])
                except ValueError as error:
                    logging.error("Error occured when converting visit date and time into timestamp object - {}.".format(error))			

                # Get difference in minutes between hospitalization and last visit
                self.preprocessed_data['LAST_SEEN_NORMAL'] = self.preprocessed_data.apply(lambda x: self.time_diff(x['VISIT_TIMESTAMP'], x['HOSPITAL_TIMESTAMP']), axis=1)
                self.preprocessed_data['LAST_SEEN_NORMAL'].fillna(0, inplace=True)

                # Create new column to set if patient has stroke in hospital and recanalization procedures were entered in timestamps
                self.preprocessed_data['HOSPITAL_STROKE_IVT_TIMESTAMPS'] = np.nan
                self.preprocessed_data.loc[
                    (self.preprocessed_data['HOSPITAL_STROKE'] == 1) &
                    ((self.preprocessed_data['IVT_ONLY'] == 2) | 
                    (self.preprocessed_data['IVT_TBY'] == 2) | 
                    (self.preprocessed_data['IVT_TBY_REFER'] == 2)),
                    'HOSPITAL_STROKE_IVT_TIMESTAMPS'] = 1
                
                self.preprocessed_data['HOSPITAL_STROKE_TBY_TIMESTAMPS'] = np.nan
                self.preprocessed_data.loc[
                    (self.preprocessed_data['HOSPITAL_STROKE'] == 1) &
                    ((self.preprocessed_data['IVT_TBY'] == 2) |
                    (self.preprocessed_data['TBY_ONLY'] == 2) | 
                    (self.preprocessed_data['TBY_REFER_LIM'] == 2) | 
                    (self.preprocessed_data['TBY_REFER_ALL'] == 2)),
                    'HOSPITAL_STROKE_TBY_TIMESTAMPS'] = 1
            
            elif data == 'atalaia':
                self.connect(self.sqls[0], datamix, nprocess, df_name='atalaia_mix')
                self.atalaiadb_df = self.dictdb_df['atalaia_mix']
                #self.atalaia_preprocessed_data = self.prepare_atalaia_df(self.atalaiadb_df)
                self.atalaia_preprocessed_data = self.atalaiadb_df.copy()
                del self.dictdb_df['atalaia_mix']

            elif data == 'qasc':
                self.__get_qasc_df(datamix, nprocess)

            elif data == 'africa':
                self.__get_africa_df(datamix, nprocess)
        
        end = time.time()
        tdelta = (end-start)/60
        logging.info('The conversion and merging run {0} minutes.'.format(tdelta))

    def __get_africa_df(self, datamix, nprocess):
        ''' Get africa_mix data from the datamix database. 
        
        :param datamix: the name of the setting from the database.ini file
        :type datamix: str
        :param nprocess: the number of process to be run
        :type nprocess: int
        '''
        df_name = 'africa_mix'
        self.connect(self.sqls[0], datamix, nprocess, df_name=df_name)
        self.africa_df = self.dictdb_df[df_name]

        # Make column names uppercassed
        column_names = [x.upper() for x in self.africa_df.columns]
        
        # Remove _EN from the column name
        import re
        column_names = [re.sub(r"_EN.*", "", name) for name in column_names]
            
        # Rename columns
        self.africa_df.rename(columns=dict(zip(self.africa_df.columns[0:], column_names)), inplace=True)
            
        self.preprocessed_data = self.africa_df.copy()
        del self.dictdb_df[df_name]


    def __get_qasc_df(self, datamix, nprocess):
        ''' Get QASC data from the database. 
        
        :param datamix: the name of the setting from the database.ini file
        :type datamix: str
        :param nprocess: the number of process to be run
        :type nprocess: int
        '''
        # Get QASC data from the database
        df_name = 'qasc_mix'
        self.connect(self.sqls[0], datamix, nprocess, df_name=df_name)
        self.qascdb_df = self.dictdb_df[df_name]

        # Make column names as uppercase
        column_names = self.qascdb_df.columns
        column_names = [x.upper() for x in column_names]
        self.qascdb_df.rename(columns=dict(zip(self.qascdb_df.columns[0:], column_names)),inplace=True)

        # Convert DATE_CREATED to datetime
        dateForm = '%Y-%m-%d'
        self.qascdb_df['DATE_CREATED'] = pd.to_datetime(self.qascdb_df['DATE_CREATED'], format=dateForm)

        self.preprocessed_data = self.qascdb_df.copy()
        del self.dictdb_df[df_name]

        self.connect(
            'SELECT DISTINCT unique_identifier, facility_name FROM study ORDER BY unique_identifier;', 
            datamix, 
            nprocess, 
            df_name='study')
        self.study_df = self.dictdb_df['study']
        del self.dictdb_df['study']


    def config(self, section):
        """ The function reading and parsing the config of database file. 

        :param section: the name of the section in database.ini file
        :type section: str
        :returns: the dictionary with the parsed section values
        :rtype: dictionary
        :raises: Exception
        """
        # Create a parser object
        parser = ConfigParser()
        # Read config file
        parser.read(self.database_ini)

        # Get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            logging.error('Connection: Section {0} not found in the {1} file'.format(section, self.database_ini))
            raise Exception('Section {0} not found in the {1} file'.format(section, self.database_ini))

        return db

    
    def connect(self, sql, section, nprocess, df_name=None):
        """ The function connecting to te database. 

        :param sql: the sql query 
        :type sql: str
        :param section: the section from the database.ini
        :type section: str
        :param nprocess: the number of processes run simultaneously
        :type nprocess: int
        :param df_name: the name of the dataframe used as key in the dictionary
        :type df_name: str
        :raises: Exception
        """

        conn = None    
        try: 
            # Read connection parameters
            params = self.config(section)

            # Connect to the PostgreSQL server
            logging.info('Process{0}: Connecting to the PostgreSQL database... '.format(nprocess))
            conn = psycopg2.connect(**params)
            # Create dataframe for given sql query
            if df_name is not None:
                self.dictdb_df[df_name] = pd.read_sql_query(sql, conn)
                logging.info('Process{0}: Dataframe {1} has been created created.'.format(nprocess, df_name))
            else:
                logging.info('Process{0}: Name of dataframe is missing.'.format(nprocess))

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

        finally:
            if conn is not None:
                conn.close()
                logging.info('Process{0}: Database connection has been closed.'.format(nprocess))
    
    
    def prepare_df(self, df, name):
        """ The function preparing the raw data from the database to be used for statistic calculation. The prepared dataframe is entered into dict_df and the name is used as key.
        
        :param df: the raw dataframe exported from the database
        :type df: pandas dataframe
        :param name: the name of the database
        :type name: str
        """

        if 'resq' in name:
            df.rename(columns={'fabry_cs': 'fabry_en'}, inplace=True)
            # If CRF is v1.2 replace BLEEDING REASON with -999
            df['bleeding_reason_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['bleeding_reason_en'], axis=1)
            # If CRF is v1.2 replace INTERVENTION with -999
            df['intervention_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['intervention_en'], axis=1)
            # If CRF is v1.2 replace RECURRENT_STROKE value with -999
            df['recurrent_stroke_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['recurrent_stroke_en'], axis=1)
            # If CRF is v1.2 replace VENTILATOR value with -999
            df['ventilator_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['ventilator_en'], axis=1)
            # If CRF is v1.2 and stroke type is 2 then neurosurgery is 3
            df['neurosurgery_en'] = df.apply(lambda x: 3 if "RESQV12" in x['oc_oid'] and x['stroke_type_en'] == 2 else x['neurosurgery_en'], axis=1)
            # If CRF is v1.2 replace BLEEDING SOURCE with -999
            df['bleeding_source_en'] = df.apply(lambda x: 3 if "RESQV12" in x['oc_oid'] else x['bleeding_source_en'], axis=1)
            # If CRF is v1.2 replace CEREBROVASCULAR EXPERT with -999
            df['cerebrovascular_expert_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['cerebrovascular_expert_en'], axis=1)
            
            # If CRF is v1.2. replace DISCHARGE SAME FACILITY with -999 if DISCHARGE_DESTINATIOn is not 2, else 1
            def discharge_same_facility(val):
                res = 1 if val == 2 else -999
                return res
            df['discharge_same_facility_en'] = df.apply(lambda x: discharge_same_facility(x['discharge_destination_en']) if "RESQV12" in x['oc_oid'] else x['discharge_same_facility_en'], axis=1) 
            # If CRF is v1.2. replace DISCHARGE OTHER FACILITY with -999 if DISCHARGE_DESTINATION is not 3, else 3
            def discharge_other_facility(val):
                res = 3 if val == 3 else -999
                return res
            df['discharge_other_facility_en'] = df.apply(lambda x: discharge_other_facility(x['discharge_destination_en']) if "RESQV12" in x['oc_oid'] else x['discharge_other_facility_en'], axis=1)
            # If CRF is v1.2 replace DISCHARGE OTHER FACILITY O2 with -999
            df['discharge_other_facility_o2_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['discharge_other_facility_o2_en'], axis=1)
            # If CRF is v1.2 replace DISCHARGE OTHER FACILITY O1 with -999
            df['discharge_other_facility_o1_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['discharge_other_facility_o1_en'], axis=1)
            # If CRF is v1.2. replace DISCHARGE OTHER FACILITY O3 with -999 if DISCHARGE_DESTINATION is not 3, else 4
            def discharge_other_facility_o3(val):
                res = 4 if val == 3 else -999
                return res
            df['discharge_other_facility_o3_en'] = df.apply(lambda x: discharge_other_facility_o3(x['discharge_destination_en']) if "RESQV12" in x['oc_oid'] else x['discharge_other_facility_o3_en'], axis=1)
            # If CRF is v1.2 replace DEPARTMENT TYPE with -999 else kepp DEPARTMENT_TYPE
            df['department_type_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['department_type_en'], axis=1)

            # Rename PT column (Prenotification + Prior mRS)
            df.rename(columns={
                'prenotification_pt_2': 'prenotification_en',
                'mrs_prior_stroke_pt_2': 'mrs_prior_stroke_en'
            }, inplace=True)
            # Get only columns ending with _en
            cols = ['site_id', 'facility_name', 'oc_oid', 'label', 'facility_country']
            cols.extend([c for c in df.columns if c.endswith('_en')])

            res = df[cols].copy()
            # Remove _en suffix from column names
            cols = res.columns
            suffix = "_en"
            new_cols = []
            for c in cols:
                if c.endswith(suffix):
                    new_cols.append(c[:len(c)-len(suffix)].upper())
                elif c == 'site_id':
                    new_cols.append('Protocol ID')
                elif c == "facility_name":
                    new_cols.append('Site Name')
                elif c == "label":
                    new_cols.append('Subject ID')
                elif c == "oc_oid":
                    new_cols.append('crf_parent_name')
                elif c == "facility_country":
                    new_cols.append('Country')
                else:
                    new_cols.append(c)

            res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)
            logging.info("Connection: Column names in RESQ were changed successfully.")

            self.dict_df[name] = res

        elif 'ivttby' in name:
            df.to_csv('ivttby.csv', sep=',', index=False)
            # Get patients inserted in IVT_TBY_DEV
            ivttby_dev = df[df['oc_oid'].str.contains('')]
            #df = df[df['oc_oid'] != "F_RESQ_IVT_TBY_1565_DEVCZ10"].copy()

            # Merge ct_time columns 
            vals = [1,2,3,4,5,6]
            df['ct_time_cz'] = df.apply(lambda x: x['ct_time_2_cz'] if (x['ct_mri_cz'] in vals and pd.isnull(x['ct_time_cz'])) else x['ct_time_cz'], axis=1)

            # Rename tby_refer_all_groin_puncture_time_cz and tby_refer_lim_groin_puncture_time_cz to something else, because we need to keep only these times with _cz_2
            df.rename(columns={
                'tby_refer_all_groin_puncture_time_cz': 'tby_refer_all_groin_puncture_time_cz_3',
                'tby_refer_all_groin_puncture_time_cz_2': 'tby_refer_all_groin_puncture_time_cz',
                'tby_refer_lim_groin_puncture_time_cz': 'tby_refer_lim_groin_puncture_time_cz_3',
                'tby_refer_lim_groin_puncture_time_cz_2': 'tby_refer_lim_groin_puncture_time_cz',
            }, inplace=True)
            # Get only columns ending with _en
            cols = ['site_id', 'facility_name', 'label', 'oc_oid']
            cols.extend([c for c in df.columns if c.endswith('_cz')])

            df = df[cols].copy()
            # Remove _en suffix from column names
            cols = df.columns
            suffix = "_cz"
            new_cols = []
            for c in cols:
                if c.endswith(suffix):
                    new_cols.append(c[:len(c)-len(suffix)].upper())
                elif c == 'site_id':
                    new_cols.append('Protocol ID')
                elif c == "facility_name":
                    new_cols.append('Site Name')
                elif c == "label":
                    new_cols.append('Subject ID')
                elif c == "oc_oid":
                    new_cols.append('crf_parent_name')
                else:
                    new_cols.append(c)
            
            df.rename(columns=dict(zip(df.columns[0:], new_cols)),inplace=True)
            df.rename(columns={'ANTITHROMBOTICS': 'ANTITHROMBOTICS_TMP', 'GLUCOSE': 'GLUCOSE_OLD'}, inplace=True)

            # Create columns for admission time using hospital times (to keep previous calculation and setting)
            df['IVT_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_ONLY'] == 2 else None, axis=1)
            df['IVT_TBY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY'] == 2 else None, axis=1)
            df['IVT_TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY_REFER'] == 2 else None, axis=1)
            df['TBY_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_ONLY'] == 2 else None, axis=1)
            df['TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER'] == 2 else None, axis=1)
            df['TBY_REFER_ALL_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_ALL'] == 2 else None, axis=1)
            df['TBY_REFER_LIM_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_LIM'] == 2 else None, axis=1)

            # Convert antithrombotics to RES-Q v2.0
            df['ANTITHROMBOTICS_TMP'] = df.apply(lambda x: int(x['ANTITHROMBOTICS_TMP']) if 'DEVCZ10' in x['crf_parent_name'] else x['ANTITHROMBOTICS_TMP'], axis=1)
            df['ANTITHROMBOTICS'] = df.apply(lambda x: self._get_tmp_antithrombotics(x['ANTITHROMBOTICS_TMP'], x['AFIB_FLUTTER']) if 'DEVCZ10' not in x['crf_parent_name'] else x['ANTITHROMBOTICS_TMP'], axis=1)

            # Create value assessed for reabilitation
            df.loc[:, 'ASSESSED_FOR_REHAB'] = np.nan
            df.loc[df['PHYSIOTHERAPIST_EVALUATION'].isin([1,2,3]), 'ASSESSED_FOR_REHAB'] = 1
            df.loc[df['PHYSIOTHERAPIST_EVALUATION'].isin([4]), 'ASSESSED_FOR_REHAB'] = 2
            df.loc[df['PHYSIOTHERAPIST_EVALUATION'].isin([5]), 'ASSESSED_FOR_REHAB'] = 3

            # Fix glucose to be consistent (they are using . or , and sometimes also unknown)
            df['GLUCOSE'] = df.apply(
                lambda x: self.fix_glucose(x['GLUCOSE_OLD']) if x['STROKE_TYPE'] == 1 else np.nan, axis=1)

            # Rename CT_MRI column to CT_MRI_OLD and CT_TIME to CT_TIME_OLD
            df.rename(columns={'CT_MRI': 'CT_MRI_OLD', 'CT_TIME': 'CT_TIME_OLD'}, inplace=True)
            # Get ischemic patients from IVT/TBY form
            ischemic_pts = df[df['STROKE_TYPE'].isin([1])].copy()
            # Convert 7 (not performed) to 1, convert 1,2,3,4,5,6 to 2 (performed)
            ischemic_pts['CT_MRI'] = ischemic_pts.apply(lambda x: 2 if x['CT_MRI_OLD'] in [1,2,3,4,5,6] else 1, axis=1)
            # Call function to get ctmri delta is <= 60 set CT_TIME to 1 else to 2
            ischemic_pts['CT_TIME'] = ischemic_pts.apply(lambda x: self.get_ctmri_delta(x['HOSPITAL_TIME'], x['CT_TIME_OLD']) if x['CT_MRI'] == 2 else np.nan, axis=1)

            # Get other stroke than ischemic from IVT/TBY form
            other_pts = df[~df['STROKE_TYPE'].isin([1]) & ~df['CT_MRI_OLD'].isin([1,2,3,4,5,6,7])].copy()
            # Rename columns to be same as in RES-Q v2.0
            other_pts.rename(columns={'CT_MRI_OTHER': 'CT_MRI', 'CT_TIME_OTHER': 'CT_TIME'}, inplace=True)
            # Switch values for CT_MRI
            other_pts['CT_MRI'] = other_pts['CT_MRI'].replace({1: 2, 2: 1})
            # If for times were selected option 2, 3 and 4 change it to 2 (done after 1 hour)
            other_pts['CT_TIME'] = other_pts['CT_TIME'].replace({3: 2, 4: 2})

            df = ischemic_pts.append(other_pts, ignore_index=False, sort=False)

            # Set discharge date to hospital date if recanalization is set to referred to another hospital and discharge date has been hidden in this case
            df.loc[
                (df['STROKE_TYPE'] == 1) &
                (df['RECANALIZATION_PROCEDURES'].isin([5,6])),
                'DISCHARGE_DATE'
            ] = df['HOSPITAL_DATE']

            # Set discharge date to hospital date if hemicraniectomy is set to referred to another hospital and discharge date has been hidden in this case
            df.loc[
                (df['STROKE_TYPE'] == 1) & 
                (df['HEMICRANIECTOMY'] == 3), 
                'DISCHARGE_DATE'
            ] = df['HOSPITAL_DATE']

            # Fix issue with carotid stenosis in the calculation -> we haven't mapped the values from IVT/TBY to 2.0 version but we were calculating values as we did convert
            # Solution: Map values from IVT/TBY to version 2.0
            # Mapping: 1, 2 => 1; 3 => 2, 4 => 3, 5 => 4
            # Rename column to keep the old values as well in the dataframe
            df['CAROTID_STENOSIS_FOLLOWUP_OLD'] = df['CAROTID_STENOSIS_FOLLOWUP']
            df['CAROTID_STENOSIS_FOLLOWUP'] = df['CAROTID_STENOSIS_FOLLOWUP'].replace({2: 1, 3: 2, 4: 3, 5: 4})
                   


            # Create country column
            df['Country'] = 'Czech Republic'

            logging.info("Connection: Column names in IVT/TBY were changed successfully.")

            self.dict_df[name] = df

        elif 'thailand' in name:
            
            # Get only columns ending with _en
            cols = ['site_id', 'facility_name', 'label', 'door_to_groin', 'door_to_needle']
            cols.extend([c for c in df.columns if c.endswith('_cz')])

            df = df[cols].copy()
            # Remove _en suffix from column names
            cols = df.columns
            suffix = "_cz"
            new_cols = []
            for c in cols:
                if c.endswith(suffix):
                    new_cols.append(c[:len(c)-len(suffix)].upper())
                elif c == 'site_id':
                    new_cols.append('Protocol ID')
                elif c == "facility_name":
                    new_cols.append('Site Name')
                elif c == "label":
                    new_cols.append('Subject ID')
                elif c == "oc_oid":
                    new_cols.append('crf_parent_name')
                elif c == "door_to_groin" or c == "door_to_needle":
                    new_cols.append(c.upper())
                else:
                    new_cols.append(c)
            
            df.rename(columns=dict(zip(df.columns[0:], new_cols)),inplace=True)

            df.loc[df['RECANALIZATION_PROCEDURES'] == 2, 'IVT_ONLY'] = 1
            df.loc[df['IVT_ONLY'] == 1, 'IVT_ONLY_NEEDLE_TIME'] = df['DOOR_TO_NEEDLE']

            df.loc[df['RECANALIZATION_PROCEDURES'] == 3, 'IVT_TBY'] = 1
            df.loc[df['IVT_TBY'] == 1, 'IVT_TBY_NEEDLE_TIME'] = df['DOOR_TO_NEEDLE']
            df.loc[df['IVT_TBY'] == 1, 'IVT_TBY_GROIN_TIME'] = df['DOOR_TO_GROIN']

            df.loc[df['RECANALIZATION_PROCEDURES'] == 4, 'TBY_ONLY'] = 1
            df.loc[df['TBY_ONLY'] ==1, 'TBY_ONLY_GROIN_PUNCTURE_TIME'] = df['DOOR_TO_GROIN']

            df.loc[df['RECANALIZATION_PROCEDURES'] == 5, 'IVT_TBY_REFER'] = 1
            df.loc[df['IVT_TBY_REFER'] == 1, 'IVT_TBY_REFER_NEEDLE_TIME'] = df['DOOR_TO_NEEDLE']

            df['CAROTID_STENOSIS'] = pd.to_numeric(df['CAROTID_STENOSIS'])
            df['ANTIHYPERTENSIVE'] = pd.to_numeric(df['ANTIHYPERTENSIVE'])

            # Create country column
            df['Country'] = 'Thailand'

            logging.info("Connection: Column names in Thailand were changed successfully.")

            self.dict_df[name] = df

    def _get_tmp_antithrombotics(self, col_vals, afib):
        """ The function converting the value for antitrombotics from IVT/TBY form to RES-Q v2.0. 

        :param col_vals: list of values for antithrombotcs in IVT/TBY (checkboxes in the form)
        :type col_vals: list
        :param afib: seelcted value for afib
        :type afib: int
        :returns: mapped value 
        :rtype: int
        """
    
        if col_vals is not None:
            vals_str = col_vals.split(',') # Split selected values using , as seperator
            vals = list(map(int, vals_str)) # Convert string values to integers
            antiplatelets_vals = [1,2,3,4,5,6] # antiplatelets values in IVT/TBY
            anticoagulants_vals = [8,9,10,11,12,13,14] # anticoagulants values in IVT/TBY
            antiplatelets_recs = 7 # antiplatelets recommended
            anticoagulants_recs = 15 # anticoagulants recommended
            nothing = 16 # nothing

            # mapping anticoagulants
            anticoagulants_dict = {
                8: 2, # warfarin
                9: 3, # dabigatran
                10: 4, # rivaroxaban
                11: 5, # apixaban
                12: 6, # edoxaban
                13: 7, # LMWH or heparin in prophylactic dose
                14: 8, # LMWH or heparin in full anticoagulant dose
            }

            # default value (now deleted by Mirek)
            if len(vals) > 15:
                res = None
            # nothing prescribed
            elif nothing in vals:
                res = 10
            else:
                # if AFIB not detected or not know we are interested only in antiplatelets, if antiplatelets recommended value in vals set result to 9 (not prescribed, but recommended), else check if some value from antiplatelets_vals is in vals, and if yes set result to 1 (antiplatelets) else set result to 10 (nothing)
                if afib in [4,5]: 
                    if antiplatelets_recs in vals:
                        res = 9
                    else:
                        # Antiplatelets values which are in selected antithrombotics
                        x = set(antiplatelets_vals).intersection(set(vals))
                        if bool(x):
                            res = 1
                        else:
                            res = 10
                # if AFIB known or detected we are interested only in anticoagulants, if anticoagulants recommended value in vals set result to 9 (not prescribed, but recommended), else check if some value from anticoagulants_vals is in vals, and if yes map value based on anticoagulants_dict else set result to 10 (nothing)
                elif afib in [1,2,3]:
                    if anticoagulants_recs in vals:
                        res = 9
                    else:
                        # Anticoagulant values which are in selected antithrombotics
                        x = set(anticoagulants_vals).intersection(set(vals))
                        if bool(x):
                            for val in x:
                                res = anticoagulants_dict[val]
                        else:
                            x = set(antiplatelets_vals).intersection(set(vals))
                            if bool(x):
                                res = 1
                            else:
                                res = 10
                            #res = 10

            #print("Vals: {0}, Results: {1}".format(vals, res))
            return res
        else:
            return None
             
    
    def get_ctmri_delta(self, hosp_time, ct_time):
        """ The function calculating door to CT date time in minutes. 
        
        :param hosp_time: the hospitalization time
        :type hosp_time: time
        :param ct_time: the time when CT/MRI has been performed
        :type ct_time: time
        :returns: 1 if datetime > 0 and < 60, 2 if results > 60 else -2
        :rtype: int
        """
        timeformat = '%H:%M:%S'

        #print('Hosp time: {0}/{1}, CT_TIME: {2}/{3}'.format(hosp_time, type(hosp_time), ct_time, type(ct_time)))
        # print(ct_time, hosp_time)
        # Check if both time are not None if yes, return 0 else return tdelta
        if hosp_time is None or ct_time is None or pd.isnull(hosp_time) or pd.isnull(ct_time):
            tdeltaMin = 0
        elif hosp_time == 0 or ct_time == 0:
            tdeltaMin = 0
        else:
            if isinstance(ct_time, datetime.time) and isinstance(hosp_time, datetime.time):
                tdelta = datetime.datetime.combine(datetime.date.today(), ct_time) - datetime.datetime.combine(datetime.date.today(), hosp_time)
            elif isinstance(ct_time, datetime.time):
                tdelta = datetime.datetime.combine(datetime.date.today(), ct_time) - datetime.datetime.strptime(hosp_time, timeformat)
            elif isinstance(hosp_time, datetime.time):
                tdelta = datetime.datetime.strptime(ct_time, timeformat) - datetime.datetime.strptime(hosp_time, timeformat)
            else:
                tdelta = datetime.datetime.strptime(ct_time, timeformat) - datetime.datetime.strptime(hosp_time, timeformat)	
            tdeltaMin = tdelta.total_seconds()/60.0

        if tdeltaMin > 60:
            res = 2
        elif tdeltaMin <= 60 and tdeltaMin >= 0:
            res = 1
        else:
            res = -2
        return res


    def _get_countries(self, df):
        """ The function obtaining all possible countries in the dataframe. 

        :param df: the preprossed dataframe
        :type df: pandas dataframe
        :returns: the list of countries
        :rtype: list
        """

        site_ids = df['Protocol ID'].apply(lambda x: pd.Series(str(x).split("_")))
        countriesSet = set(site_ids[0])
        countriesList = list(countriesSet)

        logging.info("Data: Countries in the dataset: {0}.".format(countriesList))
        return countriesList
    
    
    def fix_glucose(self, value):
        """ The function fixing the glucose value. The issue is that users are entering glucose with comma or dot as seprator. Sometimes, also nonsense appears. 
        
        :param value: the entered value in the glucose field
        :type value: str
        :returns: fixed number
        :rtype: int
        """
        if value is None:
            return value
        elif "," in value:
            res = value.replace(',', '.')
        elif value == '-99':
            res = value
        elif '-' in value:
            res = value.replace('-', '.')
        elif '.' in value:
            res = value
        elif len(value) > 5:
            res = '-1'
        else:
            res = value

        return res

    def check_data(self, df, nprocess):
        """ The function calling the CheckData object. The dates and times are checked and if they are incorrect, they are fixed. 

        :param df: the raw dataframe 
        :type df: DataFrame
        :param nprocess: the number of processes run simulataneously
        :type nprocess: int
        :returns: the dataframe with preprocessed data
        :rtype: DataFrame
        """
        chd = CheckData(df=df, nprocess=nprocess)

        logging.info("Connection: The data were preprocessed.")

        return chd.preprocessed_data


    def prepare_atalaia_df(self, df):
        """ The function preparing the atalaia dataframe if data is equal to atalaia. The column names are renamed.
        
        :param df: the raw data exported from the database
        :type df: DataFrame
        :returns: the prepared dataframe
        :rtype: DataFrame
        """

        # Get only columns ending with _en
        cols = ['site_id', 'facility_name', 'oc_oid', 'label']
        cols.extend([c for c in df.columns if c.endswith('_es')])

        res = df[cols].copy()
        # Remove _en suffix from column names
        cols = res.columns
        suffix = "_es"
        new_cols = []
        for c in cols:
            if c.endswith(suffix):
                new_cols.append(c[:len(c)-len(suffix)].upper())
            elif c == 'site_id':
                new_cols.append('Protocol ID')
            elif c == "facility_name":
                new_cols.append('Site Name')
            elif c == "label":
                new_cols.append('Subject ID')
            elif c == "oc_oid":
                new_cols.append('crf_parent_name')
            else:
                new_cols.append(c)

        res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)
        logging.info("Connection: Column names in RESQ were changed successfully.")
        return res

    def time_diff(self, visit_date, hospital_date):
        """ The function calculating the difference in minutes between two dates. 

        :param visit_date: the last seen normal date
        :type visit_date: date
        :param hospital_date: the date of hospitalization
        :type hospital_date: date
        :returns: the difference in minutes
        :rtype: int
        """
        if (type(visit_date) is pd.Timestamp or type(visit_date) is datetime.datetime) and (type(hospital_date) is pd.Timestamp or type(hospital_date) is datetime.datetime):
            time_diff = hospital_date - visit_date
            # Convert difference to minutes
            total_minutes = time_diff.total_seconds() / 60.0
        else:
            total_minutes = 0

        if total_minutes < 0 or total_minutes > 40000:
            total_minutes = 0
        
        return total_minutes

    def fix_date(self, visit_date, hospital_date):
        """ Fix date in the case that visit year is incorrect. 
        
        :param visit_date: the visit date 
        :type visit_date: date
        :param hospital_date: the hospital date
        :type hospital_date: date
        :returns: fixed visit date
        :rtype: date
        """
        
        if visit_date is None:
            return None
        else:
            if visit_date.year > datetime.datetime.now().year:
                visit_date = hospital_date + relativedelta(year=hospital_date.year)
        
        return visit_date