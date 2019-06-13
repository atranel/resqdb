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

class Connection():
    """ A connection with one property: a section. 

    To use:
    >>> conn = Connection()
    """

    def __init__(self, nprocess=1):

        start = time.time()

        # Create log file in the working folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Connecting to datamix database!')   

        # Get absolute path
        path = os.path.dirname(__file__)
        self.database_ini = os.path.join(path, 'database.ini')


        # Set section
        datamix = 'datamix'
        # Create empty dictionary
        self.sqls = ['SELECT * from resq_mix', 'SELECT * from ivttby_mix']
        # List of dataframe names
        self.names = ['resq', 'ivttby']
        # Dictionary initialization - db dataframes
        self.dictdb_df = {}
        # Dictioanry initialization - prepared dataframes
        self.dict_df = {}

        if nprocess == 1:
            # Get dataframe from db
            df_name = self.names[0]
            self.connect(self.sqls[0], datamix, nprocess, df_name=df_name)
            
            # Get dataframe from db
            df_name = self.names[1]
            self.connect(self.sqls[1], datamix, nprocess, df_name=df_name)

            for k, v in self.dictdb_df.items():
                self.prepare_resq_df(df=v, name=k)

            self.df = pd.DataFrame()
            for i in range(0, len(self.names)):
                self.df = self.df.append(self.dict_df[self.names[i]], sort=False)
                logging.info("Connection: {0} dataframe has been appended to the resulting dataframe!".format(self.names[i]))
            # Get all country code in dataframe
            self.countries = self._get_countries(df=self.df)
            # Get preprocessed data
            self.preprocessed_data = self.check_data(df=self.df)
        else:
            threads = []
            for i in range(0, len(self.names)):
                df_name = self.names[i]
                process = Thread(target=self.connect(self.sqls[i], datamix, i, df_name=df_name))
                process.start()
                threads.append(process)

            for process in threads:
                process.join()

            treads = []
            for i in range(0, len(self.names)):
                df_name = self.names[i]
                process = Thread(target=self.prepare_df(self.dictdb_df[df_name], df_name))
                process.start()
                threads.append(process)

            for process in threads:
                process.join()

            self.df = pd.DataFrame()
            for i in range(0, len(self.names)):
                self.df = self.df.append(self.dict_df[self.names[i]], sort=False)
                logging.info("Connection: {0} dataframe has been appended to the resulting dataframe!.".format(self.names[i]))

            # Get all country code in dataframe
            self.countries = self._get_countries(df=self.df)
            # Dictionary initialization - db dataframes
            self.dfs = np.array_split(self.df, 4)
            self.pre_df = {}
            threads = []
            for i in range(0, len(self.dfs)):
                process = Thread(target=self.check_data(self.dfs[i], name=str(i)))
                process.start()
                threads.append(process)

            for process in threads:
                process.join()

            self.preprocessed_data = {}
            for k, v in self.pre_df.items():
                self.preprocessed_data = self.preprocessed_data.append(v, sort=False)
        
        end = time.time()
        tdelta = (end-start)/60
        logging.info('The conversion and merging run {0} minutes.'.format(tdelta))

    def config(self, section):
        """ Read and parse the config database file. 
        
        Raises: 
            Exception: If the section couldn't be find in the database.ini file)
        Returns: 
            The dictionary of parameters to enable connection to database.
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
        """ Connects to the database specified in the databse.ini file.
        
        Args:
            sql: The SQL query run to get dataframe from the database.
        Raises: 
            Exception: If the connection was not successful. 
        Returns: 
            The new dataframe containing data from database.
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
        """ Prepare dataframe to calculation. Convert column names etc. Return converted dataframe. """
        if 'resq' in name:
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
            df['bleeding_source_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['bleeding_source_en'], axis=1)
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

            # Get only columns ending with _en
            cols = ['site_id', 'facility_name', 'oc_oid', 'subject_id']
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
                elif c == "subject_id":
                    new_cols.append('Subject ID')
                elif c == "oc_oid":
                    new_cols.append('crf_parent_name')
                else:
                    new_cols.append(c)

            res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)
            logging.info("Connection: Column names in RESQ were changed successfully.")

            self.dict_df[name] = res

        elif 'ivttby' in name:

            # Get patients inserted in IVT_TBY_DEV
            ivttby_dev = df[df['oc_oid'].str.contains('')]
            # Get only columns ending with _en
            cols = ['site_id', 'facility_name', 'subject_id', 'oc_oid']
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
                elif c == "subject_id":
                    new_cols.append('Subject ID')
                elif c == "oc_oid":
                    new_cols.append('crf_parent_name')
                else:
                    new_cols.append(c)
            df.rename(columns=dict(zip(df.columns[0:], new_cols)),inplace=True)
            df.rename(columns={'ANTITHROMBOTICS': 'ANTITHROMBOTICS_TMP'}, inplace=True)

            df['IVT_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_ONLY'] == 2 else None, axis=1)
            df['IVT_TBY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY'] == 2 else None, axis=1)
            df['IVT_TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY_REFER'] == 2 else None, axis=1)
            df['TBY_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_ONLY'] == 2 else None, axis=1)
            df['TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER'] == 2 else None, axis=1)
            df['TBY_REFER_ALL_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_ALL'] == 2 else None, axis=1)
            df['TBY_REFER_LIM_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_LIM'] == 2 else None, axis=1)

            # Convert antithrombotics to RES-Q v2.0
            df['ANTITHROMBOTICS'] = df.apply(lambda x: self._get_tmp_antithrombotics(x['ANTITHROMBOTICS_TMP']) if 'DEVCZ10' not in x['crf_parent_name'] else x['ANTITHROMBOTICS_TMP'], axis=1)

            logging.info("Connection: Column names in IVT/TBY were changed successfully.")

            self.dict_df[name] = df


    def prepare_resq_df(self, df):
        """ Change column names. """
             
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
        df['bleeding_source_en'] = df.apply(lambda x: -999 if "RESQV12" in x['oc_oid'] else x['bleeding_source_en'], axis=1)
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

        # Get only columns ending with _en
        cols = ['site_id', 'facility_name', 'oc_oid', 'subject_id']
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
            elif c == "subject_id":
                new_cols.append('Subject ID')
            elif c == "oc_oid":
                new_cols.append('crf_parent_name')
            else:
                new_cols.append(c)

        res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)
        logging.info("Connection: Column names in RESQ were changed successfully.")

        return res
    
    
    def prepare_ivt_tby_df(self, df):
        """ Get column names in the form of RESQv2.0. """
        # Get patients inserted in IVT_TBY_DEV
        ivttby_dev = df[df['oc_oid'].str.contains('')]
        # Get only columns ending with _en
        cols = ['site_id', 'facility_name', 'subject_id', 'oc_oid']
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
            elif c == "subject_id":
                new_cols.append('Subject ID')
            elif c == "oc_oid":
                new_cols.append('crf_parent_name')
            else:
                new_cols.append(c)
        df.rename(columns=dict(zip(df.columns[0:], new_cols)),inplace=True)
        df.rename(columns={'ANTITHROMBOTICS': 'ANTITHROMBOTICS_TMP'}, inplace=True)

        df['IVT_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_ONLY'] == 2 else None, axis=1)
        df['IVT_TBY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY'] == 2 else None, axis=1)
        df['IVT_TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['IVT_TBY_REFER'] == 2 else None, axis=1)
        df['TBY_ONLY_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_ONLY'] == 2 else None, axis=1)
        df['TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER'] == 2 else None, axis=1)
        df['TBY_REFER_ALL_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_ALL'] == 2 else None, axis=1)
        df['TBY_REFER_LIM_ADMISSION_TIME'] = df.apply(lambda x: x['HOSPITAL_TIME'] if x['TBY_REFER_LIM'] == 2 else None, axis=1)

        # Convert antithrombotics to RES-Q v2.0
        df['ANTITHROMBOTICS'] = df.apply(lambda x: self._get_tmp_antithrombotics(x['ANTITHROMBOTICS_TMP']) if 'DEVCZ10' not in x['crf_parent_name'] else x['ANTITHROMBOTICS_TMP'], axis=1)

        logging.info("Connection: Column names in IVT/TBY were changed successfully.")

        return df

    def _get_tmp_antithrombotics(self, col_vals):
        vals = col_vals.split(',')
        antiplatelets_vals = [1,2,3,4,5,6]
        anticoagulants_vals = [8,9,10,11,12,13,14]
        antiplatelets_recs = 7
        anticoagulants_recs = 15
        nothing = 16

        if len(vals) > 15:
            res = None
        else:
            for val in vals:
                
                if int(val) in antiplatelets_vals and int(val) != antiplatelets_recs:
                    res = 1
                elif int(val) in anticoagulants_vals and int(val) != anticoagulants_recs:
                    if int(val) == 8: 
                        res = 2
                    elif int(val) == 9:
                        res = 3
                    elif int(val) == 10:
                        res = 4
                    elif int(val) == 11:
                        res = 5
                    elif int(val) == 12:
                        res = 6
                    elif int(val) == 13:
                        res = 7 
                    elif int(val) == 14:
                        res = 8
                elif int(val) == anticoagulants_recs or int(val) == antiplatelets_recs:
                    res = 9
                elif int(val) == nothing:
                    res = 10
            
        return res
             
    
    def _get_countries(self, df):
        """Return list of countries present in the dataframe.

        Args:
            df: The raw dataframe
        Returns:
            The list of country codes present in the dataframe.
        """
        site_ids = df['Protocol ID'].apply(lambda x: pd.Series(str(x).split("_")))
        countriesSet = set(site_ids[0])
        countriesList = list(countriesSet)

        logging.info("Data: Countries in the dataset: {0}.".format(countriesList))
        return countriesList
    
    
    def check_data(self, df, name=None):
        """ Check dates and times and fix according to algorithm.
        
        Args: 
            df: The raw dataframe with fixed columns
        Returns: 
            The preprocessed data.
        """

        chd = CheckData(df=df)

        logging.info("Connection: The data were preprocessed.")

        if name is None:
            return chd.get_preprocessed_data()
        else:
            self.pre_df[name] = chd.get_preprocessed_data()

