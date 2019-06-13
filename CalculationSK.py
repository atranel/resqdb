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
from datetime import datetime, time, date
import time
import sqlite3
from numpy import inf
import pytz
import xlsxwriter

class Connection:
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
        #self.sqls = ['SELECT * from slovakia']
        self.sqls = ['SELECT * from slovakia', 'SELECT * from slovakia_2018']
        # List of dataframe names
        #self.names = ['slovakia']
        self.names = ['slovakia', 'slovakia_2018']
        # Dictionary initialization - db dataframes
        self.dictdb_df = {}
        # Dictioanry initialization - prepared dataframes
        self.dict_df = {}

        df_name = self.names[0]
        self.connect(self.sqls[0], datamix, nprocess, df_name=df_name)

        df_name = self.names[1]
        self.connect(self.sqls[1], datamix, nprocess, df_name=df_name)
        
        for k, v in self.dictdb_df.items():
            self.prepare_df(df=v, name=k)

        self.df = pd.DataFrame()
        for i in range(0, len(self.names)):
            #print(self.dict_df[self.names[i]])
            self.df = self.df.append(self.dict_df[self.names[i]], sort=False)
            logging.info("Connection: {0} dataframe has been appended to the resulting dataframe!".format(self.names[i]))

        # Get all country code in dataframe
        self.countries = self._get_countries(df=self.df)
        # Get preprocessed data
        # self.preprocessed_data = self.check_data(df=self.df)

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


    def _calculate_time(self, ct_date, hospital_date, rec_date, used_col=None):
        """ Calculate needle time or groin time. """
        
        rec_time = rec_date - hospital_date
        tdeltamin = rec_time.total_seconds()/60.0
        col = 'HOSPITAL_DATE'

        if used_col is None:
            if tdeltamin <= 1:
                try:
                    if hospital_date.strftime('%Y-%m-%d') > rec_date.strftime('%Y-%m-%d'):
                        rec_time = rec_date - ct_date
                        tdeltamin = rec_time.total_seconds()/60.0
                        col = 'CT_TIME'
                    else:
                        if hospital_date.strftime('%Y-%m-%d') == rec_date.strftime('%Y-%m-%d'):
                            rec_time = rec_date - hospital_date
                            tdeltamin = rec_time.total_seconds()/60.0
                            col = 'HOSPITAL_DATE'
                            if tdeltamin <= 1:
                                rec_time = rec_date - ct_date
                                tdeltamin = rec_time.total_seconds()/60.0
                                col = 'CT_TIME'
                except ValueError:
                    return None
            elif tdeltamin > 1 and tdeltamin <= 10:
                hosp_time = rec_date - hospital_date
                hosp_time_mins = hosp_time.total_seconds()/60.0        
                rec_time = rec_date - ct_date
                tdeltamin = rec_time.total_seconds()/60.0
                col = 'CT_TIME'
                if hosp_time_mins > tdeltamin:
                    tdeltamin = hosp_time_mins
                    col = 'HOSPITAL_DATE'
        else:
            if used_col == 'HOSPITAL_DATE':
                rec_time = rec_date - hospital_date
                tdeltamin = rec_time.total_seconds()/60.0
            elif used_col == 'CT_TIME':
                rec_time = rec_date - ct_date
                tdeltamin = rec_time.total_seconds()/60.0

        return tdeltamin, used_col

    def _calculate_ct_time(self, hospital_date, ct_date):
        """ Calculate CT/TIME and return 1 if CT under 1 hour else return 2. """

        ct_diff = ct_date - hospital_date
        tdeltamin = ct_diff.total_seconds()/60.0

        if tdeltamin < 0 or tdeltamin > 60:
            return 2
        else:
            return 1
    
    def prepare_df(self, df, name):
        """ Rename column names and map values. """

        if name == 'slovakia':
            res = df.copy()
            # Remove _en suffix from column names
            cols = res.columns

            new_cols = []
            for c in cols:
                if c == 'anonym':
                    new_cols.append("Protocol ID")
                elif c == 'subject_id':
                    new_cols.append("Subject ID")
                else:
                    new_cols.append(c.upper())

            res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)


            res['NEEDLE_TIME_MIN'], res['USED_COL'] = zip(*res.apply(lambda x: self._calculate_time(x['CT_TIME'], x['HOSPITAL_DATE'], x['NEEDLE_TIME']) if x['NEEDLE_TIME'].date else (np.nan, None), axis=1))

            res['GROIN_TIME_MIN'], res['USED_COL'] = zip(*res.apply(lambda x: self._calculate_time(x['CT_TIME'], x['HOSPITAL_DATE'], x['GROIN_TIME'], x['USED_COL']) if x['GROIN_TIME'].date else (np.nan, None), axis=1))

            res['CT_TIME_WITHIN'] = res.apply(lambda x: self._calculate_ct_time(x['HOSPITAL_DATE'], x['CT_TIME']) if x['CT_MRI'] == 2 else np.nan, axis=1)

            res.drop(['USED_COL'], inplace=True, axis=1)
            
            #res['CRF'] = 'SLOVAKIA_2019'

            res.rename(columns={'DOOR_TO_NEEDLE': 'DOOR_TO_NEEDLE_OLD', 'NEEDLE_TIME_MIN': 'DOOR_TO_NEEDLE', 'DOOR_TO_GROIN': 'DOOR_TO_GROIN_OLD', 'GROIN_TIME_MIN': 'DOOR_TO_GROIN', 'CT_TIME': 'CT_DATE', 'CT_TIME_WITHIN': 'CT_TIME'}, inplace=True)

            logging.info("Connection: Column names in Slovakia were changed successfully.")

            self.dict_df[name] = res

        elif name == 'slovakia_2018':

            res = df.copy()
            # Remove _en suffix from column names
            cols = res.columns

            new_cols = []
            for c in cols:
                if c == 'anonym':
                    new_cols.append("Protocol ID")
                elif c == 'subject_id':
                    new_cols.append("Subject ID")
                else:
                    new_cols.append(c.upper())

            res.rename(columns=dict(zip(res.columns[0:], new_cols)), inplace=True)

            #print(list(res.columns))

            res['NEEDLE_TIME_MIN'], res['USED_COL'] = zip(*res.apply(lambda x: self._calculate_time(x['CT_TIME'], x['HOSPITAL_DATE'], x['NEEDLE_TIME']) if x['NEEDLE_TIME'].date else (np.nan, None), axis=1))

            res['GROIN_TIME_MIN'], res['USED_COL'] = zip(*res.apply(lambda x: self._calculate_time(x['CT_TIME'], x['HOSPITAL_DATE'], x['GROIN_TIME'], x['USED_COL']) if x['GROIN_TIME'].date else (np.nan, None), axis=1))

            res['CT_TIME_WITHIN'] = res.apply(lambda x: self._calculate_ct_time(x['HOSPITAL_DATE'], x['CT_TIME']) if x['CT_MRI'] == 2 else np.nan, axis=1)

            res.drop(['USED_COL'], inplace=True, axis=1)
            
            #res['CRF'] = 'SLOVAKIA_2019'

            res.rename(columns={'DOOR_TO_NEEDLE': 'DOOR_TO_NEEDLE_OLD', 'NEEDLE_TIME_MIN': 'DOOR_TO_NEEDLE', 'DOOR_TO_GROIN': 'DOOR_TO_GROIN_OLD', 'GROIN_TIME_MIN': 'DOOR_TO_GROIN', 'CT_TIME': 'CT_DATE', 'CT_TIME_WITHIN': 'CT_TIME'}, inplace=True)


            #res['CRF'] = 'SLOVAKIA_2018'

            logging.info("Connection: Column names in Slovakia_2018 were changed successfully.")

            self.dict_df[name] = res

    
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
            

class FilterDataset:
    """ Filter dataset by country and the time period. 

    Args:
        df: The preprocessed dataframe
        country: The country code of country which should be included in the resulted calculation (default: None)
        date1: The first daate included in the filtered dataframe (default: None)
        date2: The last date included in the filtered dataframe (default: None)
    Returns:
        The filtered dataframe. 
    """

    def __init__(self, df, country=None, date1=None, date2=None):

        # Create log file in the working folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
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

            self.fdf = self._filter_by_country(country=country)
            logging.info('FilterDataset: Data have been filtered for country {0}!'.format(self.country)) 
        
        if self.date1 is not None and self.date2 is not None:

            self.fdf = self._filter_by_date()
            logging.info('FilterDataset: Data have been filtered for date {0} - {1}!'.format(self.date1, self.date2))
        
        
    def _filter_by_country(self, country):
        """ Return dataframe containing only rows for the given country. 
        
        Args:
            country: The changed country code.
        Returns:
            The filtered dataframe. 
        """
        df = self.fdf[self.fdf['Protocol ID'].str.startswith(self.country) == True]

        return df

    def _filter_by_date(self):
        """ Return dataframe containing only rows where discharge date is between these two dates. 

        Args: 
            date1: The first date included in the filtered dataframe
            date2: The last date included in the filtered dataframe
        Returns: 
            The filtered dataframe
        """
        
        df = self.fdf[(self.fdf['DISCHARGE_DATE'] >= self.date1) & (self.fdf['DISCHARGE_DATE'] <= self.date2)]

        return df


class GeneratePreprocessedData:
    """
    Generate preprocessed data as table. One sheet are preprocessed data, 2nd sheet are legend data.    
    """

    def __init__(self, df, split_sites=False, site=None, report=None, quarter=None, country_code=None):
        """Create object of preprocessed data. 

        Args:
            df: preprocessed data in datafraem
            split_sites: True if preprocessed data has to be generated per site (default: False)
            site: Protocol ID of site (default: None)
            country_code: country code (default: None)
            report: type of the report, eg. quarter (default: None)
            period: type of the period, eg. H1_2018 (default: None)
        """
        # Create log file in the working folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.df = df
        self.split_sites = split_sites
        self.report = report
        self.quarter = quarter
        self.country_code = country_code

        # If Site is not None, filter dataset according to site code
        if site is not None:
            df = self.df[self.df['Protocol ID'].str.contains(site) == True]
            # Generate preprocessed data for site
            self._generate_preprocessed_data(df=df, site_code=site)
            logging.info('Preprocessed data: The preprocessed data were generated for site {0}'.format(site))
        
        # Generate formatted statistics per site + country as site is included
        if (split_sites) and site is None:
            logging.info('Preprocessed data: Generate preprocessed data per site.')
            # Get set of all site ids
            site_ids = set(self.df['Protocol ID'].tolist())
            #site_ids = set(site_ids)  
            for i in site_ids:
                df = self.df[self.df['Protocol ID'].str.contains(i) == True]
                self._generate_preprocessed_data(df=df, site_code=i)
                logging.info('Preprocessed data: The preprocessed data were generated for site {0}'.format(site))

        self._generate_preprocessed_data(self.df, site_code=None)
        logging.info('Preprocessed data: The preprocessed data were generate for all data.')

    def _generate_preprocessed_data(self, df, site_code):
        """
        Generate preprocessed data with legend. 

        Keyword arguments: 
            df = filtered dataframe
            site_code = code of site included in the dataframe
        """
        
        if site_code is not None:
            output_file = self.report + "_" + site_code + "_" + self.quarter + "_preprocessed_data.xlsx"
        else:
            output_file = self.report + "_" + self.country_code + "_" + self.quarter + "_preprocessed_data.xlsx"
        
        #output_file = "preprocessed_data.xlsx"
        
        df = df.copy()
        
        # Convert dates to strings
        dateformat = "%Y-%m-%d"
        timestamp = "%Y-%m-%d %H:%M"
        timeformat = "%H:%M"
        def convert_to_string(datetime, format):
            if datetime is None or datetime is np.nan:
                return datetime
            else:
                return datetime.strftime(format)
        
        
       # if df['VISIT_DATE'].dtype != np.object:
        df['VISIT_DATE'] = df.apply(lambda x: convert_to_string(x['VISIT_DATE'], dateformat), axis=1)
       # if df['VISIT_DATE_OLD'].dtype != np.object:
        #if df['VISIT_TIME'].dtype != np.object:
        df['VISIT_TIME'] = df.apply(lambda x: convert_to_string(x['VISIT_TIME'], timeformat), axis=1)
        df['HOSPITAL_DATE'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_DATE'], timestamp), axis=1)
        #if df['HOSPITAL_TIME'].dtype != np.object:
       # df['HOSPITAL_TIME'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_TIME'], timeformat), axis=1)
        df['DISCHARGE_DATE'] = df.apply(lambda x: convert_to_string(x['DISCHARGE_DATE'], timestamp), axis=1)
        
        

        # Replace NAN values with 0
        df.fillna(value="", inplace=True)

        # Create workbook
        workbook = xlsxwriter.Workbook(output_file)
        logging.info('Preprocessed data: The workbook was created.')
        # Create worksheets
        preprocessed_data_sheet = workbook.add_worksheet('Preprocessed_raw_data')
        legend_sheet = workbook.add_worksheet('Legend_v2.0')
        additional_desc_sheet = workbook.add_worksheet('Additional_description')

        ### PREPROCESSED DATA
        preprocessed_data = df.values.tolist()
        # Set width of columns
        preprocessed_data_sheet.set_column(0, 150, 30)
        # Number of columns/rows
        ncol = len(df.columns) - 1
        nrow = len(df)

        # Create header
        col = []
        for j in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = df.columns.tolist()[j]
            col.append(tmp)

        options = {'data': preprocessed_data,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 1'
                   }
        preprocessed_data_sheet.add_table(0, 0, nrow, ncol, options)
        logging.info('Preprocessed data: The sheet "Preprocessed data" was added.')
    
        workbook.close()


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
        def get_country_name(value):
            if value == "UZB":
                value = 'UZ'
            country_name = pytz.country_names[value]
            return country_name

        if comparison == False:
            self.df['Protocol ID'] = self.df.apply(lambda row: row['Protocol ID'].split()[2] if (len(row['Protocol ID'].split()) == 3) else row['Protocol ID'].split()[0], axis=1)
            # uncomment if you want stats between countries and set comparison == True
            #self.df['Protocol ID'] = self.df.apply(lambda x: x['Protocol ID'].split("_")[0], axis=1)
        #print(set(list(self.df['Protocol ID'])))

        # If you want to compare, instead of Site Names will be Country names. 
        if comparison:
            if self.df['Protocol ID'].dtype == np.object:
                self.df['Site Name'] = self.df.apply(lambda x: get_country_name(x['Protocol ID']) if get_country_name(x['Protocol ID']) != "" else x['Protocol ID'], axis=1)
        
        
        if (country):
            country = self.df.copy()
            self.country_name = pytz.country_names[country_code]
            country['Protocol ID'] = self.country_name
            country['Site Name'] = self.country_name
            self.df = pd.concat([self.df, country])
        else:
            self.country_name = ""
        
        if comparison == False:
            # Get Protocol IDs and Total Patients 
            self.statsDf = self.df.groupby(['Protocol ID']).size().reset_index(name="Total Patients")
            self.statsDf['Site Name'] = ""
            self.statsDf = self.statsDf[['Protocol ID', 'Site Name', 'Total Patients']]
        else:
            # Get Protocol IDs and Total Patients 
            self.statsDf = self.df.groupby(['Protocol ID', 'Site Name']).size().reset_index(name="Total Patients")

        # Median age
        self.statsDf['Median patient age'] = self.df.groupby(['Protocol ID']).AGE.agg(['median']).rename(columns={'median': 'Median patient age'})['Median patient age'].tolist()


        self.df.drop(['ANTITHROMBOTICS'], inplace=True, axis=1)

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
        discharge_subset = self.df[~self.df['RECANALIZATION_PROCEDURES'].isin([5, 6])]
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

        #########
        # NIHSS #
        #########
        # Get Median of NIHSS score
        tmpDf = is_ich_cvt.groupby(['Protocol ID']).NIHSS_SCORE.agg(['median']).rename(columns={'median': 'NIHSS median score'})
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
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=3, new_column_name='# CT/MRI - In other hospital')

        # Get % CT/MRI Not performed	
        self.statsDf['% CT/MRI - In other hospital'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - In other hospital']/x['is_ich_tia_cvt_patients']) * 100), 2) if x['is_ich_tia_cvt_patients'] > 0 else 0, axis=1)        

        # Get patients CT/MRI performed
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=2, new_column_name='# CT/MRI - performed')

        # Get % CT/MRI performed	
        self.statsDf['% CT/MRI - performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - performed']/(x['is_ich_tia_cvt_patients'] - x['# CT/MRI - In other hospital'])) * 100), 2) if (x['is_ich_tia_cvt_patients'] - x['# CT/MRI - In other hospital']) > 0 else 0, axis=1)

        # Get patients CT/MRI not known
        self.statsDf = self._get_values_for_factors(column_name="CT_MRI", value=1, new_column_name='# CT/MRI - Not performed')

        # Get % CT/MRI not known	
        self.statsDf['% CT/MRI - Not performed'] = self.statsDf.apply(lambda x: round(((x['# CT/MRI - Not performed']/(x['is_ich_tia_cvt_patients'] - x['# CT/MRI - In other hospital'])) * 100), 2) if (x['is_ich_tia_cvt_patients'] - x['# CT/MRI - In other hospital']) > 0 else 0, axis=1)

        
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
        

        #############################
        # RECANALIZATION PROCEDURES #
        #############################
        needle = isch.loc[(isch['DOOR_TO_NEEDLE'] < 0) | (isch['DOOR_TO_NEEDLE'] > 400)].copy()
        groin = isch.loc[(isch['DOOR_TO_NEEDLE'] == 0) & ((isch['DOOR_TO_GROIN'] < 0) | (isch['DOOR_TO_GROIN'] > 700))].copy()
        #indexes = isch.loc[((isch['DOOR_TO_NEEDLE'] < 0) & (isch['DOOR_TO_NEEDLE'] > 400)) | ((isch['DOOR_TO_NEEDLE'] == 0) & (isch['DOOR_TO_GROIN'] < 0) & (isch['DOOR_TO_GROIN'] > 700))].copy()
        number_of_patients = len(needle.index.values) + len(groin.index.values)
        #print('Number of patients removed: {0}.'.format(number_of_patients))
        recan_tmp = isch.drop(needle.index.values)
        recan_tmp.drop(groin.index.values, inplace=True)
        recan_tmp.to_csv('recan_tmp.csv', sep=',')
        self.tmp = recan_tmp.groupby(['Protocol ID', 'RECANALIZATION_PROCEDURES']).size().to_frame('count').reset_index()

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
       # recanalization_procedure_iv_tpa = isch.loc[(isch['RECANALIZATION_PROCEDURES'].isin([2, 3, 5])) & (isch['DOOR_TO_NEEDLE'] > 0) & (isch['DOOR_TO_NEEDLE'] <= 400)]
        recanalization_procedure_iv_tpa = isch[isch['RECANALIZATION_PROCEDURES'].isin([2, 3, 5])].copy()

        # Replace NA values by 0
        recanalization_procedure_iv_tpa.fillna(0, inplace=True)
        recanalization_procedure_iv_tpa['IVTPA'] = recanalization_procedure_iv_tpa['DOOR_TO_NEEDLE']
        #print(recanalization_procedure_iv_tpa['IVTPA'].tolist())

        tmp = recanalization_procedure_iv_tpa.groupby(['Protocol ID']).IVTPA.agg(['median']).rename(columns={'median': 'Median DTN (minutes)'}).reset_index()
        #print(tmp)
        self.statsDf = self.statsDf.merge(tmp, how='outer')
        self.statsDf.fillna(0, inplace=True)

        #self.statsDf = self.statsDf.merge(interval_vals_df, how='outer')
        
        ##############
        # MEDIAN DTG #
        ##############
        # Get patients receiving TBY
        self.statsDf.loc[:, '# TBY'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + endovascular treatment'], axis=1)

        # Get patients receiving TBY %
        self.statsDf['% TBY'] = self.statsDf.apply(lambda x: round(((x['# TBY']/x['isch_patients']) * 100), 2) if x['isch_patients'] > 0 else 0, axis=1)

        
        # Get only patients recanalized TBY
        recanalization_procedure_tby_dtg = isch[isch['RECANALIZATION_PROCEDURES'].isin([4, 3])].copy()
        #recanalization_procedure_tby_dtg = isch.loc[(isch['RECANALIZATION_PROCEDURES'].isin([4, 3])) & (isch['DOOR_TO_GROIN'] > 0) & (isch['DOOR_TO_GROIN'] <= 700)]
        recanalization_procedure_tby_dtg.fillna(0, inplace=True)

        # Get IVTPA in minutes
        recanalization_procedure_tby_dtg['TBY'] = recanalization_procedure_tby_dtg['DOOR_TO_GROIN']

        tmp = recanalization_procedure_tby_dtg.groupby(['Protocol ID']).TBY.agg(['median']).rename(columns={'median': 'Median DTG (minutes)'}).reset_index()
        #print(tmp)
        self.statsDf = self.statsDf.merge(tmp, how='outer')
        self.statsDf.fillna(0, inplace=True)

        #######################
        # DYPSHAGIA SCREENING #
        #######################
        self.tmp = is_ich_cvt.groupby(['Protocol ID', 'DYSPHAGIA_SCREENING']).size().to_frame('count').reset_index()

        # Get patients dysphagia screening - not known
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=6, new_column_name='# dysphagia screening - not known')

        # Get % patients dysphagia screening - not known
        self.statsDf['% dysphagia screening - not known'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - not known']/x['is_ich_cvt_patients']) * 100), 2) if x['is_ich_cvt_patients'] > 0 else 0, axis=1)

        # Get patients dysphagia screening - Guss test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=1, new_column_name='# dysphagia screening - Guss test')

        # Get % patients dysphagia screening - Guss test
        self.statsDf['% dysphagia screening - Guss test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Guss test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Other test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=2, new_column_name='# dysphagia screening - Other test')

        # Get % patients dysphagia screening - Other test
        self.statsDf['% dysphagia screening - Other test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Other test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Another centre
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=3, new_column_name='# dysphagia screening - Another centre')

        # Get % patients dysphagia screening - Another centre
        self.statsDf['% dysphagia screening - Another centre'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Another centre']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Not done
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=4, new_column_name='# dysphagia screening - Not done')

        # Get % patients dysphagia screening - Not done
        self.statsDf['% dysphagia screening - Not done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Not done']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        # Get patients dysphagia screening - Unable to test
        self.statsDf = self._get_values_for_factors(column_name="DYSPHAGIA_SCREENING", value=5, new_column_name='# dysphagia screening - Unable to test')

        # Get % patients dysphagia screening - Unable to test
        self.statsDf['% dysphagia screening - Unable to test'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening - Unable to test']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

        self.statsDf['# dysphagia screening done'] = self.statsDf['# dysphagia screening - Guss test'] + self.statsDf['# dysphagia screening - Other test'] + self.statsDf['# dysphagia screening - Another centre']
        self.statsDf['% dysphagia screening done'] = self.statsDf.apply(lambda x: round(((x['# dysphagia screening done']/(x['is_ich_cvt_patients'] - x['# dysphagia screening - not known'])) * 100), 2) if (x['is_ich_cvt_patients'] - x['# dysphagia screening - not known']) > 0 else 0, axis=1)

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

       
        ###############################
        # ANTITHROMBOTICS WITHOUT CVT #
        ###############################
        # Get patients with prescribed antithrombotics
        def get_antithrombotics(vals):
            set_vals = list(set(vals))
            
            if len(set_vals) == 1:
                if set_vals[0] == 2:
                    res = 2
                elif set_vals[0] == 0:
                    res = None
                else:
                    res = 1
            else:
                res = 1      

           # print(set_vals, res)
            return res


       # is_tia['ANTITHROMBOTICS'] = is_tia.apply(lambda x:  (x['UKON_WARFARIN'] == 2 and x['UKON_DABIGATRAN'] == 2 and x['UKON_RIVAROXABAN'] == 2 and x['UKON_APIXABAN'] == 2  and x['UKON_EDOXABAN'] == 2 and x['UKON_LMW'] == 2 and x['UKON_ANTIKOAGULANCIA'] == 2  and x['UKON_HEPARIN_VTE'] == 2 and x['UKON_ASA'] == 2 and x['UKON_CLOPIDOGREL'] == 2) else 1, axis=1)

        is_tia.loc[:, 'ANTITHROMBOTICS'] = is_tia.apply(lambda x: get_antithrombotics([x['UKON_WARFARIN'], x['UKON_DABIGATRAN'], x['UKON_RIVAROXABAN'], x['UKON_APIXABAN'], x['UKON_EDOXABAN'], x['UKON_LMW'],x['UKON_ANTIKOAGULANCIA'], x['UKON_HEPARIN_VTE'], x['UKON_ASA'], x['UKON_CLOPIDOGREL']]), axis=1)

        #print(is_tia['ANTITHROMBOTICS'])

        # filter not dead patient with ischemic and transient CMP
        antithrombotics = is_tia[~is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        # calculate antithrombotics df patients
        self.statsDf['antithrombotics_patients'] = self._count_patients(dataframe=antithrombotics)
        # Filter dead patients with ischemic and transient CMP
        ischemic_transient_dead = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([5])].copy()
        # Count patients
        self.statsDf['ischemic_transient_dead_patients'] = self._count_patients(dataframe=ischemic_transient_dead)

        ischemic_transient_dead_prescribed = is_tia[is_tia['DISCHARGE_DESTINATION'].isin([5]) & is_tia['ANTITHROMBOTICS'].isin([1])].copy()
        self.statsDf['ischemic_transient_dead_patients_prescribed'] = self._count_patients(dataframe=ischemic_transient_dead_prescribed)
        

        # Calculate antiplatelets (ASA and clopidogrel)
        antithrombotics['ANTIPLATELETS'] = antithrombotics.apply(lambda x: 2 if x['UKON_ASA'] == 2 and x['UKON_CLOPIDOGREL'] == 2 else 1, axis=1)
        self.tmp = antithrombotics.groupby(['Protocol ID', 'ANTIPLATELETS']).size().to_frame('count').reset_index()

        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTIPLATELETS", value=1, new_column_name='# patients receiving antiplatelets')

        # Get % patients receiving antiplatelets
        self.statsDf['% patients receiving antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# patients receiving antiplatelets']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)


        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_WARFARIN']).size().to_frame('count').reset_index()
        # Get patients receiving Vit. K antagonist
        self.statsDf = self._get_values_for_factors(column_name="UKON_WARFARIN", value=1, new_column_name='# patients receiving Vit. K antagonist')

        # Get % patients receiving Vit. K antagonist
        #self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_DABIGATRAN']).size().to_frame('count').reset_index()
        # Get patients receiving dabigatran
        self.statsDf = self._get_values_for_factors(column_name="UKON_DABIGATRAN", value=1, new_column_name='# patients receiving dabigatran')

        # Get % patients receiving dabigatran
        #self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_RIVAROXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving rivaroxaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_RIVAROXABAN", value=1, new_column_name='# patients receiving rivaroxaban')

        # Get % patients receiving rivaroxaban
        #self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_APIXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving apixaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_APIXABAN", value=1, new_column_name='# patients receiving apixaban')

        # Get % patients receiving apixaban
        #self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_EDOXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving edoxaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_EDOXABAN", value=1, new_column_name='# patients receiving edoxaban')

        # Get % patients receiving edoxaban
        #self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_HEPARIN_VTE']).size().to_frame('count').reset_index()
        # Get patients receiving LMWH or heparin in prophylactic dose
        self.statsDf = self._get_values_for_factors(column_name="UKON_HEPARIN_VTE", value=1, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')

        # Get % patients receiving LMWH or heparin in prophylactic dose
        #self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        antithrombotics['UKON_LMW_ANTICOAGULACNI'] = antithrombotics.apply(lambda x: 2 if x['UKON_LMW'] == 2 and x['UKON_ANTIKOAGULANCIA'] == 2 else 1, axis=1)
        self.tmp = antithrombotics.groupby(['Protocol ID', 'UKON_LMW_ANTICOAGULACNI']).size().to_frame('count').reset_index()
        # Get patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf = self._get_values_for_factors(column_name="UKON_LMW_ANTICOAGULACNI", value=1, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')

        # Get % patients receiving LMWH or heparin in full anticoagulant dose
        #self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)
        
        # Get patients not prescribed antithrombotics, but recommended
        self.statsDf['# patients not prescribed antithrombotics, but recommended'] = 0
        #self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=9, new_column_name='# patients not prescribed antithrombotics, but recommended')

        # Get % patients not prescribed antithrombotics, but recommended
        self.statsDf['% patients not prescribed antithrombotics, but recommended'] = self.statsDf.apply(lambda x: round(((x['# patients not prescribed antithrombotics, but recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        # Get patients neither receiving antithrombotics nor recommended
        self.statsDf['# patients neither receiving antithrombotics nor recommended'] = 0
        #self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=10, new_column_name='# patients neither receiving antithrombotics nor recommended')

        # Get % patients neither receiving antithrombotics nor recommended
        self.statsDf['% patients neither receiving antithrombotics nor recommended'] = self.statsDf.apply(lambda x: round(((x['# patients neither receiving antithrombotics nor recommended']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        ## ANTITHROMBOTICS - PATIENTS PRESCRIBED + RECOMMENDED
        # patients prescribed antithrombotics
        #self.statsDf.loc[:, '# patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: x['# patients receiving antiplatelets'] + x['# patients receiving Vit. K antagonist'] + x['# patients receiving dabigatran'] + x['# patients receiving rivaroxaban'] + x['# patients receiving apixaban'] + x['# patients receiving edoxaban'] + x['# patients receiving LMWH or heparin in prophylactic dose'] + x['# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1)
        self.tmp = antithrombotics.groupby(['Protocol ID', 'ANTITHROMBOTICS']).size().to_frame('count').reset_index()

        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed antithrombotics')


        # Get % patients prescribed antithrombotics
        #self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100), 2) if (x['is_tia_cvt_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0 else 0, axis=1)
        self.statsDf['% patients prescribed antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics']/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100), 2) if (x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0 else 0, axis=1)

        
        #  patients prescribed or recommended antithrombotics
        self.statsDf = self._get_values_for_factors(column_name="ANTITHROMBOTICS", value=1, new_column_name='# patients prescribed or recommended antithrombotics')

        # Get % patients prescribed or recommended antithrombotics
        # From patients prescribed or recommended antithrombotics remove patient who had prescribed antithrombotics and were dead (nominator)
        #self.statsDf['% patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics'] - x['ischemic_transient_dead_patients_prescribed'])/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended'])) * 100, 2) if ((x['is_tia_patients'] - x['ischemic_transient_dead_patients'] - x['# patients not prescribed antithrombotics, but recommended']) > 0) else 0, axis=1)
        self.statsDf['% patients prescribed or recommended antithrombotics'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed or recommended antithrombotics'] - x['ischemic_transient_dead_patients_prescribed'])/(x['is_tia_patients'] - x['ischemic_transient_dead_patients'])) * 100, 2) if ((x['is_tia_patients'] - x['ischemic_transient_dead_patients']) > 0) else 0, axis=1)
        
        #.round(decimals=2)) 
        self.statsDf.drop(['# patients receiving Vit. K antagonist', '# patients receiving dabigatran', '# patients receiving rivaroxaban', '# patients receiving apixaban', '# patients receiving edoxaban', '# patients receiving LMWH or heparin in prophylactic dose','# patients receiving LMWH or heparin in full anticoagulant dose'], axis=1, inplace=True)

        self.statsDf.fillna(0, inplace=True)
        ###########################################
        # ANTIPLATELETS - PRESCRIBED WITHOUT AFIB #
        ###########################################
        is_tia['ANTIPLATELETS'] = is_tia.apply(lambda x: get_antithrombotics([x['UKON_ASA'], x['UKON_CLOPIDOGREL']]), axis=1)    
    
        # patients not referred
        afib_flutter_not_detected_or_not_known = is_tia[is_tia['AFIB_FLUTTER'].isin([4, 5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known)

        afib_flutter_not_detected_or_not_known_dead = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_not_detected_or_not_known_dead_patients'] = self._count_patients(dataframe=afib_flutter_not_detected_or_not_known_dead)

        prescribed_antiplatelets_no_afib = afib_flutter_not_detected_or_not_known[afib_flutter_not_detected_or_not_known['ANTIPLATELETS'].isin([1])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib)

        prescribed_antiplatelets_no_afib_dead = prescribed_antiplatelets_no_afib[prescribed_antiplatelets_no_afib['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['prescribed_antiplatelets_no_afib_dead_patients'] = self._count_patients(dataframe=prescribed_antiplatelets_no_afib_dead)

        self.tmp = afib_flutter_not_detected_or_not_known.groupby(['Protocol ID', 'ANTIPLATELETS']).size().to_frame('count').reset_index()
        
        # Get patients receiving antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTIPLATELETS", value=1, new_column_name='# patients prescribed antiplatelets without aFib')

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antiplatelets without aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed antiplatelets without aFib'] - x['prescribed_antiplatelets_no_afib_dead_patients'])/(x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients'])) * 100, 2) if ((x['afib_flutter_not_detected_or_not_known_patients'] - x['afib_flutter_not_detected_or_not_known_dead_patients']) > 0) else 0, axis=1)

        #########################################
        # ANTICOAGULANTS - PRESCRIBED WITH AFIB #
        #########################################
    
        # patients not referred 
        afib_flutter_detected = is_tia[is_tia['AFIB_FLUTTER'].isin([1, 2, 3])].copy()
        self.statsDf['afib_flutter_detected_patients'] = self._count_patients(dataframe=afib_flutter_detected)

         # Get patients with prescribed anticoagulants
        afib_flutter_detected['ANTICOAGULANTS'] = afib_flutter_detected.apply(lambda x: get_antithrombotics([x['UKON_WARFARIN'], x['UKON_DABIGATRAN'], x['UKON_RIVAROXABAN'], x['UKON_APIXABAN'], x['UKON_EDOXABAN'], x['UKON_LMW'], x['UKON_ANTIKOAGULANCIA'], x['UKON_HEPARIN_VTE']]), axis=1)

        afib_flutter_detected_not_dead = afib_flutter_detected[~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_patients_not_dead'] = self._count_patients(dataframe=afib_flutter_detected_not_dead)

        anticoagulants_prescribed = afib_flutter_detected[afib_flutter_detected['ANTICOAGULANTS'].isin([1]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed anticoagulants with aFib'] = self._count_patients(dataframe=anticoagulants_prescribed)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_WARFARIN']).size().to_frame('count').reset_index()
        # Additional calculation 
        # Get patients receiving Vit. K antagonist
        self.statsDf = self._get_values_for_factors(column_name="UKON_WARFARIN", value=1, new_column_name='# patients receiving Vit. K antagonist')

        # Get % patients receiving Vit. K antagonist
        #self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['# patients prescribed anticoagulants with aFib']) * 100), 2) if x['# patients prescribed anticoagulants with aFib'] > 0 else 0, axis=1)
        self.statsDf['% patients receiving Vit. K antagonist'] = self.statsDf.apply(lambda x: round(((x['# patients receiving Vit. K antagonist']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_DABIGATRAN']).size().to_frame('count').reset_index()
        # Get patients receiving dabigatran
        self.statsDf = self._get_values_for_factors(column_name="UKON_DABIGATRAN", value=1, new_column_name='# patients receiving dabigatran')

        # Get % patients receiving dabigatran
        self.statsDf['% patients receiving dabigatran'] = self.statsDf.apply(lambda x: round(((x['# patients receiving dabigatran']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_RIVAROXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving rivaroxaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_RIVAROXABAN", value=1, new_column_name='# patients receiving rivaroxaban')

        # Get % patients receiving rivaroxaban
        self.statsDf['% patients receiving rivaroxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving rivaroxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_APIXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving apixaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_APIXABAN", value=1, new_column_name='# patients receiving apixaban')

        # Get % patients receiving apixaban
        self.statsDf['% patients receiving apixaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving apixaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_EDOXABAN']).size().to_frame('count').reset_index()
        # Get patients receiving edoxaban
        self.statsDf = self._get_values_for_factors(column_name="UKON_EDOXABAN", value=1, new_column_name='# patients receiving edoxaban')

        # Get % patients receiving edoxaban
        self.statsDf['% patients receiving edoxaban'] = self.statsDf.apply(lambda x: round(((x['# patients receiving edoxaban']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_HEPARIN_VTE']).size().to_frame('count').reset_index()
        # Get patients receiving LMWH or heparin in prophylactic dose
        self.statsDf = self._get_values_for_factors(column_name="UKON_HEPARIN_VTE", value=1, new_column_name='# patients receiving LMWH or heparin in prophylactic dose')

        # Get % patients receiving LMWH or heparin in prophylactic dose
        self.statsDf['% patients receiving LMWH or heparin in prophylactic dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in prophylactic dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        anticoagulants_prescribed['UKON_LMW_ANTICOAGULACNI'] = anticoagulants_prescribed.apply(lambda x: 2 if x['UKON_LMW'] == 2 and x['UKON_ANTIKOAGULANCIA'] == 2 else 1, axis=1)
        self.tmp = anticoagulants_prescribed.groupby(['Protocol ID', 'UKON_LMW_ANTICOAGULACNI']).size().to_frame('count').reset_index()
        # Get patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf = self._get_values_for_factors(column_name="UKON_LMW_ANTICOAGULACNI", value=1, new_column_name='# patients receiving LMWH or heparin in full anticoagulant dose')

        # Get % patients receiving LMWH or heparin in full anticoagulant dose
        self.statsDf['% patients receiving LMWH or heparin in full anticoagulant dose'] = self.statsDf.apply(lambda x: round(((x['# patients receiving LMWH or heparin in full anticoagulant dose']/x['afib_flutter_detected_patients_not_dead']) * 100), 2) if x['afib_flutter_detected_patients_not_dead'] > 0 else 0, axis=1)

        
        #anticoagulants_recommended = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9])].copy()
        #self.statsDf['anticoagulants_recommended_patients'] = self._count_patients(dataframe=anticoagulants_recommended)
        self.statsDf['anticoagulants_recommended_patients'] = 0

        afib_flutter_detected_dead = afib_flutter_detected[afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['afib_flutter_detected_dead_patients'] = self._count_patients(dataframe=afib_flutter_detected_dead)

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed anticoagulants with aFib'] =  self.statsDf.apply(lambda x: round(((x['# patients prescribed anticoagulants with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)

        ##########################################
        # ANTITHROMBOTICS - PRESCRIBED WITH AFIB #
        ##########################################
        # patients not reffered 
        antithrombotics_prescribed = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([1]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        self.statsDf['# patients prescribed antithrombotics with aFib'] = self._count_patients(dataframe=antithrombotics_prescribed)

        #recommended_antithrombotics_with_afib_alive = afib_flutter_detected[afib_flutter_detected['ANTITHROMBOTICS'].isin([9]) & ~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])].copy()
        #self.statsDf['recommended_antithrombotics_with_afib_alive_patients'] = self._count_patients(dataframe=recommended_antithrombotics_with_afib_alive)
        self.statsDf['recommended_antithrombotics_with_afib_alive_patients'] = 0

        # Get % patients receiving antiplatelets
        self.statsDf['% patients prescribed antithrombotics with aFib'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed antithrombotics with aFib']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'] - x['recommended_antithrombotics_with_afib_alive_patients']) > 0 else 0, axis=1)
        
        ###########
        # STATINS #
        ###########
        self.tmp = is_tia.groupby(['Protocol ID', 'STATIN']).size().to_frame('count').reset_index()

        # Get patients prescribed statins - Yes
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=1, new_column_name='# patients prescribed statins - Yes')

        # Get % patients prescribed statins - Yes
        self.statsDf['% patients prescribed statins - Yes'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Yes']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get patients prescribed statins - No
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=2, new_column_name='# patients prescribed statins - No')

        # Get % patients prescribed statins - No
        self.statsDf['% patients prescribed statins - No'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - No']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

        # Get patients prescribed statins - Not known
        self.statsDf = self._get_values_for_factors(column_name="STATIN", value=3, new_column_name='# patients prescribed statins - Not known')

        # Get % patients prescribed statins - Not known
        self.statsDf['% patients prescribed statins - Not known'] = self.statsDf.apply(lambda x: round(((x['# patients prescribed statins - Not known']/x['is_tia_patients']) * 100), 2) if x['is_tia_patients'] > 0 else 0, axis=1)

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

        ################
        # ANGEL AWARDS #
        ################
        #### TOTAL PATIENTS #####
        self.statsDf['# total patients >= 30'] = self.statsDf['Total Patients'] >= 30

        #### DOOR TO THROMBOLYSIS THERAPY - MINUTES ####
        #self.statsDf.loc[:, 'patients_eligible_recanalization'] = self.statsDf.apply(lambda x: x['# recanalization procedures - Not done'] + x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)
        self.statsDf.loc[:, 'patients_eligible_recanalization'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - Endovascular treatment alone'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)

        #self.statsDf.loc[:, 'patients_eligible_recanalization'] = self.statsDf.apply(lambda x: x['# recanalization procedures - IV tPa'] + x['# recanalization procedures - IV tPa + endovascular treatment'] + x['# recanalization procedures - IV tPa + referred to another centre for endovascular treatment'], axis=1)

        # patients treated with door to recanalization therapy < 60 minutes
        # for tby, we are only looking at patients that have had ONLY tby, not tpa + tby, as we awould be counting those patients twice (penalizing twice)
        recanalization_procedure_tby_only_dtg =  recanalization_procedure_tby_dtg[recanalization_procedure_tby_dtg['RECANALIZATION_PROCEDURES'].isin([4])]

        recanalization_procedure_iv_tpa_under_60 = recanalization_procedure_iv_tpa.loc[(recanalization_procedure_iv_tpa['IVTPA'] > 0) & (recanalization_procedure_iv_tpa['IVTPA'] <= 60)]
        #recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] <= 60]
        #recanalization_procedure_iv_tpa_under_60 = recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] < 60]

        recanalization_procedure_tby_only_dtg_under_60 = recanalization_procedure_tby_only_dtg.loc[(recanalization_procedure_tby_only_dtg['TBY'] > 0) & (recanalization_procedure_tby_only_dtg['TBY'] <= 60)]
        #recanalization_procedure_tby_only_dtg_under_60 = recanalization_procedure_tby_only_dtg[recanalization_procedure_tby_only_dtg['TBY'] <= 60]
        #recanalization_procedure_tby_only_dtg_under_60 = recanalization_procedure_tby_only_dtg[recanalization_procedure_tby_only_dtg['TBY'] < 60]
        
        # patients treated with door to recanalization therapy < 60 minutes
        self.statsDf['# patients treated with door to recanalization therapy < 60 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_60) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_60)
        #self.statsDf['# patients treated with door to recanalization therapy < 60 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_60)


        # % patients treated with door to recanalization therapy < 60 minutes
        self.statsDf['% patients treated with door to recanalization therapy < 60 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 60 minutes']/x['patients_eligible_recanalization']) * 100), 2) if x['patients_eligible_recanalization'] > 0 else 0, axis=1)

        recanalization_procedure_iv_tpa_under_45 = recanalization_procedure_iv_tpa.loc[(recanalization_procedure_iv_tpa['IVTPA'] > 0) & (recanalization_procedure_iv_tpa['IVTPA'] <= 45)]
        #recanalization_procedure_iv_tpa_under_45 = recanalization_procedure_iv_tpa[recanalization_procedure_iv_tpa['IVTPA'] <= 45]
        recanalization_procedure_tby_only_dtg_under_45 = recanalization_procedure_tby_only_dtg.loc[(recanalization_procedure_tby_only_dtg['TBY'] > 0) & (recanalization_procedure_tby_only_dtg['TBY'] <= 45)]
        #recanalization_procedure_tby_only_dtg_under_45 = recanalization_procedure_tby_only_dtg[recanalization_procedure_tby_only_dtg['TBY'] <= 45]

        # patients treated with door to recanalization therapy < 45 minutes
        self.statsDf['# patients treated with door to recanalization therapy < 45 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_45) + self._count_patients(dataframe=recanalization_procedure_tby_only_dtg_under_45)
        #self.statsDf['# patients treated with door to recanalization therapy < 45 minutes'] = self._count_patients(dataframe=recanalization_procedure_iv_tpa_under_45)

        # % patients treated with door to recanalization therapy < 45 minutes
        self.statsDf['% patients treated with door to recanalization therapy < 45 minutes'] = self.statsDf.apply(lambda x: round(((x['# patients treated with door to recanalization therapy < 45 minutes']/x['patients_eligible_recanalization']) * 100), 2) if x['patients_eligible_recanalization'] > 0 else 0, axis=1)

        # Get % patients recanalized
        #self.statsDf['patient_recan_%'] = self.statsDf.apply(lambda x: round(((x['patients_eligible_recanalization']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Endovascular treatment alone'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'] - x['# recanalization procedures - Endovascular treatment alone']) > 0 else 0, axis=1)
        self.statsDf['patient_recan_%'] = self.statsDf.apply(lambda x: round(((x['patients_eligible_recanalization']/(x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre'])) * 100), 2) if (x['isch_patients'] - x['# recanalization procedures - Referred to another centre for endovascular treatment'] - x['# recanalization procedures - Referred to another centre for endovascular treatment and hospitalization continues at the referred to centre'] - x['# recanalization procedures - Referred for endovascular treatment and patient is returned to the initial centre'] - x['# recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre']) > 0 else 0, axis=1)

        #### RECANALIZATION RATE ####
        # recanalization rate out of total ischemic incidence
        self.statsDf['# recanalization rate out of total ischemic incidence'] = self.statsDf['patients_eligible_recanalization']

        # % recanalization rate out of total ischemic incidence
        self.statsDf['% recanalization rate out of total ischemic incidence'] = self.statsDf['patient_recan_%']

        self.statsDf.drop(['patient_recan_%'], inplace=True, axis=1)

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
        non_transferred_antiplatelets = antithrombotics[~antithrombotics['RECANALIZATION_PROCEDURES'].isin([5,6])]
        #antithrombotics_discharged_home = antithrombotics[antithrombotics['DISCHARGE_DESTINATION'].isin([1])]
        # Get temporary dataframe with patients who have prescribed antithrombotics and ischemic stroke
        antiplatelets = non_transferred_antiplatelets[non_transferred_antiplatelets['STROKE_TYPE'].isin([1])]
        #antiplatelets = antithrombotics[antithrombotics['STROKE_TYPE'].isin([1])]
        #antiplatelets = antithrombotics_discharged_home[antithrombotics_discharged_home['STROKE_TYPE'].isin([1])]
        # Filter temporary dataframe and get only patients who have not been detected or not known for aFib flutter. 
        antiplatelets = antiplatelets[antiplatelets['AFIB_FLUTTER'].isin([4, 5])]
        # Get patients who have prescribed antithrombotics 
        except_recommended = antiplatelets[antiplatelets['ANTITHROMBOTICS'].isin([1,2])]

        # Get number of patients who have prescribed antithrombotics and ischemic stroke, have not been detected or not known for aFib flutter.
        self.statsDf['except_recommended_patients'] = self._count_patients(dataframe=except_recommended)
        # Get temporary dataframe groupby protocol ID and antithrombotics column
        self.tmp = antiplatelets.groupby(['Protocol ID', 'ANTIPLATELETS']).size().to_frame('count').reset_index()
        # ischemic stroke patients discharged with antiplatelets
        self.statsDf = self._get_values_for_factors(column_name="ANTIPLATELETS", value=1, new_column_name='# ischemic stroke patients discharged with antiplatelets')
        # % ischemic stroke patients discharged with antiplatelets
        self.statsDf['% ischemic stroke patients discharged with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged with antiplatelets']/x['except_recommended_patients']) * 100), 2) if x['except_recommended_patients'] > 0 else 0, axis=1)

        # discharged home
        antiplatelets_discharged_home = antiplatelets[antiplatelets['DISCHARGE_DESTINATION'].isin([1])]
        
        if (antiplatelets_discharged_home.empty):
            # Get temporary dataframe groupby protocol ID and antithrombotics column
            self.tmp = antiplatelets.groupby(['Protocol ID', 'ANTIPLATELETS']).size().to_frame('count').reset_index()
            # ischemic stroke patients discharged with antiplatelets
            self.statsDf = self._get_values_for_factors(column_name="ANTIPLATELETS", value=1, new_column_name='# ischemic stroke patients discharged home with antiplatelets')
            # % ischemic stroke patients discharged with antiplatelets
            self.statsDf['% ischemic stroke patients discharged home with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged home with antiplatelets']/x['except_recommended_patients']) * 100), 2) if x['except_recommended_patients'] > 0 else 0, axis=1)
            self.statsDf['except_recommended_discharged_home_patients'] = self.statsDf['except_recommended_patients']
        else:
            # Get temporary dataframe groupby protocol ID and antithrombotics column
            self.tmp = antiplatelets_discharged_home.groupby(['Protocol ID', 'ANTIPLATELETS']).size().to_frame('count').reset_index()
            # Get patients who have prescribed antithrombotics 
            except_recommended_discharged_home = except_recommended[except_recommended['DISCHARGE_DESTINATION'].isin([1])]

            # Get number of patients who have prescribed antithrombotics and ischemic stroke, have not been detected or not known for aFib flutter.
            self.statsDf['except_recommended_discharged_home_patients'] = self._count_patients(dataframe=except_recommended_discharged_home)
            # ischemic stroke patients discharged with antiplatelets
            self.statsDf = self._get_values_for_factors(column_name="ANTIPLATELETS", value=1, new_column_name='# ischemic stroke patients discharged home with antiplatelets')
            # % ischemic stroke patients discharged with antiplatelets
            self.statsDf['% ischemic stroke patients discharged home with antiplatelets'] = self.statsDf.apply(lambda x: round(((x['# ischemic stroke patients discharged home with antiplatelets']/x['except_recommended_discharged_home_patients']) * 100), 2) if x['except_recommended_discharged_home_patients'] > 0 else 0, axis=1)

        self.statsDf['# ischemic stroke patients discharged (home) with antiplatelets'] = self.statsDf.apply(lambda x: x['# ischemic stroke patients discharged with antiplatelets'] if x['# ischemic stroke patients discharged with antiplatelets'] > x['# ischemic stroke patients discharged home with antiplatelets'] else x['# ischemic stroke patients discharged home with antiplatelets'], axis=1)
        self.statsDf['% ischemic stroke patients discharged (home) with antiplatelets'] = self.statsDf.apply(lambda x: x['% ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['% ischemic stroke patients discharged home with antiplatelets'], axis=1)

        # afib patients discharged with anticoagulants
        self.statsDf['# afib patients discharged with anticoagulants'] = self._count_patients(dataframe=anticoagulants_prescribed)
        # Get temporary dataframe with patients who are not dead with detected aFib flutter and with prescribed antithrombotics 
        #afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (afib_flutter_detected['ANTICOAGULANTS'].isin([1]))]
        afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (afib_flutter_detected['ANTICOAGULANTS'].isin([1, 2]))]
        # Get afib patients discharged and not dead
        self.statsDf['afib_detected_discharged_patients'] = self._count_patients(dataframe=afib_detected_discharged_home)
        # % afib patients discharged with anticoagulants	
        #self.statsDf['% afib patients discharged with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged with anticoagulants']/(x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients'])) * 100), 2) if (x['afib_flutter_detected_patients'] - x['afib_flutter_detected_dead_patients']) > 0 else 0, axis=1)
        self.statsDf['% afib patients discharged with anticoagulants'] = self.statsDf.apply(lambda x: round(((x['# afib patients discharged with anticoagulants']/x['afib_detected_discharged_patients']) * 100), 2) if (x['afib_detected_discharged_patients']) > 0 else 0, axis=1)
        
        # Get temporary dataframe with patients who have prescribed anticoagulats and were discharged home 
        non_trasferred_anticoagulants = anticoagulants_prescribed[~anticoagulants_prescribed['RECANALIZATION_PROCEDURES'].isin([5,6])]
        anticoagulants_prescribed_discharged_home = non_trasferred_anticoagulants[non_trasferred_anticoagulants['DISCHARGE_DESTINATION'].isin([1])]
        #anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([1])]
        # Get temporary dataframe with patients who have been discharge at home with detected aFib flutter and with prescribed antithrombotics
        #afib_detected_discharged_home = afib_flutter_detected[(afib_flutter_detected['DISCHARGE_DESTINATION'].isin([1])) & (~afib_flutter_detected['ANTITHROMBOTICS'].isin([9]))]
        afib_detected_discharged_home = afib_flutter_detected[(afib_flutter_detected['DISCHARGE_DESTINATION'].isin([1])) & (afib_flutter_detected['ANTICOAGULANTS'].isin([1, 2])) & (~afib_flutter_detected['RECANALIZATION_PROCEDURES'].isin([5,6]))]

        # Check if temporary dataframe is empty. If yes, the value is calculated not only for discharged home, but only dead patients are excluded
        if (anticoagulants_prescribed_discharged_home.empty):
            # afib patients discharged home with anticoagulants	
            anticoagulants_prescribed_discharged_home = anticoagulants_prescribed[~anticoagulants_prescribed['DISCHARGE_DESTINATION'].isin([5])]
            # Get temporary dataframe with patients who are not dead with detected aFib flutter and with prescribed antithrombotics 
            afib_detected_discharged_home = afib_flutter_detected[(~afib_flutter_detected['DISCHARGE_DESTINATION'].isin([5])) & (afib_flutter_detected['ANTICOAGULANTS'].isin([1, 2]))]
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

        self.statsDf['# afib patients discharged (home) with anticoagulants'] = self.statsDf.apply(lambda x: x['# afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['# afib patients discharged home with anticoagulants'], axis=1)
        self.statsDf['% afib patients discharged (home) with anticoagulants'] = self.statsDf.apply(lambda x: x['% afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['% afib patients discharged home with anticoagulants'], axis=1)

        #### STROKE UNIT ####
        # stroke patients treated in a dedicated stroke unit / ICU
        self.statsDf['# stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['# patients hospitalized in stroke unit / ICU']

        # % stroke patients treated in a dedicated stroke unit / ICU	
        self.statsDf['% stroke patients treated in a dedicated stroke unit / ICU'] = self.statsDf['% patients hospitalized in stroke unit / ICU']

        # % stroke patients treated in a dedicated stroke unit / ICU (2nd)
        self.statsDf['% stroke patients treated in a dedicated stroke unit / ICU (2nd)'] = self.statsDf.apply(lambda x: x['% patients hospitalized in stroke unit / ICU'] if x['# patients hospitalized in stroke unit / ICU'] > 0 else 1, axis=1)

        self.statsDf.fillna(0, inplace=True)

        self.statsDf.rename(columns={"Protocol ID": "Site ID"}, inplace=True)

        # Save results into .csv
        #self.statsDf.to_csv('slovakia_results.csv', sep=',', index=False)

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

    def _get_values_for_factors(self, column_name, value, new_column_name, df=None):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (int): The integers value represents specific value from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
        """		
        #if df is None:
        if (np.issubdtype(self.tmp[column_name].dtype, np.number)):
            value = value
        else:
            value = str(value)

        tmpDf = self.tmp[self.tmp[column_name] == value].reset_index()[['Protocol ID', 'count']]
        factorDf = self.statsDf.merge(tmpDf, how='outer')
        factorDf.rename(columns={'count': new_column_name}, inplace=True)
        factorDf.fillna(0, inplace=True)

        """
        else:
            if (df[column_name].dtype != np.number):
                value = str(value)
            else:
                value = value 

            tmpDf = df[df[column_name] == value].reset_index()[['Protocol ID', 'count']]
            factorDf = self.statsDf.merge(tmpDf, how='outer')
            factorDf.rename(columns={'count': new_column_name}, inplace=True)
            factorDf.fillna(0, inplace=True)
        """
        return factorDf

    def _get_values_for_factors_more_values(self, column_name, value, new_column_name, df=None):
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (list): The list of integers representing specific values from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
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
        """ Returns the column with number of patients group by Protocol ID. 

        Keyword arguments:
            column_name (str): The name of column for which we are want calculate number of patients. 
            value (str): The str of integers representing specific values from the dataframe[column_name]. 
            new_column_name (str): The name of new column name. 

        Returns:
            dataframe (dataframe): The statsDf to which new created column was appended. 
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
        """ Calculate differnce between two times. 

        Args:
            hospital_time: The time of hospitalization
            ct_time: The time when CT has been perfrmed. 
        Returns:
            The calculated difference in minutes.
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
                
class GenerateFormattedStats:
    """
    This class generate formatted statistics in .xslx file. For angel awards are set colored conditions. 
    
    Keyword arguments:
        df: the dataframe with statistcs
        country: True if country should be included as site (default: False)
        country_code: the code of country (default: None)
        split_sites: True if you want to generate individual file fo each site (default: False)
        site: the site code (default: None)
        report: the report name (default: None)
        quarter: the quarter name (default: None)
    """
    def __init__(self, df, country=False, country_code=None, split_sites=False, site=None, report=None, quarter=None, comp=False):

        self.df_unformatted = df.copy()
        self.df = df.copy()
        self.country_code = country_code
        self.report = report
        self.quarter = quarter
        self.comp = comp

        def delete_columns(columns):
            for i in columns:
                if i in self.df.columns:
                    self.df.drop([i], inplace=True, axis=1)
        # Drop tmp column 
        delete_columns(['isch_patients', 'is_ich_patients', 'is_ich_tia_cvt_patients', 'is_ich_cvt_patients', 'is_tia_patients', 'is_ich_sah_cvt_patients', 'is_tia_cvt_patients', 'cvt_patients', 'ich_sah_patients', 'ich_patients',  'sah_patients', 'discharge_subset_patients','discharge_subset_alive_patients', 'neurosurgery_patients', 'not_reffered_patients', 'reffered_patients', 'afib_detected_during_hospitalization_patients', 'afib_not_detected_or_not_known_patients', 'antithrombotics_patients', 'ischemic_transient_dead_patients', 'afib_flutter_not_detected_or_not_known_patients', 'afib_flutter_not_detected_or_not_known_dead_patients', 'prescribed_antiplatelets_no_afib_patients', 'prescribed_antiplatelets_no_afib_dead_patients', 'afib_flutter_detected_patients', 'anticoagulants_recommended_patients', 'afib_flutter_detected_dead_patients', 'recommended_antithrombotics_with_afib_alive_patients', 'discharge_subset_same_centre_patients', 'discharge_subset_another_centre_patients', 'patients_eligible_recanalization', '# patients having stroke in the hospital - No', '% patients having stroke in the hospital - No', '# recurrent stroke - No', '% recurrent stroke - No', '# patients assessed for rehabilitation - Not known', '% patients assessed for rehabilitation - Not known', '# level of consciousness - not known', '% level of consciousness - not known', '# CT/MRI - Performed later than 1 hour after admission', '% CT/MRI - Performed later than 1 hour after admission', '# patients put on ventilator - Not known', '% patients put on ventilator - Not known', '# patients put on ventilator - No', '% patients put on ventilator - No', '# IV tPa', '% IV tPa', '# TBY', '% TBY', '# DIDO TBY', '# dysphagia screening - not known', '% dysphagia screening - not known', '# dysphagia screening time - After first 24 hours', '% dysphagia screening time - After first 24 hours', '# other afib detection method - Not detected or not known', '% other afib detection method - Not detected or not known', '# carotid arteries imaging - Not known', '% carotid arteries imaging - Not known', '# carotid arteries imaging - No', '% carotid arteries imaging - No', 'vascular_imaging_cta_norm', 'vascular_imaging_mra_norm', 'vascular_imaging_dsa_norm', 'vascular_imaging_none_norm', 'bleeding_arterial_hypertension_perc_norm', 'bleeding_aneurysm_perc_norm', 'bleeding_arterio_venous_malformation_perc_norm', 'bleeding_anticoagulation_therapy_perc_norm', 'bleeding_amyloid_angiopathy_perc_norm', 'bleeding_other_perc_norm', 'intervention_endovascular_perc_norm', 'intervention_neurosurgical_perc_norm', 'intervention_other_perc_norm', 'intervention_referred_perc_norm', 'intervention_none_perc_norm', 'vt_treatment_anticoagulation_perc_norm', 'vt_treatment_thrombectomy_perc_norm', 'vt_treatment_local_thrombolysis_perc_norm', 'vt_treatment_local_neurological_treatment_perc_norm', 'except_recommended_patients', 'afib_detected_discharged_home_patients', '% dysphagia screening done', '# dysphagia screening done', 'alert_all', 'alert_all_perc', 'drowsy_all', 'drowsy_all_perc', 'comatose_all', 'comatose_all_perc', 'antithrombotics_patients_with_cvt', 'ischemic_transient_cerebral_dead_patients', '# patients receiving antiplatelets with CVT', '% patients receiving antiplatelets with CVT', '# patients receiving Vit. K antagonist with CVT', '% patients receiving Vit. K antagonist with CVT', '# patients receiving dabigatran with CVT', '% patients receiving dabigatran with CVT', '# patients receiving rivaroxaban with CVT', '% patients receiving rivaroxaban with CVT', '# patients receiving apixaban with CVT', '% patients receiving apixaban with CVT', '# patients receiving edoxaban with CVT', '% patients receiving edoxaban with CVT', '# patients receiving LMWH or heparin in prophylactic dose with CVT', '% patients receiving LMWH or heparin in prophylactic dose with CVT', '# patients receiving LMWH or heparin in full anticoagulant dose with CVT', '% patients receiving LMWH or heparin in full anticoagulant dose with CVT', '# patients not prescribed antithrombotics, but recommended with CVT', '% patients not prescribed antithrombotics, but recommended with CVT', '# patients neither receiving antithrombotics nor recommended with CVT', '% patients neither receiving antithrombotics nor recommended with CVT', '# patients prescribed antithrombotics with CVT', '% patients prescribed antithrombotics with CVT', '# patients prescribed or recommended antithrombotics with CVT', '% patients prescribed or recommended antithrombotics with CVT', 'afib_flutter_not_detected_or_not_known_patients_with_cvt', 'afib_flutter_not_detected_or_not_known_dead_patients_with_cvt', 'prescribed_antiplatelets_no_afib_patients_with_cvt', 'prescribed_antiplatelets_no_afib_dead_patients_with_cvt', '# patients prescribed antiplatelets without aFib with CVT', '% patients prescribed antiplatelets without aFib with CVT', 'afib_flutter_detected_patients_with_cvt', '# patients prescribed anticoagulants with aFib with CVT', 'anticoagulants_recommended_patients_with_cvt', 'afib_flutter_detected_dead_patients_with_cvt', '% patients prescribed anticoagulants with aFib with CVT', '# patients prescribed antithrombotics with aFib with CVT', 'recommended_antithrombotics_with_afib_alive_patients_with_cvt', '% patients prescribed antithrombotics with aFib with CVT', 'afib_flutter_detected_patients_not_dead', 'except_recommended_discharged_home_patients', 'afib_detected_discharged_patients', 'ischemic_transient_dead_patients_prescribed', 'is_tia_discharged_home_patients'])

        # Connect to database and get country name according to country code.
        def select_country(value):
            country_name = pytz.country_names[value]
            return country_name

        # If country is used as site, the country name is selected from countries dictionary by country code. :) 
        if (country):
            if self.country_code == 'UZB':
                self.country_code = 'UZ'
            self.country_name = select_country(self.country_code)
        else:
            self.country_name = None

        # If split_sites is True, then go through dataframe and generate graphs for each site (the country will be included as site in each file).
        site_ids = self.df['Site ID'].tolist()
        # Delete country name from side ids list.
        try:
            site_ids.remove(self.country_name)
        except:
            pass
       
        if site is not None:
            df = self.df[self.df['Site ID'].isin([site, self.country_name])].copy()
            df_unformatted = self.df_unformatted[self.df_unformatted['Site ID'].isin([site, self.country_name])].copy()
            self._generate_formatted_statistics(df=df, df_tmp=df_unformatted, site_code=site)

        # Generate formatted statistics for all sites individualy + country as site is included
        if (split_sites) and site is None:
            for i in site_ids:
                df = self.df[self.df['Site ID'].isin([i, self.country_name])].copy()
                df_unformatted = self.df_unformatted[self.df_unformatted['Site ID'].isin([i, self.country_name])].copy()
                self._generate_formatted_statistics(df=df, df_tmp=df_unformatted, site_code=i)
    
        # Produce formatted statistics for all sites + country as site
        if site is None:
            self._generate_formatted_statistics(df=self.df, df_tmp=self.df_unformatted)

    def _generate_formatted_statistics(self, df, df_tmp, site_code=None):
        """Generate formatted statistics in xlsx file.
        
        Keyword arguments:
            df: the dataframe with statistics containing only necessary columns
            df_tmp: the dataframe with statistics containing all calculation
            site_code: the code of site (default: NOne)
        """

        if self.country_code is None and site_code is None:
            name_of_unformatted_stats = self.report + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + self.quarter + ".xlsx"
        elif site_code is None:
            name_of_unformatted_stats = self.report + "_" + self.country_code + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + self.country_code + "_" + self.quarter + ".xlsx"
        else:
            # self.report + "_" + site_code + "_" + self.quarter + ".csv"
            # self.report + "_" + site_code + "_" + self.quarter + ".xlsx"
            name_of_unformatted_stats = self.report + "_" + site_code + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + site_code + "_" + self.quarter + ".xlsx"

        df_tmp.to_csv(name_of_unformatted_stats, sep=",", encoding='utf-8', index=False)
        workbook1 = xlsxwriter.Workbook(name_of_output_file, {'strings_to_numbers': True})
        # create worksheet
        worksheet = workbook1.add_worksheet()

        # set width of columns
        worksheet.set_column(0, 4, 15)
        worksheet.set_column(4, 350, 60)
        # number of columns
        
        ncol = len(df.columns) - 1
       # print(ncol)

        # number of rows
        nrow = len(df) + 2

        col = []
        column_names = df.columns.tolist()
        column_names.append('Proposed Award')
        for i in range(0, ncol + 2):
            tmp = {}
            print(column_names[i])
            tmp['header'] = column_names[i]
            # if (i >= 2):
            #    tmp['total_function': 'sum']
            col.append(tmp)

        #df = statistics[0:nrow]
        # print(statistics)

        statistics = df.values.tolist()

        ########################
        # DICTIONARY OF COLORS #
        ########################
        colors = {
            "gender": "#477187",
            "stroke_hosp": "#535993",
            "recurrent_stroke": "#D4B86A",
            "department_type": "#D4A46A",
            "hospitalization": "#D4916A",
            "rehab": "#D4BA6A",
            "stroke": "#565595",
            "consciousness": "#468B78",
            "gcs": "#B9D6C1",
            "nihss": "#C5D068",
            "ct_mri": "#AA8739",
            "vasc_img": "#277650",
            "ventilator": "#AA5039",
            "recanalization_procedure": "#7F4C91",
            "median_times": "#BEBCBC",
            "dysphagia": "#F49B5B",
            "hemicraniectomy": "#A3E4D7",
            "neurosurgery": "#F8C471",
            "neurosurgery_type": "#CACFD2",
            "bleeding_reason": "#CB4335",
            "bleeding_source": "#9B59B6",
            "intervention": "#5DADE2",
            "vt_treatment": "#F5CBA7",
            "afib": "#A2C3F3",
            "carot": "#F1C40F",
            "antithrombotics": "#B5E59F",
            "statin": "#28B463",
            "carotid_stenosis": "#B9D6C1",
            "carot_foll": "#BFC9CA",
            "antihypertensive": "#7C7768",
            "smoking": "#F9C991",
            "cerebrovascular": "#91C09E",
            "discharge_destination": "#C0EFF5",
            "discharge_destination_same_centre": "#56A3A6",
            "discharge_destination_another_centre": "#E8DF9C",
            "discharge_destination_within_another_centre": "#538083",
            "angel_awards": "#B87333",
            "angel_resq_awards": "#341885",
            "columns": "#3378B8",
            "green": "#A1CCA1",
            "orange": "#DF7401",
            "gold": "#FFDF00",
            "platinum": "#c0c0c0",
            "black": "#ffffff",
            "red": "#F45D5D"
        }

        #statistics = statistics[1:nrow]

        ################
        # angel awards #
        ################
        awards = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("angel_awards")})

        awards_color = workbook1.add_format({
            'fg_color': colors.get("angel_awards")})

        worksheet.merge_range('EB1:FB1', 'ESO ANGELS AWARDS', awards)
        worksheet.write('EB2', '', awards_color)
        worksheet.write('EC2', '', awards_color)
        worksheet.write('ED2', '', awards_color)
        worksheet.write('EE2', '', awards_color)
        worksheet.write('EF2', '', awards_color)
        worksheet.write('EG2', '', awards_color)
        worksheet.write('EH2', '', awards_color)
        worksheet.write('EI2', '', awards_color)
        worksheet.write('EJ2', '', awards_color)
        worksheet.write('EK2', '', awards_color)
        worksheet.write('EL2', '', awards_color)
        worksheet.write('EM2', '', awards_color)
        worksheet.write('EN2', '', awards_color)
        worksheet.write('EO2', '', awards_color)
        worksheet.write('EP2', '', awards_color)
        worksheet.write('EQ2', '', awards_color)
        worksheet.write('ER2', '', awards_color)
        worksheet.write('ES2', '', awards_color)
        worksheet.write('ET2', '', awards_color)
        worksheet.write('EU2', '', awards_color)
        worksheet.write('EV2', '', awards_color)
        worksheet.write('EW2', '', awards_color)
        worksheet.write('EX2', '', awards_color)
        worksheet.write('EY2', '', awards_color)
        worksheet.write('EZ2', '', awards_color)
        worksheet.write('FA2', '', awards_color)
        worksheet.write('FB2', '', awards_color)
        # worksheet.write('LU2', '', awards_color)


        worksheet.set_column('EC:EC', None, None, {'hidden': True})
        # orksheet.set_column('LC:LC', None, None, {'hidden': True})
        worksheet.set_column('EE:EE', None, None, {'hidden': True})
        # worksheet.set_column('LE:LE', None, None, {'hidden': True})
        worksheet.set_column('EG:EG', None, None, {'hidden': True})
        # worksheet.set_column('LG:LG', None, None, {'hidden': True})
        worksheet.set_column('EI:EI', None, None, {'hidden': True})
        # worksheet.set_column('LI:LI', None, None, {'hidden': True})
        worksheet.set_column('EK:EK', None, None, {'hidden': True})
        # worksheet.set_column('LK:LK', None, None, {'hidden': True})
        worksheet.set_column('EM:EM', None, None, {'hidden': True})
        worksheet.set_column('EN:EN', None, None, {'hidden': True})
        worksheet.set_column('EO:EO', None, None, {'hidden': True})
        worksheet.set_column('EP:EP', None, None, {'hidden': True})
        worksheet.set_column('EQ:EQ', None, None, {'hidden': True})
        #worksheet.set_column('LQ:LQ', None, None, {'hidden': True})
        worksheet.set_column('ES:ES', None, None, {'hidden': True})
        worksheet.set_column('ET:ET', None, None, {'hidden': True})
        worksheet.set_column('EU:EU', None, None, {'hidden': True})
        worksheet.set_column('EV:EV', None, None, {'hidden': True})
        worksheet.set_column('EW:EW', None, None, {'hidden': True})
        worksheet.set_column('EY:EY', None, None, {'hidden': True})
        worksheet.set_column('EZ:EZ', None, None, {'hidden': True})

        # format for green color
        green = workbook1.add_format({
            'bold': 2,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': colors.get("green")})

        # format for gold color
        gold = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': colors.get("gold")})

        # format for platinum color
        plat = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': colors.get("platinum")})

        # format for gold black
        black = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#000000',
            'color': colors.get("black")})

        # format for red color
        red = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': colors.get("red")})


        # add table into worksheet
        options = {'data': statistics,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 8'
                   }
        #worksheet.set_column('E:V', 100)

        worksheet.add_table(2, 0, nrow, ncol + 1, options)

        # total number of rows
        number_of_rows = len(statistics) + 2

        # column where angels awards starts
        coln = 131

        if not self.comp:    
            # if cell contain TRUE in column > 30 patients (DR) it will be colored to green
            awards = []
            row = 4
            while row < nrow + 2:
                cell_n = 'EB' + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'TRUE',
                                                    'format': green})
                row += 1

            for row in range(0, nrow - 2):
                if (statistics[row][coln] == False):            
                    awards.append("NONE")
                else:
                    awards.append("TRUE")


            def angels_awards_ivt_60(column_name, coln=coln):
                """Add conditional formatting to angels awards for ivt < 60."""
                coln = coln + 2
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 50,
                                                        'maximum': 74.99,
                                                        'format': gold})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 75,
                                                        'format': black})
                    row += 1

                for row in range(nrow - 2):
                    if (awards[row] != "NONE"):
                        if (float(statistics[row][coln]) >= 50 and float(statistics[row][coln]) <= 74.99):
                            awards[row] = "GOLD"
                        elif (float(statistics[row][coln]) >= 75):
                            awards[row] = "DIAMOND"
                        else:
                            awards[row] = "NONE"


            angels_awards_ivt_60('ED')


            def angels_awards_ivt_45(column_name, coln=coln):
                """Add conditional formatting to angels awards for ivt < 45."""
                coln = coln + 4
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '<=',
                                                        'value': 49.99,
                                                        'format': plat})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 50,
                                                        'format': black})
                    row += 1

                for row in range(nrow - 2):
                    if (awards[row] != "NONE"):
                        if (float(statistics[row][coln]) <= 49.99):
                            if (awards[row] != "GOLD" or awards[row] == "DIAMOND"):
                                awards[row] = "PLATINUM"
                        elif (float(statistics[row][coln]) >= 50):
                            if (awards[row] != "GOLD"):
                                awards[row] = "DIAMOND"
                        else:
                            awards[row] = "NONE"


            angels_awards_ivt_45('EF')


            # setting colors of cells according to their values
            def angels_awards_recan(column_name, coln=coln):
                """Add conditional formatting to angels awards for recaalization procedures."""
                coln = coln + 6

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 5,
                                                        'maximum': 14.99,
                                                        'format': gold})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 15,
                                                        'maximum': 24.99,
                                                        'format': plat})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 25,
                                                        'format': black})
                    row += 1

                for row in range(nrow - 2):
                    if (awards[row] != "NONE"):
                        if (float(statistics[row][coln]) >= 5 and float(statistics[row][coln]) <= 14.99):
                            if (awards[row] == "PLATINUM" or awards[row] == "DIAMOND"):
                                awards[row] = "GOLD"
                        elif (float(statistics[row][coln]) >= 15 and float(statistics[row][coln]) <= 24.99):
                            if (awards[row] == "DIAMOND"):
                                awards[row] = "PLATINUM"
                        elif (float(statistics[row][coln]) >= 25):
                            if (awards[row] == "DIAMOND"):
                                awards[row] == "DIAMOND"
                        else:
                            awards[row] = "NONE"


            angels_awards_recan('EH')


            def angels_awards_processes(column_name, n, coln=coln, count=True):
                """Add conditional formatting to angels awards for processes."""
                count = count
                num = n
                coln = coln + num
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 80,
                                                        'maximum': 84.99,
                                                        'format': gold})

                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 85,
                                                        'maximum': 89.99,
                                                        'format': plat})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 90,
                                                        'format': black})
                    row += 1
                if (count):
                    for row in range(nrow - 2):
                        if (awards[row] != "NONE"):
                            if (float(statistics[row][coln]) >= 80 and float(statistics[row][coln]) <= 84.99):
                                if (awards[row] == "PLATINUM" or awards[row] == "DIAMOND"):
                                    awards[row] = "GOLD"
                            elif (float(statistics[row][coln]) >= 85 and float(statistics[row][coln]) <= 89.99):
                                if (awards[row] == "DIAMOND"):
                                    awards[row] = "PLATINUM"
                            elif (float(statistics[row][coln]) >= 90):
                                if (awards[row] == "DIAMOND"):
                                    awards[row] = "DIAMOND"
                            else:
                                awards[row] = "NONE"


            angels_awards_processes('EJ', 8)
            angels_awards_processes('EL', 10)
            angels_awards_processes('ER', 16)
            angels_awards_processes('EX', 22)

            # setting colors of cells according to their values
            def angels_awards_hosp(column_name, n, coln=coln):
                """Add conditional formatting to angels awards for hospitalization."""
                num = n
                coln = coln + num
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '<=',
                                                        'value': 0,
                                                        'format': plat})
                    row += 1

                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 0.99,
                                                        'format': black})
                    row += 1

                '''
                for row in range(nrow - 2):
                    if (awards[row] != "NONE"):
                        if (float(statistics[row][coln]) < 80):
                            if (awards[row] == "DIAMOND"):
                                awards[row] = "PLATINUM"
                        elif (float(statistics[row][coln]) >= 80):
                            if (awards[row] == "DIAMOND"):
                                awards[row] = "DIAMOND"
                        else:
                            awards[row] = "NONE"

                '''
                for row in range(nrow - 2):
                    if (awards[row] != "NONE"):
                        if (float(statistics[row][coln]) <= 0.99):
                            if (awards[row] == "DIAMOND"):
                                awards[row] = "PLATINUM"
                        elif (float(statistics[row][coln]) >= 1):
                            if (awards[row] == "DIAMOND"):
                                awards[row] = "DIAMOND"
                        else:
                            awards[row] = "NONE"

            angels_awards_hosp('FA', 25)

            coln = coln + 26
            worksheet.write_column(3, coln, awards)

            row = 4
            while row < nrow + 2:
                cell_n = 'FB' + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'NONE',
                                                    'format': green})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = 'FB' + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'GOLD',
                                                    'format': gold})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = 'FB' + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'PLATINUM',
                                                    'format': plat})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = 'FB' + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'DIAMOND',
                                                    'format': black})
                row += 1
        else:
            pass

        workbook1.close()