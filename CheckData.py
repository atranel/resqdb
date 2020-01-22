# -*- coding: utf-8 -*-
"""
File name: CheckData.py
Package: resq
Written by: Marie Jankujova - jankujova.marie@fnusa.cz on 11-2017
Version: v1.0
"""

import sys, os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
from dateutil.relativedelta import relativedelta
from threading import Thread

class CheckData:
    """ The class checking the dates and times in the dataframe. 
    
    :param df: the dataframe containing the raw data
    :type df: pandas dataframe
    :param nprocess: the number of processes to be run simultaneously
    :type nprocess: int
    """

    def __init__(self, df, nprocess=None):
        
        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.df = df.copy()
        self.nprocess = nprocess

        if self.nprocess is not None:
            # Dictionary initialization - db dataframes
            self.dfs = np.array_split(self.df, self.nprocess)
            self.pre_df = {}

            threads = []
            for i in range(0, len(self.dfs)):
                logging.info("Process{0}: Check of data has been started.".format(i))
                process = Thread(target=self.get_preprocessed_data(self.dfs[i], n=i, name=str(i)))
                process.start()
                threads.append(process)

            for process in threads:
                process.join()

            self.preprocessed_data = pd.DataFrame()
            for k, v in self.pre_df.items():
                self.preprocessed_data = self.preprocessed_data.append(v, sort=False)

        else:
            self.preprocessed_data = self.get_preprocessed_data(self.df)


    def time_diff(self, visit_date, hospital_date):
        """ The function calculating the difference in minutes between hospital date and visit date. 

        :param visit_date: the last seen normal date
        :type visit_date: the timestamp
        :param hospital_date: the date of hospitalization
        :type hospital_date: the timestamp
        :returns: the difference between two timestamps
        """
        if type(visit_date) is pd.Timestamp and type(hospital_date) is pd.Timestamp:
            time_diff = hospital_date - visit_date
            # Convert difference to minutes
            total_minutes = time_diff.total_seconds() / 60.0
        else:
            total_minutes = 0

        if total_minutes < 0 or total_minutes > 40000:
            total_minutes = 0
        
        return total_minutes


    def get_preprocessed_data(self, df, n=None, name=None):    
        """ The function preparing the preprocessed data from the raw data. 

        :param df: the dataframe with the raw data
        :type df: pandas dataframe
        :param n: the number of the process if more processes are run simultaneously
        :type n: int
        :param name: the name of dataframe used as key in the dictionary
        :type name: str
        :returns: preprocessed data if one process has been run
        """     
        
        if self.nprocess is not None:
            preprocessed_data = df.copy()
            # Calculate hospital days
            preprocessed_data['HOSPITAL_DAYS'] = preprocessed_data.apply(lambda x: self._get_hospital_days(x['HOSPITAL_DATE'], x['DISCHARGE_DATE']), axis=1)
            if n is not None:
                logging.info("Process{0}: Hospital days were calculated.".format(n))
            else:
                logging.info("Hospital days were calculated.")
            
            # Add to old columns suffix _OLD
            preprocessed_data.rename(columns={"VISIT_DATE": "VISIT_DATE_OLD", "HOSPITAL_DATE": "HOSPITAL_DATE_OLD","DISCHARGE_DATE": "DISCHARGE_DATE_OLD", "HOSPITAL_DAYS": "HOSPITAL_DAYS_OLD",}, inplace=True)

            # Fix hospital/discharge date if hospital days < 0 or > 300
            preprocessed_data['VISIT_DATE'], preprocessed_data['HOSPITAL_DATE'], preprocessed_data['DISCHARGE_DATE'], preprocessed_data['HOSPITAL_DAYS'], preprocessed_data['HOSPITAL_DAYS_FIXED'] = zip(*preprocessed_data.apply(lambda x: self._fix_dates(visit_date=x['VISIT_DATE_OLD'], hosp_date=x['HOSPITAL_DATE_OLD'], disc_date=x['DISCHARGE_DATE_OLD']) if (x['HOSPITAL_DAYS_OLD'] < 0 or x['HOSPITAL_DAYS_OLD'] > 300) else (x['VISIT_DATE_OLD'], x['HOSPITAL_DATE_OLD'], x['DISCHARGE_DATE_OLD'], x['HOSPITAL_DAYS_OLD'], False), axis=1))

            preprocessed_data['VISIT_TIMESTAMP'] = preprocessed_data.apply(lambda x: datetime.combine(x['VISIT_DATE'], x['VISIT_TIME']) if x['VISIT_TIME'] is not None else None, axis=1)
            preprocessed_data['HOSPITAL_TIMESTAMP'] = preprocessed_data.apply(lambda x: datetime.combine(x['HOSPITAL_DATE'], x['HOSPITAL_TIME']) if x['HOSPITAL_TIME'] is not None else None, axis=1)
            preprocessed_data['LAST_SEEN_NORMAL'] = preprocessed_data.apply(lambda x: self.time_diff(x['VISIT_TIMESTAMP'], x['HOSPITAL_TIMESTAMP']), axis=1)

            if n is not None:
                logging.info("Process{0}: Dates were fixed.".format(n))
            else:
                logging.info("Check data: Dates were fixed.")

            # Fix times
            preprocessed_data = self._fix_times(df=preprocessed_data)
            if n is not None:
                logging.info("Process{0}: Times were fixed and differences in minutes has been calculated.".format(n))
            else:
                logging.info("Times were fixed an differences in minutes has been calculated.")

            self.pre_df[name] = preprocessed_data

        else:
            preprocessed_data = df.copy()
            # Calculate hospital days
            preprocessed_data['HOSPITAL_DAYS'] = preprocessed_data.apply(lambda x: self._get_hospital_days(x['HOSPITAL_DATE'], x['DISCHARGE_DATE']), axis=1)
            if self.nprocess is not None:
                logging.info("Process{0}: Hospital days were calculated.".format(self.nprocess))
            else:
                logging.info("Hospital days were calculated.")
            
            # Add to old columns suffix _OLD
            preprocessed_data.rename(columns={"VISIT_DATE": "VISIT_DATE_OLD", "HOSPITAL_DATE": "HOSPITAL_DATE_OLD","DISCHARGE_DATE": "DISCHARGE_DATE_OLD", "HOSPITAL_DAYS": "HOSPITAL_DAYS_OLD",}, inplace=True)

            # Fix hospital/discharge date if hospital days < 0 or > 300
            preprocessed_data['VISIT_DATE'], preprocessed_data['HOSPITAL_DATE'], preprocessed_data['DISCHARGE_DATE'], preprocessed_data['HOSPITAL_DAYS'], preprocessed_data['HOSPITAL_DAYS_FIXED'] = zip(*preprocessed_data.apply(lambda x: self._fix_dates(visit_date=x['VISIT_DATE_OLD'], hosp_date=x['HOSPITAL_DATE_OLD'], disc_date=x['DISCHARGE_DATE_OLD']) if (x['HOSPITAL_DAYS_OLD'] < 0 or x['HOSPITAL_DAYS_OLD'] > 300) else (x['VISIT_DATE_OLD'], x['HOSPITAL_DATE_OLD'], x['DISCHARGE_DATE_OLD'], x['HOSPITAL_DAYS_OLD'], False), axis=1))

            preprocessed_data['VISIT_TIMESTAMP'] = preprocessed_data.apply(lambda x: datetime.combine(x['VISIT_DATE'], x['VISIT_TIME']) if x['VISIT_TIME'] is not None else None, axis=1)
            preprocessed_data['HOSPITAL_TIMESTAMP'] = preprocessed_data.apply(lambda x: datetime.combine(x['HOSPITAL_DATE'], x['HOSPITAL_TIME']) if x['HOSPITAL_TIME'] is not None else None, axis=1)
            preprocessed_data['LAST_SEEN_NORMAL'] = preprocessed_data.apply(lambda x: self.time_diff(x['VISIT_TIMESTAMP'], x['HOSPITAL_TIMESTAMP']), axis=1)


            if self.nprocess is not None:
                logging.info("Process{0}: Dates were fixed.".format(self.nprocess))
            else:
                logging.info("Check data: Dates were fixed.")

            # Fix times
            preprocessed_data = self._fix_times(df=preprocessed_data)
            if self.nprocess is not None:
                logging.info("Process{0}: Times were fixed and differences in minutes has been calculated.".format(self.nprocess))
            else:
                logging.info("Times were fixed and differences in minutes has been calculated.")

            return preprocessed_data

    
    def _fix_dates(self, visit_date, hosp_date, disc_date):
        """ The function fixing the hospital date and discharge date if hospital days were negative. 

        :param visit_date: the last seen normal date
        :type visit_date: date
        :param hosp_date: the date of hospitalization
        :type hosp_date: date
        :param disc_date: the discharge date
        :type disc_date: date
        :returns: fixed visit_date, fixed hosp_date, fixed disc_date, hospital_days calculated from the fixed dates, `True` if values has been fixed
        """
        # Set to True if hospital date or discharge date was fixed. Default: True
        fixed = False

        hospital_days = self._get_hospital_days(hosp_date, disc_date)

        # If discharge date is default, set discharge date to be hospital date + 1 day.
        if disc_date.year == 1900:
            #disc_date = hosp_date + relativedelta(days=+1)
            disc_date = hosp_date 
            fixed = True
        
        # If hospital days are negative
        if hospital_days < 0:
            if hosp_date.year != disc_date.year:
                if hosp_date.year != visit_date.year:
                    hosp_date = hosp_date + relativedelta(year=visit_date.year)
                if disc_date.year != visit_date.year:
                    disc_date = disc_date + relativedelta(year=visit_date.year)

            hospital_days = self._get_hospital_days(hosp_date, disc_date)
        
            if hospital_days <= -300:
                if (disc_date.year + 1) > datetime.now().year:
                    hosp_date = hosp_date + relativedelta(years=-1)
                    disc_date = disc_date + relativedelta(years=-1)
                # Added on April 03, 2019
                elif hosp_date.year > visit_date.year and hosp_date.month == visit_date.month:
                    hosp_date = hosp_date + relativedelta(year=visit_date.year)
            elif hospital_days > -300 and hospital_days < 0:
                if disc_date.month == hosp_date.month and hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(months=+1)
                if disc_date.month == hosp_date.month and hosp_date.month != visit_date.month:
                    if hosp_date.day > disc_date.day and hosp_date.day >= visit_date.day:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                        if hosp_date.year != visit_date.year:
                            hosp_date = hosp_date + relativedelta(year=visit_date.year)
                    elif hosp_date.day > disc_date.day and hosp_date.day < visit_date.day:
                        disc_date = disc_date + relativedelta(months=+1)
                elif disc_date.month < hosp_date.month and hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                        disc_date = disc_date + relativedelta(months=+1)
                    elif disc_date.day >= hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                elif disc_date.month == visit_date.month and hosp_date.month != visit_date.month:
                    hosp_date = hosp_date + relativedelta(month=visit_date.month)
                elif disc_date.month != visit_date.month and hosp_date.month != visit_date.month:
                    if hosp_date.month > disc_date.month:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                        if hosp_date.year != visit_date.year:
                            hosp_date = hosp_date + relativedelta(year=visit_date.year)
                    elif hosp_date.month < disc_date.month and hosp_date.month < visit_date.month:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                        if hosp_date.year != visit_date.year:
                            hosp_date = hosp_date + relativedelta(year=visit_date.year)
            
            hospital_days = self._get_hospital_days(hosp_date, disc_date)
            
            if hospital_days < -300:
                if visit_date.year != hosp_date.year and disc_date.year == hosp_date.year:
                    hosp_date = hosp_date + relativedelta(year=hosp_date.year)
                elif visit_date.year == hosp_date.year and disc_date.year == hosp_date.year:
                    disc_date = disc_date + relativedelta(years=+1)
            elif hospital_days > -300 and hospital_days < 0:
                if disc_date.month == hosp_date.month and hosp_date.month != visit_date.month:
                    if hosp_date.day > disc_date.day:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                elif disc_date.month == hosp_date.month:
                    disc_date = disc_date + relativedelta(months=+1)
                elif disc_date.month < hosp_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                        disc_date = disc_date + relativedelta(months=+1)
                    else: 
                        disc_date = disc_date + relativedelta(month=hosp_date.month)

            hospital_days = self._get_hospital_days(hosp_date, disc_date)

            if hospital_days > 300:
                if hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                        disc_date = disc_date + relativedelta(months=+1)
                    elif disc_date.day >= hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                elif hosp_date.month != visit_date.month and disc_date.month > visit_date.month:
                    hosp_date = hosp_date + relativedelta(month=visit_date.month)
                else:
                    hosp_date = hosp_date + relativedelta(month=disc_date.month) 
            elif hospital_days > 100 and hospital_days < 300:
                if disc_date.month == visit_date.month and hosp_date.month != visit_date.month:
                    hosp_date = hosp_date + relativedelta(month=visit_date.month)
                elif disc_date.month > visit_date.month and hosp_date.month < visit_date.month:
                    hosp_date = hosp_date + relativedelta(month=visit_date.month)
                elif disc_date.year == datetime.now().year and disc_date.month > datetime.now().month:
                    disc_date = disc_date + relativedelta(month=hosp_date.month)
        
            hospital_days = self._get_hospital_days(hosp_date, disc_date)

            fixed = True
        
        hospital_days = self._get_hospital_days(hosp_date, disc_date)

        # Hospital days > 300
        if hospital_days > 300:
            if hosp_date.year != disc_date.year:
                if hosp_date.year != visit_date.year:
                    hosp_date = hosp_date + relativedelta(year=visit_date.year)
                # Added on April 03, 2019
                else:
                    if hosp_date.month < visit_date.month and hosp_date.day >= visit_date.day:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                if disc_date.year != visit_date.year:
                    if hosp_date.month < visit_date.month:
                        hosp_date = hosp_date + relativedelta(year=disc_date.year)
                    else:
                        disc_date = disc_date + relativedelta(year=visit_date.year)
            
            hospital_days = self._get_hospital_days(hosp_date, disc_date)
        
            if hospital_days < 0:
                if disc_date.month == hosp_date.month and hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(months=+1)
                elif disc_date.month == hosp_date.month and hosp_date.month != visit_date.month:
                    if hosp_date.day > disc_date.day and hosp_date.day >= visit_date.day:
                        hosp_date = hosp_date + relativedelta(month=visit_date.month)
                    elif hosp_date.day > disc_date.day and hosp_date.day < visit_date.day:
                        disc_date = disc_date + relativedelta(months=+1)
                elif disc_date.month < hosp_date.month and hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                        disc_date = disc_date + relativedelta(months=+1)
                    elif disc_date.day >= hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
                elif disc_date.month == visit_date.month and hosp_date.month != visit_date.month:
                    hosp_date = hosp_date + relativedelta(month=visit_date.month)
                elif disc_date.month != visit_date.month and hosp_date.month != visit_date.month:
                    if hosp_date.month > disc_date.month:
                        if hosp_date.day < disc_date.day:
                            hosp_date = hosp_date + relativedelta(month=disc_date.month)
                        else:
                            hosp_date = hosp_date + relativedelta(month=visit_date.month)

            if hospital_days > 300:
                if hosp_date.month == visit_date.month:
                    if disc_date.day < hosp_date.day:
                        disc_date = disc_date + relativedelta(year=(hosp_date.year + 1))
                    elif disc_date.day >= hosp_date.day:
                        disc_date = disc_date + relativedelta(month=hosp_date.month)
            fixed = True
        
        hospital_days = self._get_hospital_days(hosp_date, disc_date)

        return visit_date, hosp_date, disc_date, hospital_days, fixed


    def _get_hospital_days(self, hosp_date, disc_date):
        """ The function calculating the number of hospital days. 

        :param hosp_date: the date of hospitalization
        :type hosp_date: date
        :param disc_date: the discharge date
        :type disc_date: date
        :returns: the number of hospital days
        """

        try: 
            diff_days = (disc_date - hosp_date).days
            # If hospital date and discharge date are the same day, replace 0 by 1.
            if diff_days == 0:
                diff_days = 1
            return diff_days
        except TypeError as error:
            logging.error(error)


    def _fix_times(self, df):
        """ The function fixing the times for recanalization procedures. 

        :param df: the dataframe with raw data
        :type df: pandas dataframe
        :returns: the dataframe with the fixed times
        """
        # IVT_ONLY - 1) filled in minutes, 2) filled admission and bolus time
        # IVT_ONLY_ADMISSION_TIME - HH:MM format
        # IVT_ONLY_BOLUS_TIME - HH:MM format
        # IVT_ONLY_NEEDLE_TIME - minutes
        # IVT_ONLY_NEEDLE_TIME_MIN - calculated by script (minutes) 
        df['IVTPA'] = 0
        df['TBY'] = 0
        # Replace NaN values by 0
        df.fillna({
            'IVT_ONLY_NEEDLE_TIME': 0, 
            'IVT_TBY_NEEDLE_TIME': 0, 
            'IVT_TBY_REFER_NEEDLE_TIME': 0
            })
        # Fill IVT only needle time into IVTPA column
        df['IVTPA'] = df.apply(lambda x: x['IVT_ONLY_NEEDLE_TIME'] if x['IVT_ONLY'] == 1 else x['IVTPA'], axis=1)

        df['IVT_ONLY_NEEDLE_TIME'] = df.apply(lambda x: 0 if x['IVT_ONLY'] == 2 else x['IVT_ONLY_NEEDLE_TIME'], axis=1) # (delete values calculated by Mirek)
        # IVT needle time
        if ('IVT_ONLY_BOLUS_TIME' in df.columns and 'IVT_ONLY_ADMISSION_TIME' in df.columns):

            df['IVT_ONLY_NEEDLE_TIME_MIN'], df['IVT_ONLY_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=
                str(x['IVT_ONLY_ADMISSION_TIME']), bolus_time=str(x['IVT_ONLY_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if (x['IVT_ONLY'] == 2) else (0, False), axis=1))
            df['IVTPA'] = df.apply(lambda x: x['IVT_ONLY_NEEDLE_TIME_MIN'] if x['IVT_ONLY'] == 2 else x['IVTPA'], axis=1)

        # Create new column called IVT_DONE, if 1 than IVT has been performed else NaN
        df['IVT_DONE'] = df.apply(lambda x: 1 if x['IVT_ONLY'] in [1,2] else np.nan, axis=1)

        # Create IVT_TBY column
        # IVT_TBY - 1) filled in minutes, 2) filled admission, bolus and groin puncture time
        # IVT_TBY_NEEDLE_TIME - Mirek's calculation of needle time
        # IVT_TBY_ADMISSION_TIME - HH:MM format
        # IVT_TBY_BOLUS_TIME - HH:MM format
        # IVT_TBY_GROIN_PUNCTURE_TIME - HH:MM format
        # IVT_TBY_NEEDLE_TIME in minutes
        # IVT_TBY_NEEDLE_TIME_MIN - calculated by script (minutes) (delete values calculated by Mirek)
        df['IVTPA'] = df.apply(lambda x: x['IVT_TBY_NEEDLE_TIME'] if x['IVT_TBY'] == 1 else x['IVTPA'], axis=1)
        df['IVT_TBY_NEEDLE_TIME'] = df.apply(lambda x: 0 if x['IVT_TBY'] == 2 else x['IVT_TBY_NEEDLE_TIME'], axis=1) #(delete values calculated by Mirek)

        # IVT TBY needle time
        if ('IVT_TBY_ADMISSION_TIME' in df.columns and 'IVT_TBY_BOLUS_TIME' in df.columns):

            df['IVT_TBY_NEEDLE_TIME_MIN'], df['IVT_TBY_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if(x['IVT_TBY'] == 2) else (0, False), axis=1))
            df['IVTPA'] = df.apply(lambda x: x['IVT_TBY_NEEDLE_TIME_MIN'] if x['IVT_TBY'] == 2 else x['IVTPA'], axis=1)

        df['IVT_DONE'] = df.apply(lambda x: 1 if x['IVT_TBY'] in [1,2] else x['IVT_DONE'], axis=1)

        

        # Create IVT_TBY_REFER column
        # IVT_TBY_REFER_ADMISSION_TIME - HH:MM format
        # IVT_TBY_REFER_ADMISSION_TIME - HH:MM format
        # IVT_TBY_REFER_NEEDLE_TIME - minutes
        # IVT_TBY_REFER_NEEDLE_TIME_MIN - calculated by script (minutes) (delete values calculated by Mirek)
        df['IVTPA'] = df.apply(lambda x: x['IVT_TBY_REFER_NEEDLE_TIME'] if x['IVT_TBY_REFER'] == 1 else x['IVTPA'], axis=1)
        df['IVT_TBY_REFER_NEEDLE_TIME'] = df.apply(lambda x: 0 if x['IVT_TBY_REFER'] == 2 else x['IVT_TBY_REFER_NEEDLE_TIME'], axis=1) # (delete values calculated by Mirek)

        # IVT TBY refer needle time
        if ('IVT_TBY_REFER_ADMISSION_TIME' in df.columns and 'IVT_TBY_REFER_BOLUS_TIME' in df.columns):

            df['IVT_TBY_REFER_NEEDLE_TIME_MIN'], df['IVT_TBY_REFER_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_REFER_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_REFER_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if(x['IVT_TBY_REFER'] == 2) else (0, False), axis=1))
            df['IVTPA'] = df.apply(lambda x: x['IVT_TBY_REFER_NEEDLE_TIME_MIN'] if x['IVT_TBY_REFER'] == 2 else x['IVTPA'], axis=1)

        df['IVT_DONE'] = df.apply(lambda x: 1 if x['IVT_TBY_REFER'] in [1,2] else x['IVT_DONE'], axis=1)

        # Create TBY_ONLY column
        # TBY_ONLY_ADMISSION_TIME - HH:MM format
        # TBY_ONLY_PUNCTURE_TIME - HH:MM format
        # TBY_ONLY_GROIN_PUNCTURE_TIME - minutes
        # TBY_ONLY_GROIN_TIME_MIN - calculated by script (minutes)

        # Fill Nan values by 0
        df.fillna({
            'TBY_ONLY_GROIN_PUNCTURE_TIME': 0, 
            'IVT_TBY_GROIN_TIME': 0, 
            'TBY_REFER_ALL_GROIN_PUNCTURE_TIME': 0, 
            'TBY_REFER_LIM_GROIN_PUNCTURE_TIME': 0})

        df['TBY'] = df.apply(lambda x: x['TBY_ONLY_GROIN_PUNCTURE_TIME'] if x['TBY_ONLY'] == 1 else x['TBY'], axis=1)
        df['TBY_ONLY_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: 0 if x['TBY_ONLY'] == 2 else x['TBY_ONLY_GROIN_PUNCTURE_TIME'], axis=1)

        # TBY only groin time
        if ('TBY_ONLY_PUNCTURE_TIME' in df.columns and 'TBY_ONLY_ADMISSION_TIME' in df.columns):

            df['TBY_ONLY_GROIN_TIME_MIN'], df['TBY_ONLY_GROIN_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_ONLY_ADMISSION_TIME']), bolus_time=str(x['TBY_ONLY_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_ONLY'] == 2) else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['TBY_ONLY_GROIN_TIME_MIN'] if x['TBY_ONLY'] == 2 else x['TBY'], axis=1)

        # Create TBY_DONE if TBY has been performed, else NaN
        df['TBY_DONE'] = df.apply(lambda x: 1 if x['TBY_ONLY'] in [1,2] else np.nan, axis=1)

        # IVT TBY groin puncture time
        # IVT_TBY_ADMISSION_TIME - HH:MM format
        # IVT_TBY_GROIN_PUNCTURE_TIME - HH:MM format
        # IVT_TBY_GROIN_TIME_MIN - calculated by script (minutes)
        df['TBY'] = df.apply(lambda x: x['IVT_TBY_GROIN_TIME'] if x['IVT_TBY'] == 1 else x['TBY'], axis=1)
        df['IVT_TBY_GROIN_TIME'] = df.apply(lambda x: 0 if x['IVT_TBY'] == 2 else x['IVT_TBY_GROIN_TIME'], axis=1)

        if ('IVT_TBY_ADMISSION_TIME' in df.columns and 'IVT_TBY_GROIN_PUNCTURE_TIME' in df.columns):

            df['IVT_TBY_GROIN_TIME_MIN'], df['IVT_TBY_GROIN_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_GROIN_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['IVT_TBY'] == 2) else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['IVT_TBY_GROIN_TIME_MIN'] if x['IVT_TBY'] == 2 else x['TBY'], axis=1)

        df['TBY_DONE'] = df.apply(lambda x: 1 if x['IVT_TBY'] in [1,2] else x['TBY_DONE'], axis=1)

        # Implement changes from F_RESQ_IVT_TBY_CZ_4
        df['TBY'] = df.apply(lambda x: x['TBY_REFER_ALL_GROIN_TIME'] if x['TBY_REFER_ALL'] == 1 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY'], axis=1)

        if ('TBY_REFER_ALL_GROIN_PUNCTURE_TIME' in df.columns and 'TBY_REFER_ALL_ADMISSION_TIME' in df.columns):
            
            df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ALL_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_ALL'] == 2 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4']) else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'] if x['TBY_REFER_ALL'] == 2 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY'], axis=1)

        df['TBY_DONE'] = df.apply(lambda x: 1 if x['TBY_REFER_ALL'] in [1,2] and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY_DONE'], axis=1)
        
        # F_RESQ_IVT_TBY_CZ_2
        # TO DO: August 2019
        # Implement changes made to IVT/TBY form. In the TBY_REFER_ALL and TBY_REFER_LIM were replace values for discharge by groin time. But in the name of column is mistake and column is names as TBY_REFER_ALL_BOLUS_TIME/TBY_REFER_LIM_BOLUS_TIME if the time is entered in HH:MM format. In the future will be these column names as TBY_REFER_ALL_PUNCTURE_TIME/TBY_REFER__LIM_PUNCTURE_TIME

        # Set nan to TBY_REFER_ALL_GROIN_PUNCTURE_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        # df['TBY'] = df.apply(lambda x: x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] if x['TBY_REFER_ALL'] == 1 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY'], axis=1)

        #df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] if x['TBY_REFER_ALL'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else 0, axis=1)
        # To calculate groin puncture from DIDO time (needed due to missing field in the prev version of form), take only times in minutes
        #df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_ALL_DIDO_TIME'] if 'IVT_TBY' in x['crf_parent_name'] and x['TBY_REFER_ALL'] == 1 else x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'], axis=1)

        """
        # Calculate TBY refer all bolus time
        if ('TBY_REFER_ALL_BOLUS_TIME' in df.columns and 'TBY_REFER_ALL_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ALL_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_ALL_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_ALL'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2') else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'] if x['TBY_REFER_ALL'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY'], axis=1)
        
        df['TBY_DONE'] = df.apply(lambda x: 1 if x['TBY_REFER_ALL'] in [1,2] and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY_DONE'], axis=1)
        """

        # Implement changes from F_RESQ_IVT_TBY_CZ_4
        # We can comment the previous code for version CZ_2 because when the times are mapped, the cz_2 is mappd to cz_4
        # TBY_REFER_ALL_GROIN_PUNCTURE_TIME_CZ -> TBY_REFER_ALL_GROIN_TIME_CZ
        # TBY_REFER_ALL_BOLUS_TIME_CZ -> TBY_REFER_ALL_GROIN_PUNCTURE_TIME_CZ_2
        # TBY_REFER_LIM_GROIN_PUNCTURE_TIME_CZ -> TBY_REFER_LIM_GROIN_TIME_CZ
        # TBY_REFER_LIM_BOLUS_TIME_CZ -> TBY_REFER_LIM_GROIN_PUNCTURE_TIME_CZ_2
        df['TBY'] = df.apply(lambda x: x['TBY_REFER_LIM_GROIN_TIME'] if x['TBY_REFER_LIM'] == 1 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY'], axis=1)

        if ('TBY_REFER_LIM_GROIN_PUNCTURE_TIME' in df.columns and 'TBY_REFER_LIM_ADMISSION_TIME' in df.columns):
            df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_LIM_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_LIM'] == 2 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4']) else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN'] if x['TBY_REFER_LIM'] == 2 and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY'], axis=1)


        df['TBY_DONE'] = df.apply(lambda x: 1 if x['TBY_REFER_LIM'] in [1,2] and x['crf_parent_name'] in ['F_RESQ_IVT_TBY_CZ_2', 'F_RESQ_IVT_TBY_CZ_4'] else x['TBY_DONE'], axis=1)

        # Set nan to TBY_REFER_LIM_GROIN_PUNCTURE_TIME if times in HH:MM were filled in (delete values calculated by Mirek)

        # df['TBY'] = df.apply(lambda x: x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] if x['TBY_REFER_LIM'] == 1 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY'], axis=1)
        # df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] if x['TBY_REFER_LIM'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else 0, axis=1)
        # To calculate groin puncture from DIDO time (needed due to missing field in the prev version of form), take only times in minutes
        # df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_LIM_DIDO_TIME'] if 'IVT_TBY' in x['crf_parent_name'] and x['TBY_REFER_LIM'] == 1 else x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'], axis=1)

        """
        # TBY refer lim groin puncture time
        if ('TBY_REFER_LIM_BOLUS_TIME' in df.columns and 'TBY_REFER_LIM_ADMISSION_TIME' in df.columns):
            df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_LIM_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_LIM_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_LIM'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2') else (0, False), axis=1))
            df['TBY'] = df.apply(lambda x: x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN'] if x['TBY_REFER_LIM'] == 2 and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY'], axis=1)


        df['TBY_DONE'] = df.apply(lambda x: 1 if x['TBY_REFER_LIM'] in [1,2] and x['crf_parent_name'] == 'F_RESQ_IVT_TBY_CZ_2' else x['TBY_DONE'], axis=1)
        """

        # IVT TBY refer dido time
        # IVT_TBY_GROIN_TIME_MIN - HH:MM format
        # IVT_TBY_REFER_DIDO_TIME_MIN - calculated by script (minutes)
        df['IVT_TBY_REFER_DIDO_TIME'] = df.apply(lambda x: np.nan if x['IVT_TBY_REFER'] == 2 else x['IVT_TBY_REFER_DIDO_TIME'], axis=1)

        if ('IVT_TBY_REFER_ADMISSION_TIME' in df.columns and 'IVT_TBY_REFER_DISCHARGE_TIME' in df.columns):

            df['IVT_TBY_REFER_DIDO_TIME_MIN'], df['IVT_TBY_REFER_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_REFER_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_REFER_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['IVT_TBY_REFER'] == 2) else (0, False), axis=1))

        # Create TBY_REFER column
        # TBY_REFER_ADMISSION_TIME - HH:MM format
        # TBY_REFER_DISCHARGE_TIME - HH:MM format
        # TBY_REFER_DIDO_TIME - minutes
        # TBY_REFER_DIDO_TIME_MIN - calculated by script (minutes)
        df['TBY_REFER_DIDO_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER'] == 2 else x['TBY_REFER_DIDO_TIME'], axis=1)        

        # TBY refer dido time
        if ('TBY_REFER_DISCHARGE_TIME' in df.columns and 'TBY_REFER_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_DIDO_TIME_MIN'], df['TBY_REFER_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER'] == 2) else (0, False), axis=1))

        # Create TBY_REFER_ALL column
        # TBY_REFER_ALL_ADMISSION_TIME - HH:MM format
        # TBY_REFER_ALL_DISCHARGE_TIME - HH:MM format
        # TBY_REFER_ALL_DIDO_TIME - minutes
        # TBY_REFER_ALL_DIDO_TIME_MIN - calculated by script (minutes)
        # Set nan to TBY_REFER_ALL_DIDO_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        df['TBY_REFER_ALL_DIDO_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER_ALL'] == 2 else x['TBY_REFER_ALL_DIDO_TIME'], axis=1)

        # TBY refer all dido time
        if ('TBY_REFER_ALL_DISCHARGE_TIME' in df.columns and 'TBY_REFER_ALL_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_ALL_DIDO_TIME_MIN'], df['TBY_REFER_ALL_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ALL_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_ALL_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_ALL'] == 2) else (0, False), axis=1))

        # Create TBY_REFER_LIM column
        # TBY_REFER_LIM_ADMISSION_TIME - HH:MM format
        # TBY_REFER_LIM_DISCHARGE_TIME - HH:MM format
        # TBY_REFER_LIM_DIDO_TIME - minutes
        # TBY_REFER_LIM_DIDO_TIME_MIN - calculated by script (minutes)
        # Set nan to TBY_REFER_LIM_DIDO_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        df['TBY_REFER_LIM_DIDO_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER_LIM'] == 2 else x['TBY_REFER_LIM_DIDO_TIME'], axis=1)

        # TBY refer lim dido time
        if ('TBY_REFER_LIM_DISCHARGE_TIME' in df.columns and 'TBY_REFER_LIM_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_LIM_DIDO_TIME_MIN'], df['TBY_REFER_LIM_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_LIM_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_LIM_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_LIM'] == 2) else (0, False), axis=1))
  
        # Check if time columns are in dataframe, if not create them and fill with NaN values
        if ('IVT_TBY_REFER_NEEDLE_TIME' not in df.columns):
            df['IVT_TBY_REFER_NEEDLE_TIME'] = np.nan
        if ('IVT_TBY_NEEDLE_TIME' not in df.columns):
            df['IVT_TBY_NEEDLE_TIME'] = np.nan
        if ('TBY_ONLY_GROIN_TIME' not in df.columns):
            df['TBY_ONLY_GROIN_TIME'] = np.nan
        if ('IVT_TBY_GROIN_TIME' not in df.columns):
            df['IVT_TBY_GROIN_TIME'] = np.nan
        if ('IVT_TBY_REFER_DIDO_TIME' not in df.columns):
            df['IVT_TBY_REFER_DIDO_TIME'] = np.nan
        if ('TBY_REFER_DIDO_TIME' not in df.columns):
            df['TBY_REFER_DIDO_TIME'] = np.nan
        if ('TBY_REFER_ALL_DIDO_TIME' not in df.columns):
            df['TBY_REFER_ALL_DIDO_TIME'] = np.nan
        if ('TBY_REFER_LIM_DIDO_TIME' not in df.columns):
            df['TBY_REFER_LIM_DIDO_TIME'] = np.nan
        # Check if all column in minutes were created in previous steps, if not create column and values replace with NaN values. Also check if column _CHANGED exists if not create this column and fill with "False" value
        if ('IVT_ONLY_NEEDLE_TIME_MIN' not in df.columns):
            df['IVT_ONLY_NEEDLE_TIME_MIN'] = np.nan
            df['IVT_ONLY_NEEDLE_TIME_CHANGED'] = False
        if ('IVT_TBY_NEEDLE_TIME_MIN' not in df.columns):
            df['IVT_TBY_NEEDLE_TIME_MIN'] = np.nan
            df['IVT_TBY_NEEDLE_TIME_MIN_CHANGED'] = False
        if ('IVT_TBY_REFER_NEEDLE_TIME_MIN' not in df.columns):
            df['IVT_TBY_REFER_NEEDLE_TIME_MIN'] = np.nan
            df['IVT_TBY_REFER_NEEDLE_TIME_MIN_CHANGED'] = False
        if ('TBY_ONLY_GROIN_TIME_MIN' not in df.columns):
            df['TBY_ONLY_GROIN_TIME_MIN'] = np.nan
            df['TBY_ONLY_GROIN_TIME_MIN_CHANGED'] = False
        if ('IVT_TBY_GROIN_TIME_MIN' not in df.columns):
            df['IVT_TBY_GROIN_TIME_MIN'] = np.nan
            df['IVT_TBY_GROIN_TIME_MIN_CHANGED'] = False
        if ('IVT_TBY_REFER_DIDO_TIME_MIN' not in df.columns):
            df['IVT_TBY_REFER_DIDO_TIME_MIN'] = np.nan
            df['IVT_TBY_REFER_DIDO_TIME_MIN_CHANGED'] = False
        if ('TBY_REFER_DIDO_TIME_MIN' not in df.columns):
            df['TBY_REFER_DIDO_TIME_MIN'] = np.nan
            df['TBY_REFER_DIDO_TIME_MIN_CHANGED'] = False
        if ('TBY_REFER_ALL_DIDO_TIME_MIN' not in df.columns):
            df['TBY_REFER_ALL_DIDO_TIME_MIN'] = np.nan
            df['TBY_REFER_ALL_DIDO_TIME_MIN_CHANGED'] = False
        if ('TBY_REFER_LIM_DIDO_TIME_MIN' not in df.columns):
            df['TBY_REFER_LIM_DIDO_TIME_MIN'] = np.nan
            df['TBY_REFER_LIM_DIDO_TIME_MIN_CHANGED'] = False
        if ('TBY_ONLY_GROIN_PUNCTURE_TIME' not in df.columns):
            df['TBY_ONLY_GROIN_PUNCTURE_TIME'] = np.nan
            df['TBY_ONLY_GROIN_PUNCTURE_TIME_CHANGED'] = False

        return df


    def _get_times_in_minutes(self, admission_time, bolus_time, hosp_time, max_time):
        """ The function calculating difference between times in minutes. 

        :param admission_time: the time of admission
        :type admission_time: time
        :param bolus_time: the time of needle time
        :type bolus_time: time
        :param hosp_time: the time of hospitalization
        :type hosp_time: time
        :param max_time: the maximum time which is realistic for the type of the recanalization treatment
        :type max_time: int
        :returns: the calculated difference in minutes, `True` if time has been fixed else `False`
        """
        
        fixed = False
        timeformat = '%H:%M:%S'
        
        if admission_time == 'None' or admission_time == None or admission_time == 'nan' or pd.isnull(admission_time):
            admission_bool = False
        else:
            admission_bool = True
        
        if bolus_time == 'None' or bolus_time == None or bolus_time == 'nan' or pd.isnull(bolus_time):
            bolus_bool = False
        else:
            bolus_bool = True
        
        if hosp_time == 'None' or hosp_time == None or hosp_time == 'nan' or pd.isnull(hosp_time):
            hosp_bool = False
        else:
            hosp_bool = True

        #print(admission_time, type(admission_time), admission_bool, bolus_time, type(bolus_time), bolus_bool, hosp_time, type(hosp_time),hosp_bool)

        # If admission time and bolus time are Nan, difference is set to 0
        if admission_bool == False and bolus_bool == False:
            tdeltaMin = 0
            fixed = True
        # If only admission time is not filled, hospital time is used as admission time. If difference is < 0, then add 1 day.
        elif admission_bool == False and hosp_bool:
            tdelta = datetime.strptime(bolus_time, timeformat) - datetime.strptime(hosp_time, timeformat)
            tdeltaMin = tdelta.total_seconds()/60.0
            if (tdeltaMin < 0):
                tdelta += timedelta(days=1)
                tdeltaMin = tdelta.total_seconds()/60.0
            fixed = True

        elif admission_bool == False and hosp_bool == False and bolus_bool:
            tdeltaMin = 0
            fixed = True
        # If bolus time is Nan, tdelta in min is set to 0
        elif bolus_bool == False:
            tdeltaMin = 0
            fixed = True
        # Else, delta is calculated bolus time - admission time. If delta is > max time (400 for needle time, 700 for groin time) then check if hospital time is not uknown and if difference between bolus time and hospital time is < max time, if yes the time is calculated as bolus time - hospital time. If bolus time - hospital time > max time or bolus time - admission time, keep bolus time - admission time.
        else:
            tdelta = datetime.strptime(bolus_time, timeformat) - datetime.strptime(admission_time, timeformat)
            tdeltaMin = tdelta.total_seconds()/60.0
            if tdeltaMin < 0:
                tdelta += timedelta(days=1)
                tdeltaMin = tdelta.total_seconds()/60.0

            if tdeltaMin > max_time and hosp_time == True:
                if (datetime.strptime(hosp_time, timeformat) != datetime.strptime(admission_time, timeformat) and datetime.strptime(hosp_time, timeformat) > datetime.strptime(admission_time, timeformat)):
                    tdelta = datetime.strptime(bolus_time, timeformat) - datetime.strptime(hosp_time, timeformat)
                    tdeltaMin = tdelta.total_seconds()/60.0
                    if (tdeltaMin < 0):
                        tdelta += timedelta(days=1)
                        tdeltaMin = tdelta.total_seconds()/60.0
                    fixed = True

        return tdeltaMin, fixed

    



        

    
