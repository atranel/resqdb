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
    """ A check of times and days with one property: a dataframe. """

    def __init__(self, df, nprocess=None):
        
        # Create log file in the working folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
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

    def get_preprocessed_data(self, df, n=None, name=None):         
        
        if self.nprocess is not None:
            """ Return preprocessed data. """
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
            """ Return preprocessed data. """
            preprocessed_data = df.copy()
            print(preprocessed_data)
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
            print(preprocessed_data['HOSPITAL_DATE', 'HOSPITAL_DATE_OLD', 'DISCHARGE_DATE', 'DISCHARGE_DATE_OLD'])
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
        """Gets fixed date

        Args:
            visit_date: The visit date.
            hosp_date: The hospital date.
            disc_date: The discharge date. 
        Returns: 
            The fixed dates if it was possible to fix the date else return the old date.
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
        """Gets the difference between two dates in days. 

        Args:
            hosp_date: The hospital date.
            disc_date: The discharge date. 
        Returns:
            The calculated number of days of stay in the hospital.
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
        """ Call this function to calculate times (needle time, bolus time, etc.) 

        Args: 
            df: The preprocessed dataframe
        Returns:
            The preprocessed dataframe with added columns.
        """
        # IVT_ONLY - 1) filled in minutes, 2) filled admission and bolus time
        # IVT_ONLY_ADMISSION_TIME - HH:MM format
        # IVT_ONLY_BOLUS_TIME - HH:MM format
        # IVT_ONLY_NEEDLE_TIME - minutes
        # IVT_ONLY_NEEDLE_TIME_MIN - calculated by script (minutes) 
        
        df['IVT_ONLY_NEEDLE_TIME'] = df.apply(lambda x: np.nan if x['IVT_ONLY'] == 2 else x['IVT_ONLY_NEEDLE_TIME'], axis=1) # (delete values calculated by Mirek)
        # IVT needle time
        if ('IVT_ONLY_BOLUS_TIME' in df.columns and 'IVT_ONLY_ADMISSION_TIME' in df.columns):

            df['IVT_ONLY_NEEDLE_TIME_MIN'], df['IVT_ONLY_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=
                str(x['IVT_ONLY_ADMISSION_TIME']), bolus_time=str(x['IVT_ONLY_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if (x['IVT_ONLY'] == 2) else (0, False), axis=1))

        # Create IVT_TBY column
        # IVT_TBY - 1) filled in minutes, 2) filled admission, bolus and groin puncture time
        # IVT_TBY_NEEDLE_TIME - Mirek's calculation of needle time
        # IVT_TBY_ADMISSION_TIME - HH:MM format
        # IVT_TBY_BOLUS_TIME - HH:MM format
        # IVT_TBY_GROIN_PUNCTURE_TIME - HH:MM format
        # IVT_TBY_NEEDLE_TIME in minutes
        # IVT_TBY_NEEDLE_TIME_MIN - calculated by script (minutes) (delete values calculated by Mirek)
        df['IVT_TBY_NEEDLE_TIME'] = df.apply(lambda x: np.nan if x['IVT_TBY'] == 2 else x['IVT_TBY_NEEDLE_TIME'], axis=1) #(delete values calculated by Mirek)

        # IVT TBY needle time
        if ('IVT_TBY_ADMISSION_TIME' in df.columns and 'IVT_TBY_BOLUS_TIME' in df.columns):

            df['IVT_TBY_NEEDLE_TIME_MIN'], df['IVT_TBY_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if(x['IVT_TBY'] == 2) else (0, False), axis=1))

        # Create IVT_TBY_REFER column
        # IVT_TBY_REFER_ADMISSION_TIME - HH:MM format
        # IVT_TBY_REFER_ADMISSION_TIME - HH:MM format
        # IVT_TBY_REFER_NEEDLE_TIME - minutes
        # IVT_TBY_REFER_NEEDLE_TIME_MIN - calculated by script (minutes) (delete values calculated by Mirek)
        df['IVT_TBY_REFER_NEEDLE_TIME'] = df.apply(lambda x: np.nan if x['IVT_TBY_REFER'] == 2 else x['IVT_TBY_REFER_NEEDLE_TIME'], axis=1) # (delete values calculated by Mirek)

        # IVT TBY refer needle time
        if ('IVT_TBY_REFER_ADMISSION_TIME' in df.columns and 'IVT_TBY_REFER_BOLUS_TIME' in df.columns):

            df['IVT_TBY_REFER_NEEDLE_TIME_MIN'], df['IVT_TBY_REFER_NEEDLE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_REFER_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_REFER_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=400) if(x['IVT_TBY_REFER'] == 2) else (0, False), axis=1))

        # Create TBY_ONLY column
        # TBY_ONLY_ADMISSION_TIME - HH:MM format
        # TBY_ONLY_PUNCTURE_TIME - HH:MM format
        # TBY_ONLY_GROIN_PUNCTURE_TIME - minutes
        # TBY_ONLY_GROIN_TIME_MIN - calculated by script (minutes)
        df['TBY_ONLY_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: np.nan if x['TBY_ONLY'] == 2 else x['TBY_ONLY_GROIN_PUNCTURE_TIME'], axis=1)

        # TBY only groin time
        if ('TBY_ONLY_PUNCTURE_TIME' in df.columns and 'TBY_ONLY_ADMISSION_TIME' in df.columns):

            df['TBY_ONLY_GROIN_TIME_MIN'], df['TBY_ONLY_GROIN_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_ONLY_ADMISSION_TIME']), bolus_time=str(x['TBY_ONLY_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_ONLY'] == 2) else (0, False), axis=1))

        # IVT TBY groin puncture time
        # IVT_TBY_ADMISSION_TIME - HH:MM format
        # IVT_TBY_GROIN_PUNCTURE_TIME - HH:MM format
        # IVT_TBY_GROIN_TIME_MIN - calculated by script (minutes)
        df['IVT_TBY_GROIN_TIME'] = df.apply(lambda x: np.nan if x['IVT_TBY'] == 2 else x['IVT_TBY_GROIN_TIME'], axis=1)

        if ('IVT_TBY_ADMISSION_TIME' in df.columns and 'IVT_TBY_GROIN_PUNCTURE_TIME' in df.columns):

            df['IVT_TBY_GROIN_TIME_MIN'], df['IVT_TBY_GROIN_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['IVT_TBY_ADMISSION_TIME']), bolus_time=str(x['IVT_TBY_GROIN_PUNCTURE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['IVT_TBY'] == 2) else (0, False), axis=1))


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
        # Set nan to TBY_REFER_ALL_GROIN_PUNCTURE_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER_ALL'] == 2 else x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'], axis=1)
        # To calculate groin puncture from DIDO time (needed due to missing field in the prev version of form), take only times in minutes
        #df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_ALL_DIDO_TIME'] if 'IVT_TBY' in x['crf_parent_name'] and x['TBY_REFER_ALL'] == 1 else x['TBY_REFER_ALL_GROIN_PUNCTURE_TIME'], axis=1)

        # TBY refer all dido time
        if ('TBY_REFER_ALL_DISCHARGE_TIME' in df.columns and 'TBY_REFER_ALL_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_ALL_DIDO_TIME_MIN'], df['TBY_REFER_ALL_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ALL_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_ALL_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_ALL'] == 2) else (0, False), axis=1))

        # Calculate TBY refer all bolus time
        if ('TBY_REFER_ALL_BOLUS_TIME' in df.columns and 'TBY_REFER_ALL_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_ALL_GROIN_PUNCTURE_TIME_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_ALL_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_ALL_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_ALL'] == 2) else (0, False), axis=1))

        # Create TBY_REFER_ALL column
        # TBY_REFER_LIM_ADMISSION_TIME - HH:MM format
        # TBY_REFER_LIM_DISCHARGE_TIME - HH:MM format
        # TBY_REFER_LIM_DIDO_TIME - minutes
        # TBY_REFER_LIM_DIDO_TIME_MIN - calculated by script (minutes)
        # Set nan to TBY_REFER_LIM_DIDO_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        df['TBY_REFER_LIM_DIDO_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER_LIM'] == 2 else x['TBY_REFER_LIM_DIDO_TIME'], axis=1)
        # Set nan to TBY_REFER_LIM_GROIN_PUNCTURE_TIME if times in HH:MM were filled in (delete values calculated by Mirek)
        df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: np.nan if x['TBY_REFER_LIM'] == 2 else x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'], axis=1)
        # To calculate groin puncture from DIDO time (needed due to missing field in the prev version of form), take only times in minutes
        # df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: x['TBY_REFER_LIM_DIDO_TIME'] if 'IVT_TBY' in x['crf_parent_name'] and x['TBY_REFER_LIM'] == 1 else x['TBY_REFER_LIM_GROIN_PUNCTURE_TIME'], axis=1)

        # TBY refer lim dido time
        if ('TBY_REFER_LIM_DISCHARGE_TIME' in df.columns and 'TBY_REFER_LIM_ADMISSION_TIME' in df.columns):

            df['TBY_REFER_LIM_DIDO_TIME_MIN'], df['TBY_REFER_LIM_DIDO_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_LIM_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_LIM_DISCHARGE_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_LIM'] == 2) else (0, False), axis=1))

        # TBY refer lim groin puncture time
        if ('TBY_REFER_LIM_BOLUS_TIME' in df.columns and 'TBY_REFER_LIM_ADMISSION_TIME' in df.columns):
            df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN'], df['TBY_REFER_LIM_GROIN_PUNCTURE_TIME_MIN_CHANGED'] = zip(*df.apply(lambda x: self._get_times_in_minutes(admission_time=str(
                x['TBY_REFER_LIM_ADMISSION_TIME']), bolus_time=str(x['TBY_REFER_LIM_BOLUS_TIME']), hosp_time=str(x['HOSPITAL_TIME']), max_time=700) if(x['TBY_REFER_LIM'] == 2) else (0, False), axis=1))
  
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
        """ Calculate differnce between two times. 

        Args:
            admission_time: The admission time
            bolus_time: The time when treatment was provided
            hospital_time: The time of hospitalization
            max_time: The maximum time in which the treatment should be provided (minutes)
        Returns:
            The calculated difference in minutes.
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

    



        

    
