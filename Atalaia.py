#### Filename: Calculation.py
#### Version: v1.0
#### Author: Marie Jankujova
#### Date: March 5, 2019
#### Description: Calculation of Angels Awards for Spain sites which are using Atalaia form. 

import pandas as pd
import numpy as np
import sys
import os
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta
import logging
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name

class CheckTimes():

    def __init__(self, df, start_date=None, end_date=None):
        """ Calculate hospital days and check if the days are in the range from 0 to 300. If they are greater or lesser then check discharge and hospital date, and based on the logic fix hospital/discharge date. """

        # Create log file in the workign folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Running Calulation')   

        self.df = df.copy()
        self.start_date = start_date
        self.end_date = end_date

        # Get dataframe without null discharge dates and null hospital dates
        self.df = self.filter_null_dates()

        if not self.df.empty:
            # Calculate hospital days
            self.df['hospital_days'] = self.df.apply(lambda x: self.calculate_hospital_days(x['discharge_date_es'], x['hospital_date_es']), axis=1, result_type='expand')

            # Save negative hospital days
            self.df[self.df['hospital_days'] < 0].to_csv("negative_hospital_days.csv", sep=",")

            # Fix negative and too positive hospital days
            self.df[['hospital_days_fixed', 'hospital_date_fixed', 'discharge_date_fixed']] = self.df.apply(lambda x: self.fix_negative_hospital_days(discharge_date=x['discharge_date_es'], hospital_date=x['hospital_date_es']) if (x['hospital_days'] < 0 or x['hospital_days'] > 300) else (x['hospital_days'], x['hospital_date_es'], x['discharge_date_es']), axis=1, result_type='expand')       

            logging.info('Calculation: Negative or too positive hospital days were fixed.')

        else:
            logging.info('Calculation: Dataframe was filtered for empty hospital and discharge dates end there are no data.')
            sys.exit()
 
    def filter_null_dates(self):
        """ Return dataframe where null discharge dates or hospital dates were excluded. """
        # Remove rows with null discharge date
        df = self.df[~pd.isnull(self.df['discharge_date_es'])]
        # Remove rows with null hospital date
        df = df[~pd.isnull(df['hospital_date_es'])]

        logging.info('Calculation: Rows with empty discharge date or empty hospital date were removed.')

        return df

    def calculate_hospital_days(self, discharge_date, hospital_date):
        """ Return number of hospital days. 

        Params:
            discharge_date
            hospital_date

        Return:
            int: number of hospital days
        """
        # Calculate hospital days
        hospital_days = (discharge_date - hospital_date).days
        # If hospital days is 0, then replace by 1
        return 1 if hospital_days == 0 else hospital_days

    def fix_negative_hospital_days(self, discharge_date, hospital_date):
        """ Fix negative hospital days. 

        Params:
            discharge_date
            hospital_date

        Return: 
            int: fixed hospital days
            date: new hospital date
            date: new discharge date
        """
        # Calculate number of days
        hospital_days = (discharge_date - hospital_date).days
        # Save value for discharge and hospital date
        discharge_date_new = discharge_date
        hospital_date_new = hospital_date
        # Check hospital days, if hospital days < -300 mostly incorrect discharge year
        if hospital_days < -300:
            discharge_date_new = discharge_date + relativedelta(years=+1)
        # If hospital days > 300, probably incorrect hospital year
        elif hospital_days > 300:
            hospital_date_new = hospital_date + relativedelta(years=+1)
        # This case means that discharge date is one or two month before the month of hospitalization
        elif hospital_days >= -31 and hospital_days < 0:
            discharge_date_new = discharge_date + relativedelta(months=+1)
        elif hospital_days > -60 and hospital_days < -31:
            discharge_date_new = discharge_date + relativedelta(months=+2)
        else:
            discharge_date_new = discharge_date
        # Check fixed hospital days, if hospital days is 0, then change to 1 day
        hospital_days_fixed = (discharge_date_new - hospital_date_new).days
        if hospital_days_fixed == 0:
            hospital_days_fixed = 1

        return hospital_days_fixed, hospital_date_new, discharge_date_new


class Filtration(CheckTimes):
    """ Filter dataframe by discharge date. """

    def filter_by_date(self):
        """ 
        Return dataframe which contains only rows where discharge date was between these two dates. The dataframe is filter by discharge date. 
        
        Params: 
            start_date: the first day included in the dataframe <start_date,end_date>
            end_date: the last day included in the dataframe.
        """
        df = self.df[(self.df['discharge_date_es'] >= self.start_date) & (self.df['discharge_date_es'] <= self.end_date)]

        logging.info('Calculation: Raw data were filtered and include only rows with discharge date between {0} and {1}.'.format(self.start_date, self.end_date))

        return df


class Calculation(Filtration):
   
    def get_total_patients(self):
        """ Calculate total patients per site. Return dataframe with total patients grouped by Site ID. 
        
        Return:
            dataframe
                Return dataframe grouped by Site ID and Total Patients as second column.
        """
        logging.info('Calculation: Get total patients.')
        try:
            self.stats_df = self.df.groupby(['site_id', 'facility_name']).size().reset_index(name="# total patients")
            logging.info('Total patients: OK.')
        except: 
            logging.info('Total patients: ERROR.')

    def get_recan_below(self, dtn, dtg, top):
        """ Get True/False if bigger from pair DTN and DTN is < max. """
        if dtn == 0 and dtg != 0:
            if dtg > top or dtg == 0 or dtg < 0:
                return False
            else:
                return True
        elif dtn != 0 and dtg == 0:
            if dtn > top or dtn == 0 or dtn < 0:
                return False
            else:
                return True
                
        else:
            minimum = min([dtn, dtg])
            if minimum > top or minimum == 0:
                return False
            elif minimum < 0:
                maximum = max([dtn, dtg])
                if maximum > top or maximum == 0 or maximum < 0:
                    return False
                else:
                    return True
            else:
                return True

    def get_recan_therapy(self):
        """ Return dataframe with patients treated with door to recanalization therapy time < 60 minutes. """
        try:
            # Filter recanalization procedures 
            #recan_df = self.df[self.df['recanalization_procedures_es'].isin([1,2])].copy()
            thrombolysis_df = self.df[self.df['recanalization_procedures_es'].isin([1,2])].copy()
            thrombectomy_df = self.df[self.df['recanalization_procedures_es'].isin([3,4])].copy()

            if not thrombolysis_df.empty:
                # Calculate DTN if the patient got IV tPa
                thrombolysis_df['DTN_IVT_ONLY'] = thrombolysis_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_only_bolus_time_es']) if (x['recanalization_procedures_es'] == 1 and x['ivt_only_bolus_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Calculate DTN if the patient got IVtPa and TBY
                thrombolysis_df['DTN_IVT_TBY'] = thrombolysis_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_tby_bolus_time_es']) if (x['recanalization_procedures_es'] == 2 and x['ivt_tby_bolus_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Sum two columns with DTN in one
                thrombolysis_df['DTN'] = thrombolysis_df.apply(lambda x: x['DTN_IVT_ONLY'] + x['DTN_IVT_TBY'], axis=1, result_type='expand')

                thrombolysis_df = thrombolysis_df[(thrombolysis_df['DTN'] > 0)]

                if not thrombolysis_df.empty:
                    thrombolysis_pts = thrombolysis_df.groupby(['site_id']).size().reset_index(name="# patients eligible thrombolysis")
                    thrombolysis_df['recan_below_60'] =  thrombolysis_df.apply(lambda x: self.get_recan_below(x['DTN'], 0, 60), axis=1) 
                    # Get only patients with DTN < 60 or DTG < 60
                    recan_below_60_df = thrombolysis_df[thrombolysis_df['recan_below_60'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombolysis < 60 minutes')
                    # Merge with recan_patients

                    tmp = pd.merge(thrombolysis_pts, recan_below_60_df, how="left", on="site_id")

                    # Calculate % for DTN or DTG < 60
                    tmp['% patients treated with door to thrombolysis < 60 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombolysis < 60 minutes']/x['# patients eligible thrombolysis'])*100,2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)

                    # Get only patients with DTN < 45
                    #recan_df['recan_below_45'] = recan_df.apply(lambda x: self.get_recan_below(x['DTN'], x['DTG'], 45), axis=1)
                    thrombolysis_df['recan_below_45'] = thrombolysis_df.apply(lambda x: self.get_recan_below(x['DTN'], 0, 45), axis=1)
                    # Get only patients with DTN below 45
                    recan_below_45_df = thrombolysis_df[thrombolysis_df['recan_below_45'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombolysis < 45 minutes')
                    # Merge with recan_patients
                    tmp = pd.merge(tmp, recan_below_45_df, how="left", on="site_id")
                    # Calculate % for DTN or DTG < 45
                    tmp['% patients treated with door to thrombolysis < 45 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombolysis < 45 minutes']/x['# patients eligible thrombolysis'])*100,2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)
                    # Add line in log
                    logging.info('Calculation: Thrombolysis time < 60 minutes and < 45 minutes has been calculated.')
                    # Remove temporary column
                    self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")  
            else:
                self.stats_df['# patients treated with door to thrombolysis < 60 minutes'] = 0
                self.stats_df['% patients treated with door to thrombolysis < 60 minutes'] = 0
                self.stats_df['# patients treated with door to thrombolysis < 45 minutes'] = 0
                self.stats_df['% patients treated with door to thrombolysis < 45 minutes'] = 0

            if not thrombectomy_df.empty:
                # Calculate DTG if the patient got IVtPa and TBY
                thrombectomy_df['DTG_IVT_TBY'] = thrombectomy_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_tby_groin_puncture_time_es']) if (x['recanalization_procedures_es'] == 2 and x['ivt_tby_groin_puncture_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Calculate DTG if the patient got TBY
                thrombectomy_df['DTG_TBY'] = thrombectomy_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['tby_only_puncture_time_es']) if (x['recanalization_procedures_es'] == 3 and x['tby_only_puncture_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Sum two columns with DTG in one
                thrombectomy_df['DTG'] = thrombectomy_df.apply(lambda x: x['DTG_IVT_TBY'] + x['DTG_TBY'], axis=1, result_type='expand')

                thrombectomy_df = thrombectomy_df[(thrombectomy_df['DTG'] > 0)]

                if not thrombectomy_df.empty:
                    thrombectomy_pts = thrombectomy_df.groupby(['site_id']).size().reset_index(name="# patients eligible thrombectomy")
                    thrombectomy_df['recan_below_90'] =  thrombectomy_df.apply(lambda x: self.get_recan_below(x['DTG'], 0, 90), axis=1) 
                    # Get only patients with DTN < 60 or DTG < 60
                    recan_below_90_df = thrombectomy_df[thrombectomy_df['recan_below_90'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombectomy < 90 minutes')
                    # Merge with recan_patients

                    tmp = pd.merge(thrombectomy_pts, recan_below_90_df, how="left", on="site_id")

                    # Calculate % for DTN or DTG < 60
                    tmp['% patients treated with door to thrombectomy < 90 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombectomy < 90 minutes']/x['# patients eligible thrombectomy'])*100,2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)

                    # Get only patients with DTN < 45
                    #recan_df['recan_below_45'] = recan_df.apply(lambda x: self.get_recan_below(x['DTN'], x['DTG'], 45), axis=1)
                    thrombectomy_df['recan_below_45'] = thrombectomy_df.apply(lambda x: self.get_recan_below(x['DTG'], 0, 60), axis=1)
                    # Get only patients with DTN below 45
                    recan_below_60_df = thrombectomy_df[thrombectomy_df['recan_below_45'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombectomy < 60 minutes')
                    # Merge with recan_patients
                    tmp = pd.merge(tmp, recan_below_60_df, how="left", on="site_id")
                    # Calculate % for DTN or DTG < 45
                    tmp['% patients treated with door to thrombectomy < 60 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombectomy < 60 minutes']/x['# patients eligible thrombectomy'])*100,2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)
                    # Add line in log
                    logging.info('Calculation: Thrombectomy time < 90 minutes and < 60 minutes has been calculated.')
                    # Remove temporary column
                    self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id") 
            else:
                self.stats_df['# patients treated with door to thrombectomy < 90 minutes'] = 0
                self.stats_df['% patients treated with door to thrombectomy < 90 minutes'] = 0
                self.stats_df['# patients treated with door to thrombectomy < 60 minutes'] = 0
                self.stats_df['% patients treated with door to thrombectomy < 60 minutes'] = 0
            
            logging.info('Recanalization procedures: OK')
        except:
            logging.info('Recanalization procedures: ERROR')


    def get_recan_rate(self):
        """ Return dataframe expanded on recanalization rate. """
        try:
            # Get patients with ishemic stroke (stroke_type=1)
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])]
            # Get ischemic patients who received recanalization procedure (recanalization_procedures_es=1,2,3)
            recan_rate_df = ischemic_df[ischemic_df['recanalization_procedures_es'].isin([1,2,3])]
            # Get number of patients per site for ischemic patients
            ischemic_pts = ischemic_df.groupby(['site_id']).size().reset_index(name="tmp_patients")
            if not recan_rate_df.empty:
                # Calculate total recanalization rate patients
                recan_rate_pts = recan_rate_df.groupby(['site_id']).size().reset_index(name='# recanalization rate out of total ischemic incidence')
                # Merge both ischemic_pts and recan_rate_pts - left merge
                tmp = pd.merge(recan_rate_pts, ischemic_pts, how="left", on="site_id")
                # Calculate %
                tmp['% recanalization rate out of total ischemic incidence'] = tmp.apply(lambda x: round((x['# recanalization rate out of total ischemic incidence']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# recanalization rate out of total ischemic incidence'] = 0
                self.stats_df['% recanalization rate out of total ischemic incidence'] = 0
        
            logging.info('Recanalization rate: OK')
        except:
            logging.info('Recanalization rate: ERROR')
        
    def get_ct_mri(self):
        """ Return dataframe expanded on CT/MRI columns. """
        try:
            # Get only IS, TIA and ICH patients who have undergone CT/MRI 
            ct_mri_df = self.df[(self.df['stroke_type_es'].isin([1,2,3]) & self.df['ct_mri_es'].isin([1]))]
            # Get only IS, TIA and ICH patients & calculate total tmp patients
            is_tia_ich_df = self.df[self.df['stroke_type_es'].isin([1,2,3])].groupby(['site_id']).size().reset_index(name="tmp_patients")
            # Check if any patients got CT/MRi
            if not ct_mri_df.empty:
                # Calculate total IS, TIA and ICH with performed CT/MRI
                tmp = ct_mri_df.groupby(['site_id']).size().reset_index(name='# suspected stroke patients undergoing CT/MRI')
                # Merge both dataframees - left merge
                tmp = pd.merge(tmp, is_tia_ich_df, how="left", on="site_id")
                # Calculate percentage value
                tmp['% suspected stroke patients undergoing CT/MRI'] = tmp.apply(lambda x: round((x['# suspected stroke patients undergoing CT/MRI']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# suspected stroke patients undergoing CT/MRI'] = 0
                self.stats_df['% suspected stroke patients undergoing CT/MRI'] = 0
            logging.info('CT/MRI: OK')
        except:
            logging.info('CT/MRI: ERROR')
    
    def get_dysphagia_screening(self):
        """ Return dataframe for all patients who underwent dysphagia screening. """
        try:
            # Filter dataframe for IS and ICH patients and dypshagia screening (GUSS test or other test)
            dysphagia_df = self.df[(self.df['stroke_type_es'].isin([1,2]) & self.df['dysphagia_screening_es'].isin([1,2]))]
            # Filter dataframe for IS and ICH patients and dysphagia screening (GUSS test, other test and not tested)
            dysphagia_ntest_df = self.df[(self.df['stroke_type_es'].isin([1,2]) & self.df['dysphagia_screening_es'].isin([1,2,4]))]
            # Calculate total tmp patients
            dysphagia_ntest_tmp_df = dysphagia_ntest_df.groupby(['site_id']).size().reset_index(name='tmp_patients')
            # Check if dysphagia dataframe is not empty
            if not dysphagia_df.empty:
                # Calculate total patients for IS and ICH patients and dysphagia screeening (GUSS test or other test)
                tmp = dysphagia_df.groupby(['site_id']).size().reset_index(name='# all stroke patients undergoing dysphagia screening')
                # Merge both temporary dataframe - left merge
                tmp = pd.merge(tmp, dysphagia_ntest_tmp_df, how="left", on="site_id")
                # Calculate percentage value
                tmp['% all stroke patients undergoing dysphagia screening'] = tmp.apply(lambda x: round((x['# all stroke patients undergoing dysphagia screening']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                # Merge dataframe with result stats
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# all stroke patients undergoing dysphagia screening'] = 0
                self.stats_df['% all stroke patients undergoing dysphagia screening'] = 0
            logging.info('Dysphagia screening: OK')
        except:
            logging.info('Dysphagia screening: ERROR')

    def get_patients_discharged_with_antiplatelets(self):
        """ Return dataframe with ischemic patients who have been discharged with prescribed antiplatelets. """
        try:
            # Get patients with ishemic stroke (stroke_type=1)
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])]
            # Filter dataframe for ischemic patients who had not determined or had unknown afib, were discharged but not dead and had prescribed antiplatelets. 
            antiplatelets_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ~ischemic_df['discharge_destination_es'].isin([5]) & ischemic_df['antithrombotics_es'].isin([1]))].copy()
            # Filter dataframe for patients not detected or not known for afib, discharged but not dead and not recommended antithrobmotics
            antiplatelets_recs_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ~ischemic_df['discharge_destination_es'].isin([5]) & ~ischemic_df['antithrombotics_es'].isin([9]))].copy()
            # Calculate total patients
            antiplatelets_recs_tmp_df = antiplatelets_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')
            # Check if antiplatelets dataframe is not empty
            if not antiplatelets_df.empty:
                # Calculate total patients who were discharged (not dead), not detected or not known for afbi and prescirbed for antiplatelets
                tmp = antiplatelets_df.groupby(['site_id']).size().reset_index(name='# ischemic stroke patients discharged with antiplatelets')
                # Merge both temporary dataframe - left merge
                tmp = pd.merge(tmp, antiplatelets_recs_tmp_df, how="left", on="site_id")
                # Calculate percentage value
                tmp['% ischemic stroke patients discharged with antiplatelets'] = tmp.apply(lambda x: round((x['# ischemic stroke patients discharged with antiplatelets']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                # Merge dataframe with result stats
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# ischemic stroke patients discharged with antiplatelets'] = 0
                self.stats_df['% ischemic stroke patients discharged with antiplatelets'] = 0
            logging.info('Discharged with antiplatelets: OK')
        except:
            logging.info('Discharged with antiplatelets: ERROR')
        
        try:
            # Get patients with ishemic stroke (stroke_type=1)
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])]
            # Filter dataframe for ischemic patients who had not determined or had unknown afib, were discharged but not dead and had prescribed antiplatelets. 
            antiplatelets_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ischemic_df['discharge_destination_es'].isin([1]) & ischemic_df['antithrombotics_es'].isin([1]))].copy()
            # Filter dataframe for patients not detected or not known for afib, discharged but not dead and not recommended antithrobmotics
            antiplatelets_recs_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ischemic_df['discharge_destination_es'].isin([1]) & ~ischemic_df['antithrombotics_es'].isin([9]))].copy()
            # Calculate total patients
            antiplatelets_recs_tmp_df = antiplatelets_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')
            # Check if antiplatelets dataframe is not empty
            if not antiplatelets_df.empty:
                # Calculate total patients who were discharged (not dead), not detected or not known for afbi and prescirbed for antiplatelets
                tmp = antiplatelets_df.groupby(['site_id']).size().reset_index(name='# ischemic stroke patients discharged home with antiplatelets')
                # Merge both temporary dataframe - left merge
                tmp = pd.merge(tmp, antiplatelets_recs_tmp_df, how="left", on="site_id")
                # Calculate percentage value
                tmp['% ischemic stroke patients discharged home with antiplatelets'] = tmp.apply(lambda x: round((x['# ischemic stroke patients discharged home with antiplatelets']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                # Merge dataframe with result stats
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# ischemic stroke patients discharged home with antiplatelets'] = 0
                self.stats_df['% ischemic stroke patients discharged home with antiplatelets'] = 0
            logging.info('Discharged with antiplatelets: OK')
        except:
            logging.info('Discharged with antiplatelets: ERROR')


        self.stats_df['# ischemic stroke patients discharged (home) with antiplatelets'] = self.stats_df.apply(lambda x: x['# ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['# ischemic stroke patients discharged home with antiplatelets'], axis=1)
        self.stats_df['% ischemic stroke patients discharged (home) with antiplatelets'] = self.stats_df.apply(lambda x: x['% ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['% ischemic stroke patients discharged home with antiplatelets'], axis=1)

        #self.stats_df.drop(['# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets'], axis=1, inplace=True)

    def get_afib_discharged_with_anticoagulants(self):
        """ Return dataframe with patients who have been discharged with anticoagulant and were detected for aFib. """
        try:
            # Filter dataframe for patients detected for afib, discharged but not dead and prescribed antithrombotics
            anticoagulants_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & ~self.df['discharge_destination_es'].isin([5]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8]))].copy()
            # Filter dataframe for patients detected for afib, discharged but not dead and prescribed antithrombotics including not prescribed at all
            anticoagulants_recs_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & ~self.df['discharge_destination_es'].isin([5]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8,10]))].copy()
            # Calculate total patients 
            anticoagulants_recs_tmp_df = anticoagulants_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')
            # Check if anticoagulants dataframe is not empty
            if not anticoagulants_df.empty:    
                # Calculate total patients who were discharged (not dead), detected for afib and prescirbed with anticoagulants
                tmp = anticoagulants_df.groupby(['site_id']).size().reset_index(name='# afib patients discharged with anticoagulants')
                # Merge both temporary dataframes 
                tmp = pd.merge(tmp, anticoagulants_recs_tmp_df, how="left", on="site_id")
                # Caculate percentage value
                tmp['% afib patients discharged with anticoagulants'] = tmp.apply(lambda x: round((x['# afib patients discharged with anticoagulants']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove reduntant temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                # Merge with stats df
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# afib patients discharged with anticoagulants'] = 0
                self.stats_df['% afib patients discharged with anticoagulants'] = 0

            logging.info('Discharged with anticoagulants: OK')
        except:
            logging.info('Discharged with anticoagulants: ERROR')

        try:
            # Filter dataframe for patients detected for afib, discharged but not dead and prescribed antithrombotics
            anticoagulants_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & self.df['discharge_destination_es'].isin([1]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8]))].copy()
            # Filter dataframe for patients detected for afib, discharged but not dead and prescribed antithrombotics including not prescribed at all
            anticoagulants_recs_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & self.df['discharge_destination_es'].isin([1]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8,10]))].copy()
            # Calculate total patients 
            anticoagulants_recs_tmp_df = anticoagulants_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')
            # Check if anticoagulants dataframe is not empty
            if not anticoagulants_df.empty:    
                # Calculate total patients who were discharged (not dead), detected for afib and prescirbed with anticoagulants
                tmp = anticoagulants_df.groupby(['site_id']).size().reset_index(name='# afib patients discharged home with anticoagulants')
                # Merge both temporary dataframes 
                tmp = pd.merge(tmp, anticoagulants_recs_tmp_df, how="left", on="site_id")
                # Caculate percentage value
                tmp['% afib patients discharged home with anticoagulants'] = tmp.apply(lambda x: round((x['# afib patients discharged home with anticoagulants']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                # Remove reduntant temporary column
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                # Merge with stats df
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# afib patients discharged home with anticoagulants'] = 0
                self.stats_df['% afib patients discharged home with anticoagulants'] = 0

            logging.info('Discharged with anticoagulants: OK')
        except:
            logging.info('Discharged with anticoagulants: ERROR')

        self.stats_df['# afib patients discharged (home) with anticoagulants'] = self.stats_df.apply(lambda x: x['# afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['# afib patients discharged home with anticoagulants'], axis=1)
        self.stats_df['% afib patients discharged (home) with anticoagulants'] = self.stats_df.apply(lambda x: x['% afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['% afib patients discharged home with anticoagulants'], axis=1)

        #self.stats_df.drop(['# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants'], axis=1, inplace=True)

    def get_hospitalized_in(self):
        """ Return dataframe with stroke patients hospitalized in a dedicated stroke unit / ICU. """
        try:
            # Get patient hospitalized in strok unit
            hosp_df = self.df[self.df['hospitalized_in_es'].isin([1])].copy()
            # Check if hospitalization dataframe is not empty
            if not hosp_df.empty:
                tmp = hosp_df.groupby(['site_id']).size().reset_index(name="# stroke patients treated in a dedicated stroke unit / ICU")
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
                self.stats_df['% stroke patients treated in a dedicated stroke unit / ICU'] = self.stats_df.apply(lambda x: round((x['# stroke patients treated in a dedicated stroke unit / ICU']/x['# total patients'])*100, 2) if x['# total patients'] > 0 else 0, axis=1)
            else:
                self.stats_df['# stroke patients treated in a dedicated stroke unit / ICU'] = 0
                self.stats_df['% stroke patients treated in a dedicated stroke unit / ICU'] = 0

            logging.info('Hospitalized in stroke unit: OK')
        except:
            logging.info('Hospitalized in stroke unit: ERROR')

    def _get_final_award(self, x):
        ''' Get the final award. Based on values in the given columns calculate the proposed award for each row in the resulted statistics. 
        
        Args:
            x: the row from the dataframe (self.angels_awards_tmp)
        Returns:
            award: the proposed award (NONE, DIAMOND, PLATINUM, GOLD)
        '''
        # Check if site gets some award
        if x['Total Patients'] == False:
            award = "NONE"
        else:
            award = "TRUE"

        thrombolysis_pts = x['# patients eligible thrombolysis']
        
        thrombolysis_therapy_lt_60min = x['% patients treated with door to thrombolysis < 60 minutes']
        
        if award == "TRUE":
            if thrombolysis_pts == 0:
                award = "DIAMOND"
            else:
                if (float(thrombolysis_therapy_lt_60min) >= 50 and float(thrombolysis_therapy_lt_60min) <= 74.99):
                    award = "GOLD"
                elif (float(thrombolysis_therapy_lt_60min) >= 75):
                    award = "DIAMOND"
                else: 
                    award = "NONE"

        thrombolysis_therapy_lt_45min = x['% patients treated with door to thrombolysis < 45 minutes']

        if award != "NONE":
            if thrombolysis_pts == 0:
                award = "DIAMOND"
            else:
                if (float(thrombolysis_therapy_lt_45min) <= 49.99):
                    if (award != "GOLD" or award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(thrombolysis_therapy_lt_45min) >= 50):
                    if (award != "GOLD"):
                        award = "DIAMOND"
                else:
                    award = "NONE"

        thrombectomy_pts = x['# patients eligible thrombectomy']
        if thrombectomy_pts != 0:
            thrombectomy_therapy_lt_90min = x['% patients treated with door to thrombectomy < 90 minutes']
            if award != "NONE":
                if (float(thrombectomy_therapy_lt_90min) >= 50 and float(thrombectomy_therapy_lt_90min) <= 74.99):
                    if (award == "PLATINUM" or award == "DIAMOND"):
                        award = "GOLD"
                elif (float(thrombectomy_therapy_lt_90min) >= 75):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else: 
                    award = "NONE"

            thrombectomy_therapy_lt_60min = x['% patients treated with door to thrombectomy < 60 minutes']
            if award != "NONE":
                if (float(thrombectomy_therapy_lt_60min) <= 49.99):
                    if (award != "GOLD" or award == "DIAMOND"):
                        award = "PLATINUM"
                elif (float(thrombectomy_therapy_lt_60min) >= 50):
                    if (award == "DIAMOND"):
                        award = "DIAMOND"
                else:
                    award = "NONE"

        recan_rate = x['% recanalization rate out of total ischemic incidence']
        if award != "NONE":
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
                award = "NONE"

        ct_mri = x['% suspected stroke patients undergoing CT/MRI']
        if award != "NONE":
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
                award = "NONE"

        dysphagia_screening = x['% all stroke patients undergoing dysphagia screening']
        if award != "NONE":
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
                award = "NONE"

        discharged_with_antiplatelets_final = x['% ischemic stroke patients discharged (home) with antiplatelets']
        if award != "NONE":
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
                award = "NONE"

        discharged_with_anticoagulants_final = x['% afib patients discharged (home) with anticoagulants']
        if award != "NONE":
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
                award = "NONE"

        stroke_unit = x['% stroke patients treated in a dedicated stroke unit / ICU']
        if award != "NONE":
            if (float(stroke_unit) <= 0.99):
                if (award == "DIAMOND"):
                    award = "PLATINUM"
            elif (float(stroke_unit) >= 1):
                if (award == "DIAMOND"):
                    award = "DIAMOND"
            else:
                award = "NONE"

        return award


    def get_stats_df(self):
        """ Return the final stats dataframe. """
        # Filter dataframe
        
        if self.start_date is not None or self.end_date is not None:
            #self.df = self.filter_by_date(self.start_date, self.end_date)
            self.df = self.filter_by_date()
        if not self.df.empty:
            # Set preprocessed data
            self.preprocessed_data = self.df.copy()
            self.get_total_patients()
            # Replace total patient by TRUE if >= 30 or FALSE if < 30
            self.stats_df['Total Patients'] = self.stats_df.apply(lambda x: 'TRUE' if x['# total patients'] >= 30 else 'FALSE', axis=1)
            self.get_recan_therapy()
            self.get_recan_rate()
            self.get_ct_mri()
            self.get_dysphagia_screening()
            self.get_patients_discharged_with_antiplatelets()
            self.get_afib_discharged_with_anticoagulants()
            self.get_hospitalized_in()
            self.stats_df['Proposed Award'] = self.stats_df.apply(lambda x: self._get_final_award(x), axis=1)
            # Delete redundant columns
            columns_to_delete = ['# patients eligible thrombectomy', '# patients eligible thrombolysis']
            for i in columns_to_delete:
                if i in self.stats_df.columns:
                    self.stats_df.drop([i], axis=1, inplace=True)
            self.rename_column()
            # Replace all Nan with 0
            self.stats_df.fillna(0, inplace=True)
            logging.info('Calculation: Angels Awards statistic was calculated successfully.')     
            return self.stats_df
        else:
            logging.warn('Calculation: There are no data for the selected date range.')

    def time_diff(self, start, end):
        """
        Calculate difference between two times. 

        Parameters:
            start : time
            end : time

        Returns:
            int
                Difference between two times in minutes.
        """
        if isinstance(start, time): # convert to datetime
            assert isinstance(end, time)
            start, end = [datetime.combine(datetime.min, t) for t in [start, end]]
        if start <= end: # e.g., 10:33:26-11:15:49
            return (end - start) / timedelta(minutes=1)
        else: # end < start e.g., 23:55:00-00:25:00
            #end += timedelta(1)
           # assert end > start
            if ((end - start) / timedelta(minutes=1)) < -500:
                end += timedelta(1)
                assert end > start
                return (end - start) / timedelta(minutes=1)
            else:
                return (end - start) / timedelta(minutes=1)

    def rename_column(self):
        """ Rename first two column name. """
        # Remove S_ from the site id
        #self.stats_df['site_id'] = self.stats_df.apply(lambda x: x['site_oid'][2:], axis=1)
        # Rename columns site_oid and site_name
        self.stats_df.rename(columns={'site_id': 'Site ID', 'facility_name': 'Site Name'}, inplace=True)
    
        
class FormatStatistic():
    """ Generate formatted excel file for calculated statistics. """

    def __init__(self, df, path):

        self.df = df
        self.path = path

        # Create log file in the workign folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Running FormatStatistic') 

        self.format(self.df)

    def format(self, df):         

        workbook1 = xlsxwriter.Workbook(self.path, {'strings_to_numbers': True})
        # create worksheet
        worksheet = workbook1.add_worksheet()

        # set width of columns
        worksheet.set_column(0, 2, 15)
        worksheet.set_column(3, 20, 40)

        ncol = len(df.columns) - 1
        nrow = len(df) + 2

        col = []
        for i in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = df.columns.tolist()[i]
            col.append(tmp)

        statistics = df.values.tolist()
        colors = {
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

        first_cell = xl_rowcol_to_cell(0, 2)
        last_cell = xl_rowcol_to_cell(0, ncol)
        worksheet.merge_range(first_cell + ":" + last_cell, 'ESO ANGELS AWARDS', awards)
        for i in range(2, ncol+1):
            cell = xl_rowcol_to_cell(1, i)
            worksheet.write(cell, '', awards_color)
       
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

        worksheet.add_table(2, 0, nrow, ncol, options)

        # total number of rows
        number_of_rows = len(statistics) + 2

        column_names = df.columns.tolist()

        hidden_columns = ['# total patients', '# patients treated with door to thrombolysis < 60 minutes', '# patients treated with door to thrombolysis < 45 minutes', '# patients treated with door to thrombectomy < 90 minutes', '# patients treated with door to thrombectomy < 60 minutes', '# recanalization rate out of total ischemic incidence', '# suspected stroke patients undergoing CT/MRI', '# all stroke patients undergoing dysphagia screening', '# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets', '# ischemic stroke patients discharged (home) with antiplatelets', '# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants', '# afib patients discharged (home) with anticoagulants', '# stroke patients treated in a dedicated stroke unit / ICU']

        for i in hidden_columns:
            index = column_names.index(i)
            column = xl_col_to_name(index)
            worksheet.set_column(column + ":" + column, None, None, {'hidden': True})

        # if cell contain TRUE in column > 30 patients (DR) it will be colored to green
        awards = []
        row = 4
        while row < nrow + 2:
            index = column_names.index('Total Patients')
            cell_n = xl_col_to_name(index) + str(row)
            worksheet.conditional_format(cell_n, {'type': 'text',
                                                'criteria': 'containing',
                                                'value': 'TRUE',
                                                'format': green})
            row += 1

        def angels_awards_ivt_60(column_name, coln):
            """Add conditional formatting to angels awards for ivt < 60."""
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

        index = column_names.index('% patients treated with door to thrombolysis < 60 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column, coln=index)

        index = column_names.index('% patients treated with door to thrombectomy < 90 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column, coln=index)


        def angels_awards_ivt_45(column_name, coln):
            """Add conditional formatting to angels awards for ivt < 45."""
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

        index = column_names.index('% patients treated with door to thrombolysis < 45 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_45(column, coln=index)

        index = column_names.index('% patients treated with door to thrombectomy < 60 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_45(column, coln=index)

        # setting colors of cells according to their values
        def angels_awards_recan(column_name, coln):
            """Add conditional formatting to angels awards for recaalization procedures."""
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

        index = column_names.index('% recanalization rate out of total ischemic incidence')
        angels_awards_recan(column_name=xl_col_to_name(index), coln=index)


        def angels_awards_processes(column_name, coln, count=True):
            """Add conditional formatting to angels awards for processes."""
            count = count
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

        index = column_names.index('% suspected stroke patients undergoing CT/MRI')
        angels_awards_processes(column_name=xl_col_to_name(index), coln=index)
        index = column_names.index('% all stroke patients undergoing dysphagia screening')
        angels_awards_processes(column_name=xl_col_to_name(index), coln=index)
        index = column_names.index('% ischemic stroke patients discharged (home) with antiplatelets')
        angels_awards_processes(column_name=xl_col_to_name(index), coln=index)
        index = column_names.index('% afib patients discharged (home) with anticoagulants')
        angels_awards_processes(column_name=xl_col_to_name(index), coln=index)

        # setting colors of cells according to their values
        def angels_awards_hosp(column_name, coln):
            """Add conditional formatting to angels awards for hospitalization."""
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

        
        index = column_names.index('% stroke patients treated in a dedicated stroke unit / ICU')
        angels_awards_hosp(column_name=xl_col_to_name(index), coln=index)

        # set color for proposed angel award
        def proposed_award(column_name, coln):
            row = 4
            while row < nrow + 2:
                cell_n = column + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'NONE',
                                                    'format': green})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = column + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'GOLD',
                                                    'format': gold})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = column + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'PLATINUM',
                                                    'format': plat})
                row += 1

            row = 4
            while row < nrow + 2:
                cell_n = column + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'DIAMOND',
                                                    'format': black})
                row += 1

        index = column_names.index('Proposed Award')
        column = xl_col_to_name(index)
        proposed_award(column, coln=index)

        workbook1.close()

class GeneratePreprocessedData():
    """
    This class generate the preprocessed data as excel spreadsheet. The preprocessed data are formatted as table. 

    Params:
        df: the preprocessed data with additional columns (fixed dates, etc.)
        path: the absolute path to the document which should be created
    """

    def __init__(self, df, path):

        self.df = df.copy()
        self.path = path

        # Create log file in the workign folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Running GeneratePreprocessedData') 

        # Repalce Nan value by 0
        self.df.fillna(0, inplace=True)
        # Call function which generate excel file
        self.generate_preprocessed_data()

    def generate_preprocessed_data(self):
        """
        Function called to create workbook and append preprocessed data.
        """
        self.df['visit_date_es'] = self.df['visit_date_es'].astype(str)
        self.df['hospital_date_es'] = self.df['hospital_date_es'].astype(str)
        self.df['discharge_date_es'] = self.df['discharge_date_es'].astype(str)
        self.df['hospital_date_fixed'] = self.df['hospital_date_fixed'].astype(str)
        self.df['discharge_date_fixed'] = self.df['discharge_date_fixed'].astype(str)
        self.df['visit_timestamp'] = self.df['visit_timestamp'].astype(str)
        self.df['hospital_timestamp'] = self.df['hospital_timestamp'].astype(str)

        preprocessed_data = self.df.values.tolist()

        workbook = xlsxwriter.Workbook(self.path)
        # create worksheet
        sheet = workbook.add_worksheet('Preprocessed_raw_data')

        # set width of columns
        sheet.set_column(0, 150, 30)
        # number of columns
        # add table into worksheet
        ncol = len(self.df.columns) - 1
        nrow = len(self.df)
        col = []
        for j in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = self.df.columns.tolist()[j]
            # if (i >= 2):
            #    tmp['total_function': 'sum']
            col.append(tmp)

        options = {'data': preprocessed_data,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 1'
                   }
        sheet.add_table(0, 0, nrow, ncol, options)

        workbook.close()

