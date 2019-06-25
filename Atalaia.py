"""
.. module:: Calculation
    :platform: Uniw, Windows
    :synopsis: module to calculate statistics for Atalaia form

.. moduleauthor:: Marie Jankujova <jankujova.marie@fnusa.cz>
"""

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
    """
    Class calculating hospital days using discharge date and hospital date. 
    
    This class provides some basic algorithms to fix hospital and discharge dates if calculated hospital days were > 300 or < 0.

    :param df: preprocessed data
    :type df: dataframe
    :param start_date: first date included in the dataframe
    :type start_date: date
    :param end_date: last date included in the dataframe
    :type end_date: date

    """

    def __init__(self, df, start_date=None, end_date=None):

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Atalaia: Running calculation!')   

        self.df = df.copy()
        self.start_date = start_date
        self.end_date = end_date

        self.df = self.filter_null_dates()

        if not self.df.empty:
            self.df['hospital_days'] = self.df.apply(lambda x: self.calculate_hospital_days(x['discharge_date_es'], x['hospital_date_es']), axis=1, result_type='expand')

            # Export negative hospital days into csv
            negative_hospital_days = self.df[self.df['hospital_days'] < 0]
            negative_hospital_days.to_csv("negative_hospital_days.csv", sep=",")

            self.df[['hospital_days_fixed', 'hospital_date_fixed', 'discharge_date_fixed']] = self.df.apply(lambda x: self.fix_negative_hospital_days(discharge_date=x['discharge_date_es'], hospital_date=x['hospital_date_es']) if (x['hospital_days'] < 0 or x['hospital_days'] > 300) else (x['hospital_days'], x['hospital_date_es'], x['discharge_date_es']), axis=1, result_type='expand')       

            logging.info('Atalaia: Negative and too much positive hospital days has been fixed!')

        else:
            logging.info('Atalaia: No available data!')
            sys.exit()
 
    def filter_null_dates(self):
        """ Filter out null discharge dates and null hospital dates. 

        :returns: dataframe -- the filtered dataframe
        """
        df = self.df[~pd.isnull(self.df['discharge_date_es'])] 
        df = df[~pd.isnull(df['hospital_date_es'])]  
        
        logging.info('Atalaia: Patients with NULL discharge dates and NULL hospital dates has been filtered out!')

        return df

    def calculate_hospital_days(self, discharge_date, hospital_date):
        """ Return difference in days between hospital date and discharge date. 

        :param discharge_date: the discharge date
        :type discharge_date: date
        :param hospital_date: the hospital date
        :type hospital_date: date
        :returns: int -- the number of days
        """
        hospital_days = (discharge_date - hospital_date).days

        # If hospital days is 0, then return 1 else return hospital days
        return 1 if hospital_days == 0 else hospital_days

    def fix_negative_hospital_days(self, discharge_date, hospital_date):
        """ Fix discharge date or hospital date if hospital days were < 0 or > 300. 

        :param discharge_date: the discharge date
        :type discharge_date: date
        :param hospital_date: the hospital date
        :type hospital_date: date
        :returns: the fixed hospital days, the fixed hospital date, the fixed discharge date
        """
        hospital_days = (discharge_date - hospital_date).days
        discharge_date_new = discharge_date
        hospital_date_new = hospital_date

        if hospital_days < -300:
            # Add 1 year to discharge date
            discharge_date_new = discharge_date + relativedelta(years=+1)
        elif hospital_days > 300:
            # Add 1 year to hospital date
            hospital_date_new = hospital_date + relativedelta(years=+1)
        elif hospital_days >= -31 and hospital_days < 0:
            # Add 1 month to discharge date
            discharge_date_new = discharge_date + relativedelta(months=+1)
        elif hospital_days > -60 and hospital_days < -31:
            # Add 2 months to discharge date
            discharge_date_new = discharge_date + relativedelta(months=+2)
        else:
            discharge_date_new = discharge_date

        hospital_days_fixed = (discharge_date_new - hospital_date_new).days
        if hospital_days_fixed == 0:
            hospital_days_fixed = 1

        return hospital_days_fixed, hospital_date_new, discharge_date_new


class Filtration(CheckTimes):
    """ Class filtrating dataframe by discharge date. """

    def filter_by_date(self):
        """ 
        Filter dataframe by discharge date. The start date and end date were defined in :class:`resqdb.Atalaia.CheckTimes` class. 

        :returns: the filtered dataframe
        """

        df = self.df[(self.df['discharge_date_es'] >= self.start_date) & (self.df['discharge_date_es'] <= self.end_date)]

        logging.info('Atalaia: Raw data were filtered and include only rows with discharge date between {0} and {1}.'.format(self.start_date, self.end_date))

        return df


class Calculation(Filtration):
   
    def get_total_patients(self):
        """ The function calculating total number of patients per site.
        
        :returns: the temporary dataframe containing Site ID and total number of patients
        """
        try:
            self.stats_df = self.df.groupby(['site_id', 'facility_name']).size().reset_index(name="# total patients")
            logging.info('Atalaia: Total patients: OK.')
        except: 
            logging.info('Atalaia: Total patients: ERROR.')

    def get_recan_below(self, dtn, dtg, top):
        """ The function checking if at least one from the pair of number is lesser then maximum. 
        
        :param dtn: door to needle time value
        :type dtn: int
        :param dtg: door to groin time value
        :type dtg: int
        :param top: limit value
        :type top: int
        :returns: `True` if one from the pair is lesser then maximum, `False` otherwise.
        """

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
        """ The function calculating number of patients treated within 60/45 minutes by thrombolysis and within 90/60 by thrombectomy. The results are merged with the dataframe containing resulted statistic! """
        
        try:
            thrombolysis_df = self.df[self.df['recanalization_procedures_es'].isin([1,2])].copy()
            thrombectomy_df = self.df[self.df['recanalization_procedures_es'].isin([3,4])].copy()

            if not thrombolysis_df.empty:
                # If time of thrombolysis has been entered as timestamp for thrombolysis, calculate time in minutes from hospital_time_es and ivt_only_bolus_time_es
                thrombolysis_df['DTN_IVT_ONLY'] = thrombolysis_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_only_bolus_time_es']) if (x['recanalization_procedures_es'] == 1 and x['ivt_only_bolus_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # If time of thrombolysis has been entered as timestamp for thrombolysis and thrombectomy, calculate time in minutes from hospital_time_es and ivt_tby_bolus_time_es
                thrombolysis_df['DTN_IVT_TBY'] = thrombolysis_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_tby_bolus_time_es']) if (x['recanalization_procedures_es'] == 2 and x['ivt_tby_bolus_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Merge two previously created columns into one
                thrombolysis_df['DTN'] = thrombolysis_df.apply(lambda x: x['DTN_IVT_ONLY'] + x['DTN_IVT_TBY'], axis=1, result_type='expand')
                # Filter out rows with negative DTN
                thrombolysis_df = thrombolysis_df[(thrombolysis_df['DTN'] > 0)]

                if not thrombolysis_df.empty:
                    # Thrombolysis < 60 minutes
                    thrombolysis_pts = thrombolysis_df.groupby(['site_id']).size().reset_index(name="# patients eligible thrombolysis")
                    thrombolysis_df['recan_below_60'] =  thrombolysis_df.apply(lambda x: self.get_recan_below(x['DTN'], 0, 60), axis=1) 
                    thrombolysis_within_60_df = thrombolysis_df[thrombolysis_df['recan_below_60'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombolysis < 60 minutes')
                    tmp = pd.merge(thrombolysis_pts, thrombolysis_within_60_df, how="left", on="site_id")
                    tmp['% patients treated with door to thrombolysis < 60 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombolysis < 60 minutes']/x['# patients eligible thrombolysis'])*100,2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)

                    # Thrombolysis < 45 minutes
                    thrombolysis_df['recan_below_45'] = thrombolysis_df.apply(lambda x: self.get_recan_below(x['DTN'], 0, 45), axis=1)
                    thrombolysis_within_45_df = thrombolysis_df[thrombolysis_df['recan_below_45'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombolysis < 45 minutes')
                    tmp = pd.merge(tmp, thrombolysis_within_45_df, how="left", on="site_id")
                    tmp['% patients treated with door to thrombolysis < 45 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombolysis < 45 minutes']/x['# patients eligible thrombolysis'])*100,2) if x['# patients eligible thrombolysis'] > 0 else 0, axis=1)

                    logging.info('Atalaia: Number of patients treated by thrombolysis within 60/45 minutes has been calculated!')

                    self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")  
            else:
                self.stats_df['# patients treated with door to thrombolysis < 60 minutes'] = 0
                self.stats_df['% patients treated with door to thrombolysis < 60 minutes'] = 0
                self.stats_df['# patients treated with door to thrombolysis < 45 minutes'] = 0
                self.stats_df['% patients treated with door to thrombolysis < 45 minutes'] = 0

            if not thrombectomy_df.empty:
                # If time of thrombectomy has been entered as timestamp for thrombectomy, calculate time in minutes from hospital_time_es and ivt_tby_groin_puncture_time_es
                thrombectomy_df['DTG_IVT_TBY'] = thrombectomy_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['ivt_tby_groin_puncture_time_es']) if (x['recanalization_procedures_es'] == 2 and x['ivt_tby_groin_puncture_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # If time of thrombectomy has been entered as timestamp for thrombolysis and thrombectomy, calculate time in minutes from hospital_time_es and tby_only_puncture_time_es
                thrombectomy_df['DTG_TBY'] = thrombectomy_df.apply(lambda x: self.time_diff(x['hospital_time_es'], x['tby_only_puncture_time_es']) if (x['recanalization_procedures_es'] == 3 and x['tby_only_puncture_time_es'] is not None and x['hospital_time_es'] is not None) else 0, axis=1)
                # Merge two previously created columns into one
                thrombectomy_df['DTG'] = thrombectomy_df.apply(lambda x: x['DTG_IVT_TBY'] + x['DTG_TBY'], axis=1, result_type='expand')
                # Filter out rows with negative DTG
                thrombectomy_df = thrombectomy_df[(thrombectomy_df['DTG'] > 0)]

                if not thrombectomy_df.empty:
                    # Thrombectomy < 90 minutes
                    thrombectomy_pts = thrombectomy_df.groupby(['site_id']).size().reset_index(name="# patients eligible thrombectomy")
                    thrombectomy_df['recan_below_90'] =  thrombectomy_df.apply(lambda x: self.get_recan_below(x['DTG'], 0, 90), axis=1) 
                    thrombectomy_within_90_df = thrombectomy_df[thrombectomy_df['recan_below_90'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombectomy < 90 minutes')
                    tmp = pd.merge(thrombectomy_pts, thrombectomy_within_90_df, how="left", on="site_id")
                    tmp['% patients treated with door to thrombectomy < 90 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombectomy < 90 minutes']/x['# patients eligible thrombectomy'])*100,2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)

                    # Thrombectomy < 60 minutes
                    thrombectomy_df['recan_below_60'] = thrombectomy_df.apply(lambda x: self.get_recan_below(x['DTG'], 0, 60), axis=1)
                    thrombectomy_within_60_df = thrombectomy_df[thrombectomy_df['recan_below_60'] == True].groupby(['site_id']).size().reset_index(name='# patients treated with door to thrombectomy < 60 minutes')
                    tmp = pd.merge(tmp, thrombectomy_within_60_df, how="left", on="site_id")
                    tmp['% patients treated with door to thrombectomy < 60 minutes'] = tmp.apply(lambda x: round((x['# patients treated with door to thrombectomy < 60 minutes']/x['# patients eligible thrombectomy'])*100,2) if x['# patients eligible thrombectomy'] > 0 else 0, axis=1)

                    logging.info('Atalaia: Number of patients treated by thrombectomy within 90/60 minutes has been calculated!')

                    self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id") 
            else:
                self.stats_df['# patients treated with door to thrombectomy < 90 minutes'] = 0
                self.stats_df['% patients treated with door to thrombectomy < 90 minutes'] = 0
                self.stats_df['# patients treated with door to thrombectomy < 60 minutes'] = 0
                self.stats_df['% patients treated with door to thrombectomy < 60 minutes'] = 0
            
            logging.info('Atalaia: Recanalization procedures: OK')
        except:
            logging.info('Atalaia: Recanalization procedures: ERROR')


    def get_recan_rate(self):
        """ The function calculating number of ischemic patients treated by recanalization procedure. The results are merged with the dataframe containing resulted statistic! """
        try:
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])] # Ischemic stroke: stroke_type_es = 1
            recan_rate_df = ischemic_df[ischemic_df['recanalization_procedures_es'].isin([1,2,3])]
            ischemic_pts = ischemic_df.groupby(['site_id']).size().reset_index(name="tmp_patients")
            if not recan_rate_df.empty:
                recan_rate_pts = recan_rate_df.groupby(['site_id']).size().reset_index(name='# recanalization rate out of total ischemic incidence')
                tmp = pd.merge(recan_rate_pts, ischemic_pts, how="left", on="site_id")
                tmp['% recanalization rate out of total ischemic incidence'] = tmp.apply(lambda x: round((x['# recanalization rate out of total ischemic incidence']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# recanalization rate out of total ischemic incidence'] = 0
                self.stats_df['% recanalization rate out of total ischemic incidence'] = 0
        
            logging.info('Atalaia: Recanalization rate: OK')
        except:
            logging.info('Atalaia: Recanalization rate: ERROR')
        
    def get_ct_mri(self):
        """ The function calculating number of patients with IS, TIA and ICH stroke who have undergone the CT/MRI. The results are merged with the dataframe containing resulted statistic! """
        try:
            # Filter patients with ischemic stroke (stroke_type_es = 1), intracerebral hemorrhage (stroke_type_es = 2) and transient ischemic stroke (stroke_type_es = 3) patients who have undergone CT/MRI (ct_mri_es = 1)
            ct_mri_df = self.df[(self.df['stroke_type_es'].isin([1,2,3]) & self.df['ct_mri_es'].isin([1]))] 
            is_tia_ich_df = self.df[self.df['stroke_type_es'].isin([1,2,3])].groupby(['site_id']).size().reset_index(name="tmp_patients")
            if not ct_mri_df.empty:
                tmp = ct_mri_df.groupby(['site_id']).size().reset_index(name='# suspected stroke patients undergoing CT/MRI')
                tmp = pd.merge(tmp, is_tia_ich_df, how="left", on="site_id")
                tmp['% suspected stroke patients undergoing CT/MRI'] = tmp.apply(lambda x: round((x['# suspected stroke patients undergoing CT/MRI']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# suspected stroke patients undergoing CT/MRI'] = 0
                self.stats_df['% suspected stroke patients undergoing CT/MRI'] = 0
            logging.info('Atalaia: CT/MRI: OK')
        except:
            logging.info('Atalaia: CT/MRI: ERROR')
    
    def get_dysphagia_screening(self):
        """ The function calculating number of patients with IS and ICH stroke who have undergone the dysphagia screening. The results are merged with the dataframe containing resulted statistic! """
        try:
            # Filter patients with ischemic stroke (stroke_type_es = 1) and intracerebral hemorrhage (stroke_type_es = 2) patients who have undergone GUSS test (dysphagia_screening_es = 1) or other test (dysphagia_screening_es = 2)
            dysphagia_df = self.df[(self.df['stroke_type_es'].isin([1,2]) & self.df['dysphagia_screening_es'].isin([1,2]))]
            # Filter patients with ischemic stroke (stroke_type_es = 1) and intracerebral hemorrhage (stroke_type_es = 2) patients who have undergone GUSS test (dysphagia_screening_es = 1), other test (dysphagia_screening_es = 2) or has not been tested (dysphagia_screening_es = 4)
            dysphagia_ntest_df = self.df[(self.df['stroke_type_es'].isin([1,2]) & self.df['dysphagia_screening_es'].isin([1,2,4]))]
            dysphagia_ntest_tmp_df = dysphagia_ntest_df.groupby(['site_id']).size().reset_index(name='tmp_patients')

            if not dysphagia_df.empty:
                tmp = dysphagia_df.groupby(['site_id']).size().reset_index(name='# all stroke patients undergoing dysphagia screening')
                tmp = pd.merge(tmp, dysphagia_ntest_tmp_df, how="left", on="site_id")
                tmp['% all stroke patients undergoing dysphagia screening'] = tmp.apply(lambda x: round((x['# all stroke patients undergoing dysphagia screening']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)

                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# all stroke patients undergoing dysphagia screening'] = 0
                self.stats_df['% all stroke patients undergoing dysphagia screening'] = 0
            logging.info('Atalaia: Dysphagia screening: OK')
        except:
            logging.info('Atalaia: Dysphagia screening: ERROR')

    def get_patients_discharged_with_antiplatelets(self):
        """ The function calculating number of ischemic patients who have been discharged with prescribed antiplatelets. The results are merged with the dataframe containing resulted statistic! """
        try:
            # Filter patients with ischemic stroke (stroke_type_es = 1)
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])]

            # Filter ischemic patients who has not been detected for aFib (afib_flutter_es = 3), no detection done (afib_flutter_es = 4) and unknown for aFib (afib_flutter_es = 5) who has not died in the hospital (discharge_destination_es != 5) and had prescribed antiplatelets (antithrombotics_es = 1)
            antiplatelets_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ~ischemic_df['discharge_destination_es'].isin([5]) & ischemic_df['antithrombotics_es'].isin([1]))].copy()

            # Filter ischemic patients who has not been detected for aFib (afib_flutter_es = 3), no detection done (afib_flutter_es = 4) and unknown for aFib (afib_flutter_es = 5) who has not died in the hospital (discharge_destination_es != 5) and had not recommended antithrombotics (antithrombotics_es != 9)
            antiplatelets_recs_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ~ischemic_df['discharge_destination_es'].isin([5]) & ~ischemic_df['antithrombotics_es'].isin([9]))].copy()
            antiplatelets_recs_tmp_df = antiplatelets_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')

            if not antiplatelets_df.empty:
                tmp = antiplatelets_df.groupby(['site_id']).size().reset_index(name='# ischemic stroke patients discharged with antiplatelets')
                tmp = pd.merge(tmp, antiplatelets_recs_tmp_df, how="left", on="site_id")
                tmp['% ischemic stroke patients discharged with antiplatelets'] = tmp.apply(lambda x: round((x['# ischemic stroke patients discharged with antiplatelets']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)

                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# ischemic stroke patients discharged with antiplatelets'] = 0
                self.stats_df['% ischemic stroke patients discharged with antiplatelets'] = 0

            logging.info('Atalaia: Discharged with antiplatelets: OK')
        except:
            logging.info('Atalaia: Discharged with antiplatelets: ERROR')
        
        try:
            # Filter patients with ischemic stroke (stroke_type_es = 1)
            ischemic_df = self.df[self.df['stroke_type_es'].isin([1])]
            # Filter ischemic patients who has not been detected for aFib (afib_flutter_es = 3), no detection done (afib_flutter_es = 4) and unknown for aFib (afib_flutter_es = 5) who has been discharged at home (discharge_destination_es = 1) and had prescribed antiplatelets (antithrombotics_es = 1)
            antiplatelets_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ischemic_df['discharge_destination_es'].isin([1]) & ischemic_df['antithrombotics_es'].isin([1]))].copy()
            # Filter ischemic patients who has not been detected for aFib (afib_flutter_es = 3), no detection done (afib_flutter_es = 4) and unknown for aFib (afib_flutter_es = 5) who has been discharged at home (discharge_destination_es = 1) and had not recommended antithrombotics (antithrombotics_es != 9)
            antiplatelets_recs_df = ischemic_df[(ischemic_df['afib_flutter_es'].isin([3,4,5]) & ischemic_df['discharge_destination_es'].isin([1]) & ~ischemic_df['antithrombotics_es'].isin([9]))].copy()
            antiplatelets_recs_tmp_df = antiplatelets_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')

            if not antiplatelets_df.empty:
                tmp = antiplatelets_df.groupby(['site_id']).size().reset_index(name='# ischemic stroke patients discharged home with antiplatelets')
                tmp = pd.merge(tmp, antiplatelets_recs_tmp_df, how="left", on="site_id")
                tmp['% ischemic stroke patients discharged home with antiplatelets'] = tmp.apply(lambda x: round((x['# ischemic stroke patients discharged home with antiplatelets']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)

                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# ischemic stroke patients discharged home with antiplatelets'] = 0
                self.stats_df['% ischemic stroke patients discharged home with antiplatelets'] = 0

            logging.info('Discharged home with antiplatelets: OK')
        except:
            logging.info('Discharged home with antiplatelets: ERROR')

        # Compare number of patients discharged with antiplatelets with discharge home with antiplatelets and get the highest number. 
        self.stats_df['# ischemic stroke patients discharged (home) with antiplatelets'] = self.stats_df.apply(lambda x: x['# ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['# ischemic stroke patients discharged home with antiplatelets'], axis=1)
        self.stats_df['% ischemic stroke patients discharged (home) with antiplatelets'] = self.stats_df.apply(lambda x: x['% ischemic stroke patients discharged with antiplatelets'] if x['% ischemic stroke patients discharged with antiplatelets'] > x['% ischemic stroke patients discharged home with antiplatelets'] else x['% ischemic stroke patients discharged home with antiplatelets'], axis=1)

        # self.stats_df.drop(['# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets'], axis=1, inplace=True)

    def get_afib_discharged_with_anticoagulants(self):
        """ The function calculating number of patients who have been discharged with prescribed anticoagulants with aFib. The results are merged with the dataframe containing resulted statistic! """
        try:
            # Filter patients known for aFib (afib_flutter_es = 1) or detected for aFib (afib_flutter_es = 2) who has not died in the hospital (discharge_destination_es != 5) and had prescribed antithrombotics (antithrombotics_es = 2,3,4,5,6,7,8)
            anticoagulants_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & ~self.df['discharge_destination_es'].isin([5]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8]))].copy()
            # Filter patients known for aFib (afib_flutter_es = 1) or detected for aFib (afib_flutter_es = 2) who has not died in the hospital (discharge_destination_es != 5) and had prescribed antithrombotics or not prescribed at all (antithrombotics_es = 2,3,4,5,6,7,8,10)
            anticoagulants_recs_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & ~self.df['discharge_destination_es'].isin([5]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8,10]))].copy()
            anticoagulants_recs_tmp_df = anticoagulants_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')

            if not anticoagulants_df.empty:    
                tmp = anticoagulants_df.groupby(['site_id']).size().reset_index(name='# afib patients discharged with anticoagulants')
                tmp = pd.merge(tmp, anticoagulants_recs_tmp_df, how="left", on="site_id")
                tmp['% afib patients discharged with anticoagulants'] = tmp.apply(lambda x: round((x['# afib patients discharged with anticoagulants']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)

                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# afib patients discharged with anticoagulants'] = 0
                self.stats_df['% afib patients discharged with anticoagulants'] = 0

            logging.info('Atalaia: Discharged with anticoagulants: OK')
        except:
            logging.info('Atalaia: Discharged with anticoagulants: ERROR')

        try:
            # Filter patients known for aFib (afib_flutter_es = 1) or detected for aFib (afib_flutter_es = 2) who has been discharge home (discharge_destination_es = 1) and had prescribed antithrombotics (antithrombotics_es = 2,3,4,5,6,7,8)
            anticoagulants_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & self.df['discharge_destination_es'].isin([1]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8]))].copy()
            # Filter patients known for aFib (afib_flutter_es = 1) or detected for aFib (afib_flutter_es = 2) who has been discharge home (discharge_destination_es = 1) and had prescribed antithrombotics or not prescribed at all (antithrombotics_es = 2,3,4,5,6,7,8,10)
            anticoagulants_recs_df = self.df[(self.df['afib_flutter_es'].isin([1,2]) & self.df['discharge_destination_es'].isin([1]) & self.df['antithrombotics_es'].isin([2,3,4,5,6,7,8,10]))].copy()
            anticoagulants_recs_tmp_df = anticoagulants_recs_df.groupby(['site_id']).size().reset_index(name='tmp_patients')

            if not anticoagulants_df.empty:    
                tmp = anticoagulants_df.groupby(['site_id']).size().reset_index(name='# afib patients discharged home with anticoagulants')
                tmp = pd.merge(tmp, anticoagulants_recs_tmp_df, how="left", on="site_id")
                tmp['% afib patients discharged home with anticoagulants'] = tmp.apply(lambda x: round((x['# afib patients discharged home with anticoagulants']/x['tmp_patients'])*100, 2) if x['tmp_patients'] > 0 else 0, axis=1)
                tmp.drop(['tmp_patients'], axis=1, inplace=True)

                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
            else:
                self.stats_df['# afib patients discharged home with anticoagulants'] = 0
                self.stats_df['% afib patients discharged home with anticoagulants'] = 0

            logging.info('Atalaia: Discharged with home anticoagulants: OK')
        except:
            logging.info('Atalaia: Discharged with home anticoagulants: ERROR')

        # Compare number of patients discharged with anticoagulants with discharge home with anticoagulants and get the highest number.
        self.stats_df['# afib patients discharged (home) with anticoagulants'] = self.stats_df.apply(lambda x: x['# afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['# afib patients discharged home with anticoagulants'], axis=1)
        self.stats_df['% afib patients discharged (home) with anticoagulants'] = self.stats_df.apply(lambda x: x['% afib patients discharged with anticoagulants'] if x['% afib patients discharged with anticoagulants'] > x['% afib patients discharged home with anticoagulants'] else x['% afib patients discharged home with anticoagulants'], axis=1)

        # self.stats_df.drop(['# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants'], axis=1, inplace=True)

    def get_hospitalized_in(self):
        """ The function calculating number of patients who have been hospitalized in a dedicated stroke unit / ICU. The results are merged with the dataframe containing resulted statistic! """
        try:
            # Filter patients hospitalized in a dedicated stroke unit (hospitalized_in_es = 1)
            hosp_df = self.df[self.df['hospitalized_in_es'].isin([1])].copy()
            if not hosp_df.empty:
                tmp = hosp_df.groupby(['site_id']).size().reset_index(name="# stroke patients treated in a dedicated stroke unit / ICU")
                self.stats_df = pd.merge(self.stats_df, tmp, how="left", on="site_id")
                self.stats_df['% stroke patients treated in a dedicated stroke unit / ICU'] = self.stats_df.apply(lambda x: round((x['# stroke patients treated in a dedicated stroke unit / ICU']/x['# total patients'])*100, 2) if x['# total patients'] > 0 else 0, axis=1)
            else:
                self.stats_df['# stroke patients treated in a dedicated stroke unit / ICU'] = 0
                self.stats_df['% stroke patients treated in a dedicated stroke unit / ICU'] = 0

            logging.info('Atalaia: Hospitalized in stroke unit: OK')
        except:
            logging.info('Atalaia: Hospitalized in stroke unit: ERROR')

    def _get_final_award(self, x):
        """ The function calculating the final award. The results are merged with the dataframe containing resulted statistic! 
        
        :param x: the row with the values to calculate proposed award
        :type x: pandas series
        :returns: proposed award
        """        
        if x['Total Patients'] == False:
            award = "NONE"
        else:
            award = "TRUE"

        thrombolysis_pts = x['# patients eligible thrombolysis']
        
        # Calculate award for thrombolysis, if no patients were eligible for thrombolysis and number of total patients was greater than minimum than the award is set to DIAMOND 
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

        # Calculate award for thrombectomy, if no patients were eligible for thrombectomy and number of total patients was greater than minimum than the award is set to the possible proposed award (eg. if in thrombolysis step award was set to GOLD then the award will be GOLD)
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
        """ The function calculating the statistics. 
        
        :returns: stats_df -- dataframe with calculated statistics
        """
        
        if self.start_date is not None or self.end_date is not None:
            self.df = self.filter_by_date()

        if not self.df.empty:
            self.preprocessed_data = self.df.copy()
            self.get_total_patients()
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
            self.stats_df.fillna(0, inplace=True)
            logging.info('Atalaia: Angels Awards statistic was calculated successfully.')     
            return self.stats_df
        else:
            logging.warn('Atalaia: There are no data for the selected date range.')

    def time_diff(self, start, end):
        """ The function calculating difference between two times. 
        
        :param start: the first time
        :type start: time
        :param end: the end time
        :type end: time
        :returns: int -- difference in minutes
        """

        if isinstance(start, time): # convert to datetime
            assert isinstance(end, time)
            start, end = [datetime.combine(datetime.min, t) for t in [start, end]]
        if start <= end: # e.g., 10:33:26-11:15:49
            return (end - start) / timedelta(minutes=1)
        else: # end < start e.g., 23:55:00-00:25:00
            # assert end > start
            if ((end - start) / timedelta(minutes=1)) < -500:
                end += timedelta(1)
                assert end > start
                return (end - start) / timedelta(minutes=1)
            else:
                return (end - start) / timedelta(minutes=1)

    def rename_column(self):
        """ The function renaming site_id and facility_name column names to Site ID and Site Name! """

        self.stats_df.rename(columns={'site_id': 'Site ID', 'facility_name': 'Site Name'}, inplace=True)
    
        
class FormatStatistic():
    """ Class generating formatted excel file containing the calculated statistics! 
    
    :param df: the dataframe with calculated statistics
    :type df: dataframe
    :param path: the path where the output file should be saved
    :type path: str
    """

    def __init__(self, df, path):

        self.df = df
        self.path = path

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Running FormatStatistic') 

        self.format(self.df)

    def format(self, df):    
        """ The function creating excel file and add formatting! 
        
        :param df: the dataframe with the statistics
        :type df: dataframe
        """

        workbook1 = xlsxwriter.Workbook(self.path, {'strings_to_numbers': True})
        worksheet = workbook1.add_worksheet()

        # set width of columns
        worksheet.set_column(0, 2, 15)
        worksheet.set_column(3, 20, 40)

        ncol = len(df.columns) - 1
        nrow = len(df) + 2

        col = []
        # Create header from column names
        for i in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = df.columns.tolist()[i]
            col.append(tmp)

        # Get list of values from dataframe
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

        awards = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("angel_awards")})

        awards_color = workbook1.add_format({
            'fg_color': colors.get("angel_awards")})

        # Convert row into letter convention
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

        number_of_rows = len(statistics) + 2

        column_names = df.columns.tolist()

        columns_to_be_hidden = ['# total patients', '# patients treated with door to thrombolysis < 60 minutes', '# patients treated with door to thrombolysis < 45 minutes', '# patients treated with door to thrombectomy < 90 minutes', '# patients treated with door to thrombectomy < 60 minutes', '# recanalization rate out of total ischemic incidence', '# suspected stroke patients undergoing CT/MRI', '# all stroke patients undergoing dysphagia screening', '# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets', '# ischemic stroke patients discharged (home) with antiplatelets', '# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants', '# afib patients discharged (home) with anticoagulants', '# stroke patients treated in a dedicated stroke unit / ICU']
        
        for i in columns_to_be_hidden:
            # Get index from column names and convert this index into Excel column
            index = column_names.index(i)
            column = xl_col_to_name(index)
            worksheet.set_column(column + ":" + column, None, None, {'hidden': True})

        row = 4

        # Format total patients (TRUE = green color)
        while row < nrow + 2:
            index = column_names.index('Total Patients')
            cell_n = xl_col_to_name(index) + str(row)
            worksheet.conditional_format(cell_n, {'type': 'text',
                                                'criteria': 'containing',
                                                'value': 'TRUE',
                                                'format': green})
            row += 1

        def angels_awards_ivt_60(column_name):
            """ The function adding format conditions for recanalization treatment (thrombolysis < 60, thrombectomy < 90)!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """
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
        angels_awards_ivt_60(column_name=xl_col_to_name(index))

        index = column_names.index('% patients treated with door to thrombectomy < 90 minutes')
        angels_awards_ivt_60(column_name=xl_col_to_name(index))


        def angels_awards_ivt_45(column_name):
            """ The function adding format conditions for recanalization treatment (thrombolysis < 45, thrombectomy < 60)!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """

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
        angels_awards_ivt_45(column_name=xl_col_to_name(index))

        index = column_names.index('% patients treated with door to thrombectomy < 60 minutes')
        angels_awards_ivt_45(column_name=xl_col_to_name(index))

        def angels_awards_recan(column_name):
            """ The function adding format conditions for recanalization rate!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """
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
        angels_awards_recan(column_name=xl_col_to_name(index))


        def angels_awards_processes(column_name, count=True):
            """ The function adding format conditions for values which have GOLD in interval <80, 85), PLATINUM in interval <85, 90) and DIAMOND in interval <90,100>!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """
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
        angels_awards_processes(column_name=xl_col_to_name(index))
        index = column_names.index('% all stroke patients undergoing dysphagia screening')
        angels_awards_processes(column_name=xl_col_to_name(index))
        index = column_names.index('% ischemic stroke patients discharged (home) with antiplatelets')
        angels_awards_processes(column_name=xl_col_to_name(index))
        index = column_names.index('% afib patients discharged (home) with anticoagulants')
        angels_awards_processes(column_name=xl_col_to_name(index))

        # setting colors of cells according to their values
        def angels_awards_hosp(column_name):
            """ The function adding format conditions for hospitalized in the stroke unit/ICU!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """
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
        angels_awards_hosp(column_name=xl_col_to_name(index))

        def proposed_award(column_name):
            """ The function adding format conditions for the proposed award!
            
            :param column_name: the column name (eg. A)
            :type column_name: str
            """
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
        proposed_award(column_name=xl_col_to_name(index))

        workbook1.close()


class GeneratePreprocessedData():
    """ Class generating preprocessed data in the excel format containing calculated statistics with intermediate columns! 
    
    :param df: the dataframe with calculated statistics
    :type df: dataframe
    :param path: the path where the output file should be saved
    :type path: str
    """

    def __init__(self, df, path):

        self.df = df.copy()
        self.path = path

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('Running GeneratePreprocessedData') 

        self.df.fillna(0, inplace=True)

        self.generate_preprocessed_data()

    def generate_preprocessed_data(self):
        """ The function creating workbook and sheet with preprocessed data! """
        # Convert dates and timestamps into string
        self.df['visit_date_es'] = self.df['visit_date_es'].astype(str)
        self.df['hospital_date_es'] = self.df['hospital_date_es'].astype(str)
        self.df['discharge_date_es'] = self.df['discharge_date_es'].astype(str)
        self.df['hospital_date_fixed'] = self.df['hospital_date_fixed'].astype(str)
        self.df['discharge_date_fixed'] = self.df['discharge_date_fixed'].astype(str)
        self.df['visit_timestamp'] = self.df['visit_timestamp'].astype(str)
        self.df['hospital_timestamp'] = self.df['hospital_timestamp'].astype(str)

        # Get list of values
        preprocessed_data = self.df.values.tolist()

        workbook = xlsxwriter.Workbook(self.path)
        sheet = workbook.add_worksheet('Preprocessed_raw_data')

        # Set width of columns
        sheet.set_column(0, 150, 30)

        ncol = len(self.df.columns) - 1
        nrow = len(self.df)
        
        # Create headers
        col = []
        for j in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = self.df.columns.tolist()[j]
            col.append(tmp)

        # Set data
        options = {'data': preprocessed_data,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 1'
                   }
        # Create table
        sheet.add_table(0, 0, nrow, ncol, options)

        workbook.close()

