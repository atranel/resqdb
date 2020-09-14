# -*- coding: utf-8 -*-
"""
File name: FormatData.py
Package: resq
Written by: Marie Jankujova - jankujova.marie@fnusa.cz on 03/2019
Version: v1.0
Version comment: This script is used to generate formatted preprocessed data, presentations or excel files. 
"""

# Import default packages
import os
import sys
from datetime import datetime, date
import zipfile
import csv
import pandas as pd
import numpy as np
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name
import logging
from collections import defaultdict, OrderedDict
import pytz

class GeneratePreprocessedData:
    """ Class generating preprocessed data in the excel format containing calculated statistics with intermediate columns! 
    
    :param df: the dataframe with preprocessed data
    :type df: pandas dataframe
    :param split_sites: `True` if preprocessed data should be generated per site
    :type split_sites: bool
    :param site: the site ID
    :type site: str
    :param report: the type of the report, eg. quarter
    :type report: str
    :param quarter: the type of the period, eg. H1_2018
    :type quarter: str
    :param country_code: the country code
    :type country_code: str
    :param csv: `True` if preprocessed data were read from csv
    :type csv: bool
    """

    def __init__(self, df, split_sites=False, site=None, report=None, quarter=None, country_code=None, csv=False, country_name=None):

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

        self.df = df.copy()
        self.split_sites = split_sites
        self.report = report
        self.quarter = quarter
        self.country_code = country_code
        self.csv = csv

        # If Site is not None, filter dataset according to site code
        if site is not None:
            self.country_code = site.split("_")[0]
            df = self.df[self.df['Protocol ID'].str.contains(site) == True]
            # Generate preprocessed data for site
            self._generate_preprocessed_data(df=df, site_code=site)
            logging.info('FormatData: Preprocessed data: The preprocessed data were generated for site {0}'.format(site))
        
        # Generate formatted statistics per site + country as site is included
        if (split_sites) and site is None:
            logging.info('FormatData: Preprocessed data: Generate preprocessed data per site.')
            # Get set of all site ids
            site_ids = set(self.df['Protocol ID'].tolist())
            #site_ids = set(site_ids)  
            for i in site_ids:
                df = self.df[self.df['Protocol ID'].str.contains(i) == True]
                self._generate_preprocessed_data(df=df, site_code=i)
                logging.info('FormatData: Preprocessed data: The preprocessed data were generated for site {0}'.format(i))

        self._generate_preprocessed_data(self.df, site_code=None)
        logging.info('FormatData: Preprocessed data: The preprocessed data were generate for all data.')

    def _generate_preprocessed_data(self, df, site_code):
        """ The function generating the preprocessed data to Excel file. 

        :param df: the dataframe with preprocessed data
        :type df: pandas dataframe
        :param site_code: the site ID
        :type site_code: str
        """
        
        if site_code is not None:
            output_file = self.report + "_" + site_code + "_" + self.quarter + "_preprocessed_data.xlsx"
        elif site_code is None and self.report is None and self.country_code is None and self.quarter is None:
            output_file = "preprocessed_data.xlsx"
        else:
            if self.country_code is None:
                 output_file = self.report + "_" + self.quarter + "_preprocessed_data.xlsx"
            else:
                output_file = self.report + "_" + self.country_code + "_" + self.quarter + "_preprocessed_data.xlsx"
        
        df = df.copy()
        
        # Convert dates to strings
        dateformat = "%m/%d/%Y"
        timeformat = "%H:%M"
        def convert_to_string(datetime, format):
            if datetime is None or datetime is np.nan or pd.isnull(datetime):
                return datetime
            else:
                return datetime.strftime(format)
        
        if not self.csv:
            #if df['VISIT_DATE'].dtype != np.object:
            df['VISIT_DATE'] = df.apply(lambda x: convert_to_string(x['VISIT_DATE'], dateformat), axis=1)
            # if df['VISIT_DATE_OLD'].dtype != np.object:
            df['VISIT_DATE_OLD'] = df.apply(lambda x: convert_to_string(x['VISIT_DATE_OLD'], dateformat), axis=1)
            #if df['VISIT_TIME'].dtype != np.object:
            df['VISIT_TIME'] = df.apply(lambda x: convert_to_string(x['VISIT_TIME'], timeformat), axis=1)
            df['HOSPITAL_DATE'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_DATE'], dateformat), axis=1)
            #if df['HOSPITAL_DATE_OLD'].dtype != np.object:
            df['HOSPITAL_DATE_OLD'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_DATE_OLD'], dateformat), axis=1)
            #if df['HOSPITAL_TIME'].dtype != np.object:
            df['HOSPITAL_TIME'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_TIME'], timeformat), axis=1)
            df['DISCHARGE_DATE'] = df.apply(lambda x: convert_to_string(x['DISCHARGE_DATE'], dateformat), axis=1)
            #if df['DISCHARGE_DATE_OLD'].dtype != np.object:
            df['DISCHARGE_DATE_OLD'] = df.apply(lambda x: convert_to_string(x['DISCHARGE_DATE_OLD'], dateformat), axis=1)
            #if df['IVT_ONLY_ADMISSION_TIME'].dtype != np.object:
            df['IVT_ONLY_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_ONLY_ADMISSION_TIME'], timeformat), axis=1)
            #if df['IVT_ONLY_BOLUS_TIME'].dtype != np.object:
            df['IVT_ONLY_BOLUS_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_ONLY_BOLUS_TIME'], timeformat), axis=1)
            #if df['IVT_TBY_ADMISSION_TIME'].dtype != np.object:
            df['IVT_TBY_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_ADMISSION_TIME'], timeformat), axis=1)
            #if df['IVT_TBY_BOLUS_TIME'].dtype != np.object:
            df['IVT_TBY_BOLUS_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_BOLUS_TIME'], timeformat), axis=1)
            # if df['IVT_TBY_GROIN_PUNCTURE_TIME'].dtype != np.object:
            df['IVT_TBY_GROIN_PUNCTURE_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_GROIN_PUNCTURE_TIME'], timeformat), axis=1)
            #if df['TBY_ONLY_ADMISSION_TIME'].dtype != np.object:
            df['TBY_ONLY_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_ONLY_ADMISSION_TIME'], timeformat), axis=1)
            #if df['TBY_ONLY_PUNCTURE_TIME'].dtype != np.object:
            df['TBY_ONLY_PUNCTURE_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_ONLY_PUNCTURE_TIME'], timeformat), axis=1)
            #if df['IVT_TBY_REFER_ADMISSION_TIME'].dtype != np.object:
            df['IVT_TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_REFER_ADMISSION_TIME'], timeformat), axis=1)
            #  if df['IVT_TBY_REFER_BOLUS_TIME'].dtype != np.object:
            df['IVT_TBY_REFER_BOLUS_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_REFER_BOLUS_TIME'], timeformat), axis=1)
            #  if df['IVT_TBY_REFER_DISCHARGE_TIME'].dtype != np.object:
            df['IVT_TBY_REFER_DISCHARGE_TIME'] = df.apply(lambda x: convert_to_string(x['IVT_TBY_REFER_DISCHARGE_TIME'], timeformat), axis=1)
            # if df['TBY_REFER_DISCHARGE_TIME'].dtype != np.object:
            df['TBY_REFER_DISCHARGE_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_DISCHARGE_TIME'], timeformat), axis=1)
            # if df['TBY_REFER_DISCHARGE_TIME'].dtype != np.object:
            df['TBY_REFER_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_ADMISSION_TIME'], timeformat), axis=1)
             # if df['TBY_REFER_ALL_DISCHARGE_TIME'].dtype != np.object:
            df['TBY_REFER_ALL_DISCHARGE_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_ALL_DISCHARGE_TIME'], timeformat), axis=1)
            #if df['TBY_REFER_ALL_ADMISSION_TIME'].dtype != np.object:
            df['TBY_REFER_ALL_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_ALL_ADMISSION_TIME'], timeformat), axis=1)
            #if df['TBY_REFER_LIM_DISCHARGE_TIME'].dtype != np.object:
            df['TBY_REFER_LIM_DISCHARGE_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_LIM_DISCHARGE_TIME'], timeformat), axis=1)
            # if df['TBY_REFER_LIM_ADMISSION_TIME'].dtype != np.object:
            df['TBY_REFER_LIM_ADMISSION_TIME'] = df.apply(lambda x: convert_to_string(x['TBY_REFER_LIM_ADMISSION_TIME'], timeformat), axis=1)
            #df['CT_TIME'] = df.apply(lambda x: convert_to_string(x['CT_TIME'], timeformat), axis=1)
        # else:
        #     df['HOSPITAL_DATE'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_DATE'], dateformat), axis=1)
            #df['DISCHARGE_DATE'] = df.apply(lambda x: convert_to_string(x['DISCHARGE_DATE'], dateformat), axis=1)
        
        #df.fillna(value="", inplace=True)
        df = df.replace(np.nan, '', regex=True)
        
        workbook = xlsxwriter.Workbook(output_file)
        logging.info('Preprocessed data: The workbook was created.')
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

        ### LEGEND
        legend = pd.read_csv(os.path.join(os.path.dirname(__file__), 'tmp', 'legend.csv'), sep=",", encoding="utf-8")
        legend.fillna(value="", inplace=True)
        legend_list = legend.values.tolist()

        labels = ['Variable Name', 'Description', 'UNITS', 'Response Type', 'Response Options - text',
                  'Response Options - value']

        legend_df = pd.DataFrame(legend_list, columns=labels)
        
        # set width of columns
        legend_sheet.set_column(0, 150, 30)
        # number of columns
        # add table into worksheet
        ncol = len(legend_df.columns) - 1
        nrow = len(legend_df)
        col = []
        for k in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = legend_df.columns.tolist()[k]
            # if (i >= 2):
            #    tmp['total_function': 'sum']
            col.append(tmp)

        options = {'data': legend_list,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 1'
                   }
        legend_sheet.add_table(0, 0, nrow, ncol, options)
        logging.info('Preprocessed data: The sheet "Legend" was added.')

        ### ADDITIONAL INFO
        # Add table with changed column names and description
        labels = ['Column names after preprocessing', 'Description']
        addition_list = [
            ['VISIT_DATE_OLD', 'The original visit date.'], 
            ['HOSPITAL_DATE_OLD', 'The original hospital date.'], 
            ['DISCHARGE_DATE_OLD', 'The original discharge date.'],
            ['HOSPITAL_DAYS_OLD', 'The hospital days calculated from original hospital and discharge date.'],
            ['VISIT_DATE', 'The fixed visit date.'],
            ['HOSPITAL_DATE', 'The fixed hospital date (fixed in the case that hospital days were negative or greater than 300 days).'],
            ['DISCHARGE_DATE', 'The fixed discharge date (fixed in the case that hospital days were negative or greater than 300 days).'],
            ['HOSPITAL_DAYS', 'The hospital days calculated from fixed hospital and discharge dates.'],
            ['HOSPITAL_DAYS_FIXED', 'TRUE for fixed dates, FALSE for original dates.'],
            ['IVT_ONLY_NEEDLE_TIME_MIN', 'The column which contains needle time in minutes (bolus time - admission time).'],
            ['IVT_ONLY_NEEDLE_TIME_CHANGED', 'TRUE for changed times (if difference was > 400, FALSE for unchanged values.'],
            ['IVT_TBY_NEEDLE_TIME_MIN', 'The column which contains needle time in minutes (bolus time - admission time).'],
            ['IVT_TBY_NEEDLE_TIME_CHANGED', 'TRUE for changed times (if difference was > 400, FALSE for unchanged values.'],
            ['IVT_TBY_REFER_NEEDLE_TIME_MIN', 'The column which contains needle time in minutes (bolus time - admission time).'],
            ['IVT_TBY_REFER_NEEDLE_TIME_CHANGED', 'TRUE for changed times (if difference was > 400, FALSE for unchanged values.'],
            ['TBY_ONLY_GROIN_TIME_MIN', 'The column which contains groin time in minutes (groin time - admission time).'],
            ['TBY_ONLY_GROIN_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.'],
            ['IVT_TBY_GROIN_TIME_MIN', 'The column which contains groin time in minutes (groin time - admission time).'],
            ['IVT_TBY_GROIN_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.'],
            ['IVT_TBY_REFER_DIDO_TIME_MIN', 'The column which contains DIDO time in minutes (discharge time - admission time).'],
            ['IVT_TBY_REFER_DIDO_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.'],
            ['TBY_REFER_DIDO_TIME_MIN', 'The column which contains DIDO time in minutes (discharge time - admission time).'],
            ['TBY_REFER_DIDO_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.'],
            ['TBY_REFER_ALL_DIDO_TIME_MIN', 'The column which contains DIDO time in minutes (discharge time - admission time).'],
            ['TBY_REFER_ALL_DIDO_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.'],
            ['TBY_REFER_LIM_DIDO_TIME_MIN', 'The column which contains DIDO time in minutes (discharge time - admission time).'],
            ['TBY_REFER_LIM_DIDO_TIME_CHANGED', 'TRUE for changed times (if difference was > 700, FALSE for unchanged values.']
        ]

        # set width of columns
        additional_desc_sheet.set_column(0, 10, 60)

        addition_df = pd.DataFrame(addition_list, columns=labels)
        ncol = len(addition_df.columns) - 1
        nrow = len(addition_df)
        col = []
        for k in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = addition_df.columns.tolist()[k]
            # if (i >= 2):
            #    tmp['total_function': 'sum']
            col.append(tmp)

        options = {'data': addition_list,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 1'
                   }

        additional_desc_sheet.add_table(0, 0, nrow, ncol, options)
        logging.info('Preprocessed data: The sheet "Additional info" was added.')
    
        workbook.close()


class GenerateFormattedAngelsAwards:
    """ Class generating formatted excel file containing only Angels Awards results. ! 
    
    :param df: the dataframe with preprocessed data
    :type df: pandas dataframe
    :param report: the type of the report, eg. quarter
    :type report: str
    :param quarter: the type of the period, eg. H1_2018
    :type quarter: str
    :param minimum_patients: the minimum number of patients sites need to met condition for total patients
    :type minimum_patients: int
    """
    def __init__(self, df, report=None, quarter=None, minimum_patients=30, one_workbook=False):

        self.df = df
        self.report = report
        self.quarter = quarter
        self.minimum_patients = minimum_patients

        if self.report is None and self.quarter is None:
            output_file = "angels_awards.xlsx"
        else:
            output_file = self.report + "_" + self.quarter + "_angels_awards.xlsx"
        
        workbook1 = xlsxwriter.Workbook(output_file, {'strings_to_numbers': True})

        if one_workbook:
            if isinstance(self.df, OrderedDict):
                for key, val in self.df.items():
                    self.formate(val, workbook1, sheet_name=key)
        else:
            self.formate(self.df, workbook1)

        workbook1.close()

    def formate(self, df, workbook1, sheet_name=None):
        """ The function formatting the Angels Awards data. 

        :param df: the temporary dataframe containing only column needed to propose award
        :type df: pandas dataframe
        :param workbook1: the active workbook object
        :type workbook1: the Workbook
        :param sheet_name: the name of sheet
        :type sheet_name: str
        """
        total_patients_column = f'# total patients >= {self.minimum_patients}'
        df = df[['Site ID', 'Site Name', total_patients_column, 'Total Patients', '% patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 45 minutes', '% patients treated with door to thrombolysis < 60 minutes', '% patients treated with door to thrombolysis < 45 minutes', '% patients treated with door to thrombectomy < 120 minutes', '% patients treated with door to thrombectomy < 90 minutes', '% recanalization rate out of total ischemic incidence', '% suspected stroke patients undergoing CT/MRI', '% all stroke patients undergoing dysphagia screening', '% ischemic stroke patients discharged (home) with antiplatelets', '% afib patients discharged (home) with anticoagulants', '% stroke patients treated in a dedicated stroke unit / ICU', 'Proposed Award (old calculation)', 'Proposed Award', '# patients eligible thrombectomy']].copy()

        if sheet_name is None:
            worksheet = workbook1.add_worksheet()
        else:
            worksheet = workbook1.add_worksheet(sheet_name)

        worksheet.set_column(0, 2, 15)
        worksheet.set_column(2, 20, 40)

        thrombectomy_patients = df['# patients eligible thrombectomy'].values
        df.drop(['# patients eligible thrombectomy'], inplace=True, axis=1)

        ncol = len(df.columns) - 1
        nrow = len(df) + 2

        col = []
        column_names = df.columns.tolist()
        # Create table header
        for i in range(0, ncol + 1):
            tmp = {}
            tmp['header'] =column_names[i]
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
        for i in range(2, ncol + 1):
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

        first_col = xl_col_to_name(0)
        last_col = xl_col_to_name(ncol + 1)
        worksheet.set_column(first_col + ":" + last_col, 30)

        worksheet.add_table(2, 0, nrow, ncol, options)

        # total number of rows
        number_of_rows = len(statistics) + 2

        self.total_patients_column = '# total patients >= {0}'.format(self.minimum_patients)
        # if cell contain TRUE in column > 30 patients (DR) it will be colored to green
        
        row = 4
        while row < nrow + 2:
            index = column_names.index(self.total_patients_column)
            cell_n = xl_col_to_name(index) + str(row)
            worksheet.conditional_format(cell_n, {'type': 'text',
                                                'criteria': 'containing',
                                                'value': 'TRUE',
                                                'format': green})
            row += 1

        
        def angels_awards_ivt_60(column_name, tmp_column=None):
            """Add conditional formatting to angels awards for ivt < 60."""
            row = 4
            while row < number_of_rows + 2:
                cell_n = column_name + str(row)   
                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': 'between',
                                                    'minimum': 50,
                                                    'maximum': 74.99,
                                                    'format': gold})
                
                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': '>=',
                                                    'value': 75,
                                                    'format': black})
                row += 1      
                
            row = 4
            if tmp_column is not None:
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    tmp_value = thrombectomy_patients[row-4]
                    if (float(tmp_value) == 0.0):
                        worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '==',
                                                        'value': 0.0,
                                                        'format': black})
                    row += 1

        index = column_names.index('% patients treated with door to thrombolysis < 60 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column)

        index = column_names.index('% patients treated with door to thrombectomy < 120 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column, tmp_column='# patients eligible thrombectomy')
            
        index = column_names.index('% patients treated with door to recanalization therapy < 60 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column)
       

        # angels_awards_ivt_60('D')


        def angels_awards_ivt_45(column_name, tmp_column=None):
            """Add conditional formatting to angels awards for ivt < 45."""
            row = 4
            while row < number_of_rows + 2:
                cell_n = column_name + str(row)
                if tmp_column is not None:
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 0.99,
                                                        'maximum': 49.99,
                                                        'format': plat})
                else:
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '<=',
                                                        'value': 49.99,
                                                        'format': plat})

                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': '>=',
                                                    'value': 50,
                                                    'format': black})
                row += 1

            if tmp_column is not None:
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    tmp_value = thrombectomy_patients[row-4]
                    if (float(tmp_value) == 0.0):
                        worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '<=',
                                                        'value': 0.99,
                                                        'format': black})
                    row += 1
                    
                        

        index = column_names.index('% patients treated with door to thrombolysis < 45 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_45(column)

        index = column_names.index('% patients treated with door to thrombectomy < 90 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_45(column, tmp_column='# patients eligible thrombectomy')

        index = column_names.index('% patients treated with door to recanalization therapy < 45 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_45(column)
            

        # setting colors of cells according to their values
        def angels_awards_recan(column_name):
            """Add conditional formatting to angels awards for recaalization procedures."""
            row = 4
            while row < number_of_rows + 2:
                cell_n = column_name + str(row)
                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': 'between',
                                                    'minimum': 5,
                                                    'maximum': 14.99,
                                                    'format': gold})

                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': 'between',
                                                    'minimum': 15,
                                                    'maximum': 24.99,
                                                    'format': plat})

                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': '>=',
                                                    'value': 25,
                                                    'format': black})
                row += 1

        index = column_names.index('% recanalization rate out of total ischemic incidence')
        angels_awards_recan(column_name=xl_col_to_name(index))


        def angels_awards_processes(column_name, count=True):
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

                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': 'between',
                                                    'minimum': 85,
                                                    'maximum': 89.99,
                                                    'format': plat})

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
            """Add conditional formatting to angels awards for hospitalization."""
            row = 4
            while row < number_of_rows + 2:
                cell_n = column_name + str(row)
                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': '<=',
                                                    'value': 0,
                                                    'format': plat})

                worksheet.conditional_format(cell_n, {'type': 'cell',
                                                    'criteria': '>=',
                                                    'value': 0.99,
                                                    'format': black})
                row += 1

        
        index = column_names.index('% stroke patients treated in a dedicated stroke unit / ICU')
        angels_awards_hosp(column_name=xl_col_to_name(index))

        
        # set color for proposed angel award
        def proposed_award(column_name):
            row = 4
            while row < nrow + 2:
                cell_n = column + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'STROKEREADY',
                                                    'format': green})

                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'GOLD',
                                                    'format': gold})

                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'PLATINUM',
                                                    'format': plat})

                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'DIAMOND',
                                                    'format': black})
                row += 1

        index = column_names.index('Proposed Award')
        column = xl_col_to_name(index)
        proposed_award(column)

        index = column_names.index('Proposed Award (old calculation)')
        column = xl_col_to_name(index)
        proposed_award(column)

        hidden_columns = ['% patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 45 minutes', 'Proposed Award (old calculation)']
        				
        for i in hidden_columns:
            if i in column_names:
                index = column_names.index(i)
                column = xl_col_to_name(index)
                worksheet.set_column(column + ":" + column, None, None, {'hidden': True})


class GenerateFormattedStats:
    """ Class generating formatted excel file containing all general statistics including formatted Angels Awards results. ! 
    
    :param df: the dataframe with preprocessed data
    :type df: pandas dataframe
    :param country: `True` if country is included in the reports as site
    :type country: bool
    :param country_code: the country code
    :type country_code: str
    :param split_sites: `True` if preprocessed data should be generated per site
    :type split_sites: bool
    :param site: the site ID
    :type site: str
    :param report: the type of the report, eg. quarter
    :type report: str
    :param quarter: the type of the period, eg. H1_2018
    :type quarter: str
    :param comp: `True` if the comparison calculation is in statistics
    :type comp: bool
    :param minimum_patients: the minimum number of patients sites need to met condition for total patients
    :type minimum_patients: int
    """

    def __init__(self, df, country=False, country_code=None, split_sites=False, site=None, report=None, quarter=None, comp=False, minimum_patients=30, country_name=None):

        self.df_unformatted = df.drop_duplicates(subset=['Site ID', 'Total Patients'], keep='first')
        self.df = df.drop_duplicates(subset=['Site ID', 'Total Patients'], keep='first')
        self.country_code = country_code
        self.report = report
        self.quarter = quarter
        self.comp = comp
        self.minimum_patients = minimum_patients
        self.total_patients_column = '# total patients >= {0}'.format(self.minimum_patients)

        self.thrombectomy_patients = self.df['# patients eligible thrombectomy'].values
        self.df.drop(['# patients eligible thrombectomy'], inplace=True, axis=1)

        import json
        # Read file with colors
        path = os.path.join(os.path.dirname(__file__), 'tmp', 'colors.json')
        with open(path, 'r', encoding='utf-8') as json_file:
            self.colors = json.load(json_file)

        def delete_columns(columns):
            """ The function deleting all temporary columns used for presentation. 
            
            :param columns: list of column names
            :type columns: list
            """
            for i in columns:
                if i in self.df.columns:
                    self.df.drop([i], inplace=True, axis=1)

        # Drop tmp column
        delete_columns(['isch_patients', 'is_ich_patients', 'is_ich_tia_cvt_patients', 'is_ich_cvt_patients', 'is_tia_patients', 'is_ich_sah_cvt_patients', 'is_tia_cvt_patients', 'cvt_patients', 'ich_sah_patients', 'ich_patients',  'sah_patients', 'discharge_subset_patients','discharge_subset_alive_patients', 'neurosurgery_patients', 'not_reffered_patients', 'reffered_patients', 'afib_detected_during_hospitalization_patients', 'afib_not_detected_or_not_known_patients', 'antithrombotics_patients', 'ischemic_transient_dead_patients', 'afib_flutter_not_detected_or_not_known_patients', 'afib_flutter_not_detected_or_not_known_dead_patients', 'prescribed_antiplatelets_no_afib_patients', 'prescribed_antiplatelets_no_afib_dead_patients', 'afib_flutter_detected_patients', 'anticoagulants_recommended_patients', 'afib_flutter_detected_dead_patients', 'recommended_antithrombotics_with_afib_alive_patients', 'discharge_subset_same_centre_patients', 'discharge_subset_another_centre_patients', 'patients_eligible_recanalization', '# patients having stroke in the hospital - No', '% patients having stroke in the hospital - No', '# recurrent stroke - No', '% recurrent stroke - No', '# patients assessed for rehabilitation - Not known', '% patients assessed for rehabilitation - Not known', '# level of consciousness - not known', '% level of consciousness - not known', '# CT/MRI - Performed later than 1 hour after admission', '% CT/MRI - Performed later than 1 hour after admission', '# patients put on ventilator - Not known', '% patients put on ventilator - Not known', '# patients put on ventilator - No', '% patients put on ventilator - No', '# IV tPa', '% IV tPa', '# TBY', '% TBY', '# DIDO TBY', '# dysphagia screening - not known', '% dysphagia screening - not known', '# dysphagia screening time - After first 24 hours', '% dysphagia screening time - After first 24 hours', '# other afib detection method - Not detected or not known', '% other afib detection method - Not detected or not known', '# carotid arteries imaging - Not known', '% carotid arteries imaging - Not known', '# carotid arteries imaging - No', '% carotid arteries imaging - No', 'vascular_imaging_cta_norm', 'vascular_imaging_mra_norm', 'vascular_imaging_dsa_norm', 'vascular_imaging_none_norm', 'bleeding_arterial_hypertension_perc_norm', 'bleeding_aneurysm_perc_norm', 'bleeding_arterio_venous_malformation_perc_norm', 'bleeding_anticoagulation_therapy_perc_norm', 'bleeding_amyloid_angiopathy_perc_norm', 'bleeding_other_perc_norm', 'intervention_endovascular_perc_norm', 'intervention_neurosurgical_perc_norm', 'intervention_other_perc_norm', 'intervention_referred_perc_norm', 'intervention_none_perc_norm', 'vt_treatment_anticoagulation_perc_norm', 'vt_treatment_thrombectomy_perc_norm', 'vt_treatment_local_thrombolysis_perc_norm', 'vt_treatment_local_neurological_treatment_perc_norm', 'except_recommended_patients', 'afib_detected_discharged_home_patients', '% dysphagia screening done', '# dysphagia screening done', 'alert_all', 'alert_all_perc', 'drowsy_all', 'drowsy_all_perc', 'comatose_all', 'comatose_all_perc', 'antithrombotics_patients_with_cvt', 'ischemic_transient_cerebral_dead_patients', '# patients receiving antiplatelets with CVT', '% patients receiving antiplatelets with CVT', '# patients receiving Vit. K antagonist with CVT', '% patients receiving Vit. K antagonist with CVT', '# patients receiving dabigatran with CVT', '% patients receiving dabigatran with CVT', '# patients receiving rivaroxaban with CVT', '% patients receiving rivaroxaban with CVT', '# patients receiving apixaban with CVT', '% patients receiving apixaban with CVT', '# patients receiving edoxaban with CVT', '% patients receiving edoxaban with CVT', '# patients receiving LMWH or heparin in prophylactic dose with CVT', '% patients receiving LMWH or heparin in prophylactic dose with CVT', '# patients receiving LMWH or heparin in full anticoagulant dose with CVT', '% patients receiving LMWH or heparin in full anticoagulant dose with CVT', '# patients not prescribed antithrombotics, but recommended with CVT', '% patients not prescribed antithrombotics, but recommended with CVT', '# patients neither receiving antithrombotics nor recommended with CVT', '% patients neither receiving antithrombotics nor recommended with CVT', '# patients prescribed antithrombotics with CVT', '% patients prescribed antithrombotics with CVT', '# patients prescribed or recommended antithrombotics with CVT', '% patients prescribed or recommended antithrombotics with CVT', 'afib_flutter_not_detected_or_not_known_patients_with_cvt', 'afib_flutter_not_detected_or_not_known_dead_patients_with_cvt', 'prescribed_antiplatelets_no_afib_patients_with_cvt', 'prescribed_antiplatelets_no_afib_dead_patients_with_cvt', '# patients prescribed antiplatelets without aFib with CVT', '% patients prescribed antiplatelets without aFib with CVT', 'afib_flutter_detected_patients_with_cvt', '# patients prescribed anticoagulants with aFib with CVT', 'anticoagulants_recommended_patients_with_cvt', 'afib_flutter_detected_dead_patients_with_cvt', '% patients prescribed anticoagulants with aFib with CVT', '# patients prescribed antithrombotics with aFib with CVT', 'recommended_antithrombotics_with_afib_alive_patients_with_cvt', '% patients prescribed antithrombotics with aFib with CVT', 'afib_flutter_detected_patients_not_dead', 'except_recommended_discharged_home_patients', 'afib_detected_discharged_patients', 'ischemic_transient_dead_patients_prescribed', 'is_tia_discharged_home_patients', '% patients detected for aFib', 'afib_flutter_detected_only', '# patients hospitalized in stroke unit / ICU or monitored bed', '% patients hospitalized in stroke unit / ICU or monitored bed', 'discharge_subset_alive_not_returned_back_patients', 'is_ich_not_referred_patients', '# carotid stenosis - >50%', '% carotid stenosis - >50%'])

        # If country is used as site, the country name is selected from countries dictionary by country code. :) 
        if (country):
            self.country_name = country_name
        else:
            self.country_name = country_name

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

    def _add_group_text(
        self, workbook, worksheet, color, start_column, end_column, column_names, out_of=None, group_name=None):
        ''' Function that will add the group text with provided formatting. 
        
        :param workbook: the active workbook object
        :type workbook: Workbook
        :param worksheet: Worksheet to which the group text should be added
        :type worksheet: WorkSheet
        :param color: the name of color from the colors.json
        :type color: str
        :param group_name: the name of group
        :type group_name: str
        :param start_column: the name of the start column
        :type start_column: str
        :param end_column: the name of the end column
        :type end_columns: str
        :param column_names: the list of columns names
        :type columns_names: list
        :param out_of: the text containing from out of
        :type out_of: str
        '''
        from xlsxwriter.utility import xl_rowcol_to_cell

        formatting = workbook.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': self.colors.get(color)
        })

        formatting_color = workbook.add_format({
            'fg_color': self.colors.get(color),
            'text_wrap': True
        })

        start_index = column_names.index(start_column)
        end_index = column_names.index(end_column)

        if group_name is not None:
            start_cell = xl_rowcol_to_cell(0, start_index)
            end_cell = xl_rowcol_to_cell(0, end_index)
            worksheet.merge_range(f'{start_cell}:{end_cell}', group_name, formatting)

        if out_of is not None:
            for i in range(start_index, end_index+1):
                if column_names[i].startswith('%'):
                    worksheet.write(xl_rowcol_to_cell(1, i), out_of, formatting_color)
                else:
                    worksheet.write(xl_rowcol_to_cell(1, i), '', formatting_color)

    def _generate_formatted_statistics(self, df, df_tmp, site_code=None):
        """ The function creating the new excel workbook and filling the statistics into it. 

        :param df: the dataframe with statistcs not including the temporary columns
        :type df: pandas dataframe
        :param df_tmp: the dataframe with statistics containing also temporary columns
        :type df_tmp: pandas dataframe
        :param site_code: the site ID
        :type site_code: str
        """

        if self.country_code is None and site_code is None:
            # Set filename for global report (including all sites)
            name_of_unformatted_stats = self.report + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + self.quarter + ".xlsx"
        elif site_code is None:
            # Set filename for country report (including sites and country results)
            name_of_unformatted_stats = self.report + "_" + self.country_code + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + self.country_code + "_" + self.quarter + ".xlsx"
        else:
            # Set filename for site report
            name_of_unformatted_stats = self.report + "_" + site_code + "_" + self.quarter + ".csv"
            name_of_output_file = self.report + "_" + site_code + "_" + self.quarter + ".xlsx"

        df_tmp.to_csv(name_of_unformatted_stats, sep=",", encoding='utf-8', index=False)
        workbook1 = xlsxwriter.Workbook(name_of_output_file, {'strings_to_numbers': True})
        worksheet = workbook1.add_worksheet()

        # set width of columns
        worksheet.set_column(0, 4, 15)
        worksheet.set_column(4, 350, 60)
        
        ncol = len(df.columns) - 1
        nrow = len(df) + 2

        column_names = df.columns.tolist()
        col = []
        # Create headers
        for i in range(0, ncol + 1):
            tmp = {}
            tmp['header'] = column_names[i]
            col.append(tmp)

        statistics = df.values.tolist()

        ##########
        # GENDER #
        ##########
        # set formatting for gender
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color="gender",
            start_column='# patients female', 
            end_column='% patients male',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='GENDER', 
        )

        ##########################
        # STROKE IN THE HOSPITAL #
        ##########################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='stroke_hosp', 
            start_column='# patients having stroke in the hospital - Yes', 
            end_column='% patients having stroke in the hospital - Yes',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='STROKE IN THE HOSPITAL',  
        )

        ####################
        # RECURRENT STROKE #
        ####################
        # set formatting for recurrent stroke
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet,
            color='recurrent_stroke', 
            start_column='# recurrent stroke - Yes', 
            end_column='% recurrent stroke - Yes',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='RECURRENT STROKE', 
        )

        ###################
        # DEPARTMENT TYPE #
        ###################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='department_type', 
            start_column='# department type - neurology',
            end_column='% department type - Other',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='DEPARTMENT TYPE', 
        )
        
        ###################
        # HOSPITALIZATION #
        ###################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='hospitalization', 
            start_column='# patients hospitalized in stroke unit / ICU',
            end_column='% patients hospitalized in standard bed',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='HOSPITALIZATION', 
        )

        #############################
        # REHABILITATION ASSESSMENT #
        #############################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='rehab', 
            start_column='# patients assessed for rehabilitation - Yes',
            end_column='% patients assessed for rehabilitation - No',
            column_names=column_names,
            out_of='out of # IS, ICH, SAH and CVT',
            group_name='REHABILITATION ASSESSMENT', 
        )

        ###############
        # STROKE TYPE #
        ###############
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='stroke', 
            start_column='# stroke type - ischemic stroke',
            end_column='% stroke type - undetermined stroke',
            column_names=column_names,
            out_of='out of # total patients',
            group_name='STROKE TYPE', 
        )

        #######################
        # CONSCIOUSNESS LEVEL #
        #######################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='consciousness', 
            start_column='# level of consciousness - alert',
            end_column='% level of consciousness - GCS',
            column_names=column_names,
            out_of='out of # ischemic + ICH',
            group_name='CONSCIOUSNESS LEVEL', 
        )

        #######
        # GCS #
        #######
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='gcs', 
            start_column='# GCS - 15-13',
            end_column='% GCS - <8',
            column_names=column_names,
            out_of='out of # ischemic + ICH',
            group_name='GLASGOW COMA SCALE', 
        )

        #########
        # NIHSS #
        #########
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='nihss', 
            start_column='# NIHSS - Not performed',
            end_column='NIHSS median score',
            column_names=column_names,
            out_of='out of # ischemic + ICH + CVT',
            group_name='NIHSS', 
        )

        ##########
        # CT/MRI #
        ##########
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='ct_mri', 
            start_column='# CT/MRI - Not performed',
            end_column='% CT/MRI - Performed within 1 hour after admission',
            column_names=column_names,
            out_of='out of # CT/MRI performed',
            group_name='CT/MRI', 
        )

        ####################
        # VASCULAR IMAGING #
        ####################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='vasc_img', 
            start_column='# vascular imaging - CTA',
            end_column='% vascular imaging - two modalities',
            column_names=column_names,
            out_of='out of # ICH + SAH',
            group_name='VASCULAR IMAGING', 
        )

        ##############
        # VENTILATOR #
        ##############
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='ventilator', 
            start_column='# patients put on ventilator - Yes',
            end_column='% patients put on ventilator - Yes',
            column_names=column_names,
            out_of='out of # IS + ICH + CVT',
            group_name='VENTILATOR', 
        )

        #############################
        # RECANALIZATION PROCEDURES #
        #############################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='recanalization_procedure', 
            start_column='# recanalization procedures - Not done',
            end_column='% recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre',
            column_names=column_names,
            out_of='out of # ischemic stroke',
            group_name='RECANALIZATION PROCEDURES', 
        )

        ################
        # MEDIAN TIMES #
        ################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='median_times', 
            start_column='Median DTN (minutes)',
            end_column='Median TBY DIDO (minutes)',
            column_names=column_names,
            out_of='',
            group_name='MEDIAN TIMES (minutes)', 
        )

        #############
        # DYSPHAGIA #
        #############
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='dysphagia', 
            start_column='# dysphagia screening - Guss test',
            end_column='% dysphagia screening - Unable to test',
            column_names=column_names,
            out_of='out of # IS + ICH + CVT',
            group_name='DYSPHAGIA SCREENING', 
        )

        #############
        # DYSPHAGIA #
        #############
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='dysphagia', 
            start_column='# dysphagia screening time - Within first 24 hours',
            end_column='% dysphagia screening time - Within first 24 hours',
            column_names=column_names,
            out_of='out of # Guss test + other test',
            group_name='DYSPHAGIA TIMES', 
        )

        ###################
        # HEMICRANIECTOMY #
        ###################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='hemicraniectomy', 
            start_column='# hemicraniectomy - Yes',
            end_column='% hemicraniectomy - Referred to another centre',
            column_names=column_names,
            out_of='out of # IS',
            group_name='HEMICRANIECTOMY', 
        )

        ################
        # NEUROSURGERY #
        ################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='neurosurgery', 
            start_column='# neurosurgery - Not known',
            end_column='% neurosurgery - No',
            column_names=column_names,
            out_of='out of # ICH',
            group_name='NEUROSURGERY', 
        )

        #####################
        # NEUROSURGERY TYPE #
        #####################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='neurosurgery_type', 
            start_column='# neurosurgery type - intracranial hematoma evacuation',
            end_column='% neurosurgery type - Referred to another centre',
            column_names=column_names,
            out_of='out of # ICH',
            group_name='NEUROSURGERY TYPE', 
        )

        ###################
        # BLEEDING REASON #
        ###################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='bleeding_reason', 
            start_column='# bleeding reason - arterial hypertension',
            end_column='% bleeding reason - more than one',
            column_names=column_names,
            out_of='out of # ICH',
            group_name='BLEEDING REASON', 
        )

        ###################
        # BLEEDING SOURCE #
        ###################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='bleeding_source', 
            start_column='# bleeding source - Known',
            end_column='% bleeding source - Not known',
            column_names=column_names,
            out_of='out of # ICH',
            group_name='BLEEDING SOURCE', 
        )

        ################
        # INTERVENTION #
        ################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='intervention', 
            start_column='# intervention - endovascular (coiling)',
            end_column='% intervention - more than one',
            column_names=column_names,
            out_of='out of # SAH',
            group_name='INTERVENTION', 
        )

        ################
        # VT TREATMENT #
        ################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='vt_treatment', 
            start_column='# VT treatment - anticoagulation',
            end_column='% VT treatment - more than one treatment',
            column_names=column_names,
            out_of='out of # CVT',
            group_name='VENOUS THROMBOSIS TREATMENT', 
        )

        #######################
        # ATRIAL FIBRILLATION #
        #######################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='afib', 
            start_column='# afib/flutter - Known',
            end_column='% other afib detection method - Yes',
            column_names=column_names,
            out_of=None,
            group_name='ATRIAL FIBRILLATION', 
        )

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='afib', 
            start_column='# afib/flutter - Known',
            end_column='% afib/flutter - Not known',
            column_names=column_names,
            out_of='out of # ischemic + TIA',
        )

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='afib', 
            start_column='# afib detection method - Telemetry with monitor allowing automatic detection of aFib',
            end_column='# other afib detection method - Yes',
            column_names=column_names,
            out_of='out of # detected during hospitalization',
        )

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='afib', 
            start_column='% other afib detection method - Yes',
            end_column='% other afib detection method - Yes',
            column_names=column_names,
            out_of='out of # not detected + not known',
        )

        ####################
        # CAROTID ARTERIES #
        ####################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='carot', 
            start_column='# carotid arteries imaging - Yes',
            end_column='% carotid arteries imaging - Yes',
            column_names=column_names,
            out_of='out of # alive ischemic + TIA',
            group_name='CAROTID ARTERIES IMAGING', 
        )


        ###################
        # ANTITHROMBOTICS #
        ###################

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='antithrombotics', 
            start_column='# patients receiving antiplatelets',
            end_column='% patients prescribed antithrombotics with aFib',
            column_names=column_names,
            group_name='ANTITHROMBOTICS', 
        )

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='antithrombotics', 
            start_column='# patients receiving antiplatelets',
            end_column='% patients receiving LMWH or heparin in full anticoagulant dose',
            column_names=column_names,
            out_of='out of # alive ischemic + TIA + CVT'
        )

        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='antithrombotics', 
            start_column='% patients prescribed anticoagulants with aFib',
            end_column='% patients prescribed anticoagulants with aFib',
            column_names=column_names,
            out_of='out of # alive with AF+'
        )

        ##########
        # STATIN #
        ##########
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='statin', 
            start_column='# patients prescribed statins - Yes',
            end_column='% patients prescribed statins - Not known',
            column_names=column_names,
            group_name='STATINS', 
            out_of='out of # IS + TIA'
        )


        ####################
        # CAROTID STENOSIS #
        ####################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='carotid_stenosis', 
            start_column='# carotid stenosis - 50%-70%',
            end_column='% carotid stenosis - Not known',
            column_names=column_names,
            group_name='CAROTID STENOSIS', 
            out_of='out of # IS + TIA'
        )

        ##############################
        # CAROTID STENOSIS FOLLOW UP #
        ##############################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='carot_foll', 
            start_column='# carotid stenosis followup - Yes',
            end_column='% carotid stenosis followup - Referred to another centre',
            column_names=column_names,
            group_name='CAROTID STENOSIS FOLLOW UP', 
            out_of='out of # IS + TIA'
        )

        ###############################
        # ANTIHYPERTENSIVE MEDICATION #
        ###############################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='antihypertensive', 
            start_column='# prescribed antihypertensives - Not known',
            end_column='% prescribed antihypertensives - No',
            column_names=column_names,
            group_name='ANTIHYPERTENSIVE MEDICATION', 
            out_of='out of # total patients - # ichemic reffered to another centre'
        )

        #####################
        # SMOKING CESSATION #
        #####################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='smoking', 
            start_column='# recommended to a smoking cessation program - not a smoker',
            end_column='% recommended to a smoking cessation program - No',
            column_names=column_names,
            group_name='SMOKING CESSATION', 
            out_of='out of # total patients - # ichemic reffered to another centre'
        )

        ##########################
        # Cerebrovascular expert #
        ##########################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='cerebrovascular', 
            start_column='# recommended to a cerebrovascular expert - Recommended, and appointment was made',
            end_column='% recommended to a cerebrovascular expert - Not recommended',
            column_names=column_names,
            group_name='CEREBROVASCULAR EXPERT', 
            out_of='out of # total patients - # ichemic reffered to another centre'
        )

        #########################
        # DISCHARGE DESTINATION #
        #########################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='discharge_destination', 
            start_column='# discharge destination - Home',
            end_column='% discharge destination - Dead',
            column_names=column_names,
            group_name='DISCHARGE DESTINATION', 
            out_of='out of # total patients - # ichemic reffered to another centre'
        )

        ##################################################
        # DISCHARGE DESTINATION - WITHIN THE SAME CENTRE #
        ##################################################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='discharge_destination_same_centre', 
            start_column='# transferred within the same centre - Acute rehabilitation',
            end_column='% transferred within the same centre - Another department',
            column_names=column_names,
            group_name='DISCHARGE DESTINATION WITHIN THE SAME CENTRE', 
            out_of='out of # transferred within the same centre'
        )

        #########################################################
        # DISCHARGE DESTINATION - TRANSFERRED TO ANOTHER CENTRE #
        #########################################################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='discharge_destination_another_centre', 
            start_column='# transferred to another centre - Stroke centre',
            end_column='% transferred to another centre - Another hospital',
            column_names=column_names,
            group_name='DISCHARGE DESTINATION TRANSFERRED TO ANOTHER CENTRE', 
            out_of='out of # transferred to another centre'
        )

        ################################################################
        # DISCHARGE DESTINATION - TRANSFERRED TO WITHIN ANOTHER CENTRE #
        ################################################################
        self._add_group_text(
            workbook=workbook1,
            worksheet=worksheet, 
            color='discharge_destination_within_another_centre', 
            start_column='# department transferred to within another centre - Acute rehabilitation',
            end_column='% department transferred to within another centre - Another department',
            column_names=column_names,
            group_name='DISCHARGE DESTINATION TRANSFERRED WITHIN TO ANOTHER CENTRE', 
            out_of='out of # transferred to another centre'
        )

        ################
        # angel awards #
        ################
        awards = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': self.colors.get("angel_awards")})

        awards_color = workbook1.add_format({
            'fg_color': self.colors.get("angel_awards")})

        first_index = column_names.index(self.total_patients_column)
        last_index = column_names.index('Proposed Award')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'ESO ANGELS AWARDS', awards)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), '', awards_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', awards_color)

        hidden_columns = ['# patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 60 minutes', '# patients treated with door to recanalization therapy < 45 minutes', '% patients treated with door to recanalization therapy < 45 minutes', '# patients treated with door to thrombolysis < 60 minutes', '# patients treated with door to thrombolysis < 45 minutes', '# patients treated with door to thrombectomy < 120 minutes', '# patients treated with door to thrombectomy < 90 minutes', '# recanalization rate out of total ischemic incidence', '# suspected stroke patients undergoing CT/MRI', '# all stroke patients undergoing dysphagia screening', '# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets', '# ischemic stroke patients discharged (home) with antiplatelets', '# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants', '# afib patients discharged (home) with anticoagulants', '# stroke patients treated in a dedicated stroke unit / ICU', 'Proposed Award (old calculation)']
        				
        for i in hidden_columns:
            index = column_names.index(i)
            column = xl_col_to_name(index)
            worksheet.set_column(column + ":" + column, None, None, {'hidden': True})

        # format for green color
        green = workbook1.add_format({
            'bold': 2,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': self.colors.get("green")})

        # format for gold color
        gold = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': self.colors.get("gold")})

        # format for platinum color
        plat = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': self.colors.get("platinum")})

        # format for gold black
        black = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#000000',
            'color': self.colors.get("black")})

        # format for red color
        red = workbook1.add_format({
            'bold': 1,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': self.colors.get("red")})


        # add table into worksheet
        options = {'data': statistics,
                   'header_row': True,
                   'columns': col,
                   'style': 'Table Style Light 8'
                   }

        worksheet.add_table(2, 0, nrow, ncol, options)

        # total number of rows
        number_of_rows = len(statistics) + 2

        if not self.comp:    
            row = 4
            while row < nrow + 2:
                index = column_names.index(self.total_patients_column)
                cell_n = xl_col_to_name(index) + str(row)
                worksheet.conditional_format(cell_n, {'type': 'text',
                                                    'criteria': 'containing',
                                                    'value': 'TRUE',
                                                    'format': green})
                row += 1

            
            def angels_awards_ivt_60(column_name, tmp_column=None):
                """Add conditional formatting to angels awards for ivt < 60."""
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)   
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 50,
                                                        'maximum': 74.99,
                                                        'format': gold})
                    
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 75,
                                                        'format': black})
                    row += 1      
                    
                row = 4
                if tmp_column is not None:
                    while row < number_of_rows + 2:
                        cell_n = column_name + str(row)
                        tmp_value = self.thrombectomy_patients[row-4]
                        if (float(tmp_value) == 0.0):
                            worksheet.conditional_format(cell_n, {'type': 'cell',
                                                            'criteria': '==',
                                                            'value': 0.0,
                                                            'format': black})
                        row += 1

            index = column_names.index('% patients treated with door to thrombolysis < 60 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_60(column)

            index = column_names.index('% patients treated with door to thrombectomy < 120 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_60(column, tmp_column='% recanalization rate out of total ischemic incidence')
                
            index = column_names.index('% patients treated with door to recanalization therapy < 60 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_60(column)
        

            # angels_awards_ivt_60('D')


            def angels_awards_ivt_45(column_name, tmp_column=None):
                """Add conditional formatting to angels awards for ivt < 45."""
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    if tmp_column is not None:
                        worksheet.conditional_format(cell_n, {'type': 'cell',
                                                            'criteria': 'between',
                                                            'minimum': 0.99,
                                                            'maximum': 49.99,
                                                            'format': plat})
                    else:
                        worksheet.conditional_format(cell_n, {'type': 'cell',
                                                            'criteria': '<=',
                                                            'value': 49.99,
                                                            'format': plat})

                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 50,
                                                        'format': black})
                    row += 1

                if tmp_column is not None:
                    row = 4
                    while row < number_of_rows + 2:
                        cell_n = column_name + str(row)
                        tmp_value = self.thrombectomy_patients[row-4]
                        if (float(tmp_value) == 0.0):
                            worksheet.conditional_format(cell_n, {'type': 'cell',
                                                            'criteria': '<=',
                                                            'value': 0.99,
                                                            'format': black})
                        row += 1
                        
                            

            index = column_names.index('% patients treated with door to thrombolysis < 45 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_45(column)

            index = column_names.index('% patients treated with door to thrombectomy < 90 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_45(column, tmp_column='% recanalization rate out of total ischemic incidence')

            index = column_names.index('% patients treated with door to recanalization therapy < 45 minutes')
            column = xl_col_to_name(index)
            angels_awards_ivt_45(column)
                

            # setting colors of cells according to their values
            def angels_awards_recan(column_name):
                """Add conditional formatting to angels awards for recaalization procedures."""
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 5,
                                                        'maximum': 14.99,
                                                        'format': gold})

                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 15,
                                                        'maximum': 24.99,
                                                        'format': plat})

                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 25,
                                                        'format': black})
                    row += 1

            index = column_names.index('% recanalization rate out of total ischemic incidence')
            angels_awards_recan(column_name=xl_col_to_name(index))


            def angels_awards_processes(column_name, count=True):
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

                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': 'between',
                                                        'minimum': 85,
                                                        'maximum': 89.99,
                                                        'format': plat})

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
                """Add conditional formatting to angels awards for hospitalization."""
                row = 4
                while row < number_of_rows + 2:
                    cell_n = column_name + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '<=',
                                                        'value': 0,
                                                        'format': plat})

                    worksheet.conditional_format(cell_n, {'type': 'cell',
                                                        'criteria': '>=',
                                                        'value': 0.99,
                                                        'format': black})
                    row += 1

            
            index = column_names.index('% stroke patients treated in a dedicated stroke unit / ICU')
            angels_awards_hosp(column_name=xl_col_to_name(index))

            
            # set color for proposed angel award
            def proposed_award(column_name):
                row = 4
                while row < nrow + 2:
                    cell_n = column + str(row)
                    worksheet.conditional_format(cell_n, {'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'STROKEREADY',
                                                        'format': green})

                    worksheet.conditional_format(cell_n, {'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'GOLD',
                                                        'format': gold})

                    worksheet.conditional_format(cell_n, {'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'PLATINUM',
                                                        'format': plat})

                    worksheet.conditional_format(cell_n, {'type': 'text',
                                                        'criteria': 'containing',
                                                        'value': 'DIAMOND',
                                                        'format': black})
                    row += 1

            index = column_names.index('Proposed Award')
            column = xl_col_to_name(index)
            proposed_award(column)
            
        else:
            pass

        workbook1.close()