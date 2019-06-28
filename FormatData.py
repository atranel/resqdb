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
import sqlite3
import csv
import pandas as pd
import numpy as np
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name
import logging
from collections import defaultdict
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

    def __init__(self, df, split_sites=False, site=None, report=None, quarter=None, country_code=None, csv=False):

        debug = 'debug_' + datetime.now().strftime('%d-%m-%Y') + '.log' 
        log_file = os.path.join(os.getcwd(), debug)
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
                logging.info('FormatData: Preprocessed data: The preprocessed data were generated for site {0}'.format(site))

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
            output_file = self.report + "_" + self.country_code + "_" + self.quarter + "_preprocessed_data.xlsx"
                
        df = df.copy()
        
        # Convert dates to strings
        dateformat = "%m/%d/%Y"
        timeformat = "%H:%M"
        def convert_to_string(datetime, format):
            if datetime is None or datetime is np.nan:
                return datetime
            else:
                return datetime.strftime(format)
        
        if not csv:
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
            print(df['DISCHARGE_DATE'].dtype)
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
            df['CT_TIME'] = df.apply(lambda x: convert_to_string(x['CT_TIME'], timeformat), axis=1)
        else:
            df['HOSPITAL_DATE'] = df.apply(lambda x: convert_to_string(x['HOSPITAL_DATE'], dateformat), axis=1)
            df['DISCHARGE_DATE'] = df.apply(lambda x: convert_to_string(x['DISCHARGE_DATE'], dateformat), axis=1)

        df.fillna(value="", inplace=True)

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
    def __init__(self, df, report=None, quarter=None, minimum_patients=30):

        self.df = df
        self.report = report
        self.quarter = quarter
        self.minimum_patients = minimum_patients

        self.formate(self.df)

    def formate(self, df):
        """ The function formatting the Angels Awards data. 

        :param df: the temporary dataframe containing only column needed to propose award
        :type df: pandas dataframe
        """
        if self.report is None and self.quarter is None:
            output_file = "angels_awards.xslx"
        else:
            output_file = self.report + "_" + self.quarter + "_angels_awards.xlsx"
            
        workbook1 = xlsxwriter.Workbook(output_file, {'strings_to_numbers': True})
        worksheet = workbook1.add_worksheet()

        worksheet.set_column(0, 2, 15)
        worksheet.set_column(2, 20, 40)

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
        awards = []
        row = 4
        while row < nrow + 2:
            index = column_names.index(self.total_patients_column)
            cell_n = xl_col_to_name(index) + str(row)
            worksheet.conditional_format(cell_n, {'type': 'text',
                                                'criteria': 'containing',
                                                'value': 'TRUE',
                                                'format': green})
            row += 1


        def angels_awards_ivt_60(column_name):
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
        angels_awards_ivt_60(column)

        index = column_names.index('% patients treated with door to thrombectomy < 90 minutes')
        column = xl_col_to_name(index)
        angels_awards_ivt_60(column)

        # angels_awards_ivt_60('D')


        def angels_awards_ivt_45(column_name):
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
        angels_awards_ivt_45(column)

        index = column_names.index('% patients treated with door to thrombectomy < 60 minutes')
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

        
        # set color for proposed angel award
        def proposed_award(column_name):
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
        proposed_award(column)

        workbook1.close()


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

    def __init__(self, df, country=False, country_code=None, split_sites=False, site=None, report=None, quarter=None, comp=False, minimum_patients=30):

        self.df_unformatted = df.copy()
        self.df = df.copy()
        self.country_code = country_code
        self.report = report
        self.quarter = quarter
        self.comp = comp
        self.minimum_patients = minimum_patients
        self.total_patients_column = '# total patients >= {0}'.format(self.minimum_patients)

        def delete_columns(columns):
            """ The function deleting all temporary columns used for presentation. 
            
            :param columns: list of column names
            :type columns: list
            """
            for i in columns:
                if i in self.df.columns:
                    self.df.drop([i], inplace=True, axis=1)

        # Drop tmp column 
        delete_columns(['isch_patients', 'is_ich_patients', 'is_ich_tia_cvt_patients', 'is_ich_cvt_patients', 'is_tia_patients', 'is_ich_sah_cvt_patients', 'is_tia_cvt_patients', 'cvt_patients', 'ich_sah_patients', 'ich_patients',  'sah_patients', 'discharge_subset_patients','discharge_subset_alive_patients', 'neurosurgery_patients', 'not_reffered_patients', 'reffered_patients', 'afib_detected_during_hospitalization_patients', 'afib_not_detected_or_not_known_patients', 'antithrombotics_patients', 'ischemic_transient_dead_patients', 'afib_flutter_not_detected_or_not_known_patients', 'afib_flutter_not_detected_or_not_known_dead_patients', 'prescribed_antiplatelets_no_afib_patients', 'prescribed_antiplatelets_no_afib_dead_patients', 'afib_flutter_detected_patients', 'anticoagulants_recommended_patients', 'afib_flutter_detected_dead_patients', 'recommended_antithrombotics_with_afib_alive_patients', 'discharge_subset_same_centre_patients', 'discharge_subset_another_centre_patients', 'patients_eligible_recanalization', '# patients having stroke in the hospital - No', '% patients having stroke in the hospital - No', '# recurrent stroke - No', '% recurrent stroke - No', '# patients assessed for rehabilitation - Not known', '% patients assessed for rehabilitation - Not known', '# level of consciousness - not known', '% level of consciousness - not known', '# CT/MRI - Performed later than 1 hour after admission', '% CT/MRI - Performed later than 1 hour after admission', '# patients put on ventilator - Not known', '% patients put on ventilator - Not known', '# patients put on ventilator - No', '% patients put on ventilator - No', '# IV tPa', '% IV tPa', '# TBY', '% TBY', '# DIDO TBY', '# dysphagia screening - not known', '% dysphagia screening - not known', '# dysphagia screening time - After first 24 hours', '% dysphagia screening time - After first 24 hours', '# other afib detection method - Not detected or not known', '% other afib detection method - Not detected or not known', '# carotid arteries imaging - Not known', '% carotid arteries imaging - Not known', '# carotid arteries imaging - No', '% carotid arteries imaging - No', 'vascular_imaging_cta_norm', 'vascular_imaging_mra_norm', 'vascular_imaging_dsa_norm', 'vascular_imaging_none_norm', 'bleeding_arterial_hypertension_perc_norm', 'bleeding_aneurysm_perc_norm', 'bleeding_arterio_venous_malformation_perc_norm', 'bleeding_anticoagulation_therapy_perc_norm', 'bleeding_amyloid_angiopathy_perc_norm', 'bleeding_other_perc_norm', 'intervention_endovascular_perc_norm', 'intervention_neurosurgical_perc_norm', 'intervention_other_perc_norm', 'intervention_referred_perc_norm', 'intervention_none_perc_norm', 'vt_treatment_anticoagulation_perc_norm', 'vt_treatment_thrombectomy_perc_norm', 'vt_treatment_local_thrombolysis_perc_norm', 'vt_treatment_local_neurological_treatment_perc_norm', 'except_recommended_patients', 'afib_detected_discharged_home_patients', '% dysphagia screening done', '# dysphagia screening done', 'alert_all', 'alert_all_perc', 'drowsy_all', 'drowsy_all_perc', 'comatose_all', 'comatose_all_perc', 'antithrombotics_patients_with_cvt', 'ischemic_transient_cerebral_dead_patients', '# patients receiving antiplatelets with CVT', '% patients receiving antiplatelets with CVT', '# patients receiving Vit. K antagonist with CVT', '% patients receiving Vit. K antagonist with CVT', '# patients receiving dabigatran with CVT', '% patients receiving dabigatran with CVT', '# patients receiving rivaroxaban with CVT', '% patients receiving rivaroxaban with CVT', '# patients receiving apixaban with CVT', '% patients receiving apixaban with CVT', '# patients receiving edoxaban with CVT', '% patients receiving edoxaban with CVT', '# patients receiving LMWH or heparin in prophylactic dose with CVT', '% patients receiving LMWH or heparin in prophylactic dose with CVT', '# patients receiving LMWH or heparin in full anticoagulant dose with CVT', '% patients receiving LMWH or heparin in full anticoagulant dose with CVT', '# patients not prescribed antithrombotics, but recommended with CVT', '% patients not prescribed antithrombotics, but recommended with CVT', '# patients neither receiving antithrombotics nor recommended with CVT', '% patients neither receiving antithrombotics nor recommended with CVT', '# patients prescribed antithrombotics with CVT', '% patients prescribed antithrombotics with CVT', '# patients prescribed or recommended antithrombotics with CVT', '% patients prescribed or recommended antithrombotics with CVT', 'afib_flutter_not_detected_or_not_known_patients_with_cvt', 'afib_flutter_not_detected_or_not_known_dead_patients_with_cvt', 'prescribed_antiplatelets_no_afib_patients_with_cvt', 'prescribed_antiplatelets_no_afib_dead_patients_with_cvt', '# patients prescribed antiplatelets without aFib with CVT', '% patients prescribed antiplatelets without aFib with CVT', 'afib_flutter_detected_patients_with_cvt', '# patients prescribed anticoagulants with aFib with CVT', 'anticoagulants_recommended_patients_with_cvt', 'afib_flutter_detected_dead_patients_with_cvt', '% patients prescribed anticoagulants with aFib with CVT', '# patients prescribed antithrombotics with aFib with CVT', 'recommended_antithrombotics_with_afib_alive_patients_with_cvt', '% patients prescribed antithrombotics with aFib with CVT', 'afib_flutter_detected_patients_not_dead', 'except_recommended_discharged_home_patients', 'afib_detected_discharged_patients', 'ischemic_transient_dead_patients_prescribed', 'is_tia_discharged_home_patients', '# patients treated with door to recanalization therapy < 60 minutes', '% patients treated with door to recanalization therapy < 60 minutes', '# patients treated with door to recanalization therapy < 45 minutes', '% patients treated with door to recanalization therapy < 45 minutes'])

        def select_country(value):
            """ The function obtaining from the pytz package the country name based on the country code. 

            :param value: the country code
            :type value: str
            """
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

        ##########
        # GENDER #
        ##########
        # set formatting for gender
        gender = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("gender")})

        gender_color = workbook1.add_format({
            'fg_color': colors.get("gender"),
            'text_wrap': True})

        first_index = column_names.index('# patients female')
        last_index = column_names.index('% patients male')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        #worksheet.merge_range('E1:H1', 'GENDER', gender)
        worksheet.merge_range(first_cell + ":" + last_cell, 'GENDER', gender)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', gender_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', gender_color)

        ##########################
        # STROKE IN THE HOSPITAL #
        ##########################
        stroke_hosp = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("stroke_hosp")})

        stroke_hosp_color = workbook1.add_format({
            'fg_color': colors.get("stroke_hosp"),
            'text_wrap': True})

        first_index = column_names.index('# patients having stroke in the hospital - Yes')
        last_index = column_names.index('% patients having stroke in the hospital - Yes')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'STROKE IN THE HOSPITAL', stroke_hosp)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', stroke_hosp_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', stroke_hosp_color)

        ####################
        # RECURRENT STROKE #
        ####################
        # set formatting for recurrent stroke
        recurrent_stroke = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("recurrent_stroke")})

        recurrent_stroke_color = workbook1.add_format({
            'fg_color': colors.get("recurrent_stroke"),
            'text_wrap': True})

        first_index = column_names.index('# recurrent stroke - Yes')
        last_index = column_names.index('% recurrent stroke - Yes')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'RECURRENT STROKE', recurrent_stroke)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', recurrent_stroke_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', recurrent_stroke_color)

        ###################
        # DEPARTMENT TYPE #
        ###################

        department_type = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("department_type")})

        department_type_color = workbook1.add_format({
            'fg_color': colors.get("department_type"),
            'text_wrap': True})

        first_index = column_names.index('# department type - neurology')
        last_index = column_names.index('% department type - Other')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'DEPARTMENT TYPE', department_type)
        
        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', department_type_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', department_type_color)
        
        ###################
        # HOSPITALIZATION #
        ###################

        hospitalization = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("hospitalization")})

        hospitalization_color = workbook1.add_format({
            'fg_color': colors.get("hospitalization"),
            'text_wrap': True})

        first_index = column_names.index('# patients hospitalized in stroke unit / ICU')
        last_index = column_names.index('% patients hospitalized in standard bed')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'HOSPITALIZATION', hospitalization)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', hospitalization_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', hospitalization_color)

        #############################
        # REHABILITATION ASSESSMENT #
        #############################

        rehab_assess = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("rehab")})

        rehab_assess_color = workbook1.add_format({
            'fg_color': colors.get("rehab"),
            'text_wrap': True})

        first_index = column_names.index('# patients assessed for rehabilitation - Yes')
        last_index = column_names.index('% patients assessed for rehabilitation - No')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'REHABILITATION ASSESSMENT', rehab_assess)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', rehab_assess_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', rehab_assess_color)

        ###############
        # STROKE TYPE #
        ###############
        stroke_type = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("stroke")})

        stroke_color = workbook1.add_format({
            'fg_color': colors.get("stroke"),
            'text_wrap': True})


        first_index = column_names.index('# stroke type - ischemic stroke')
        last_index = column_names.index('% stroke type - undetermined stroke')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'STROKE TYPE', stroke_type)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients', stroke_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', stroke_color)

        #######################
        # CONSCIOUSNESS LEVEL #
        #######################

        consciousness_level = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("consciousness")})

        consciousness_level_color = workbook1.add_format({
            'fg_color': colors.get("consciousness"),
            'text_wrap': True})

        first_index = column_names.index('# level of consciousness - alert')
        last_index = column_names.index('% level of consciousness - GCS')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'CONSCIOUSNESS LEVEL', consciousness_level)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ischemic + ICH', consciousness_level_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', consciousness_level_color)

        #######
        # GCS #
        #######

        gcs = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("gcs")})

        gcs_color = workbook1.add_format({
            'fg_color': colors.get("gcs"),
            'text_wrap': True})

        first_index = column_names.index('# GCS - 15-13')
        last_index = column_names.index('% GCS - <8')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'GLASGOW COMA SCALE', gcs)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ischemic + ICH', gcs_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', gcs_color)

        #########
        # NIHSS #
        #########
        nihss = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("nihss")})

        nihss_color = workbook1.add_format({
            'fg_color': colors.get("nihss"),
            'text_wrap': True})

        first_index = column_names.index('# NIHSS - Not performed')
        last_index = column_names.index('NIHSS median score')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'NIHSS', nihss)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ischemic + ICH + CVT', nihss_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', nihss_color)

        ##########
        # CT/MRI #
        ##########
        ct_mri = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("ct_mri")})

        ct_mri_color = workbook1.add_format({
            'fg_color': colors.get("ct_mri"),
            'text_wrap': True})

        first_index = column_names.index('# CT/MRI - Not performed')
        last_index = column_names.index('% CT/MRI - Performed within 1 hour after admission')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'CT/MRI', ct_mri)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # CT/MRI performed', ct_mri_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', ct_mri_color)

        ####################
        # VASCULAR IMAGING #
        ####################
        vascular_imaging = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("vasc_img")})

        vascular_imaging_color = workbook1.add_format({
            'fg_color': colors.get("vasc_img"),
            'text_wrap': True})

        first_index = column_names.index('# vascular imaging - CTA')
        last_index = column_names.index('% vascular imaging - two modalities')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)
        worksheet.merge_range(first_cell + ":" + last_cell, 'VASCULAR IMAGING', vascular_imaging)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ICH + SAH', vascular_imaging_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', vascular_imaging_color)

        ##############
        # VENTILATOR #
        ##############
        ventilator = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("ventilator")})

        ventilator_color = workbook1.add_format({
            'fg_color': colors.get("ventilator"),
            'text_wrap': True})

        first_index = column_names.index('# patients put on ventilator - Yes')
        last_index = column_names.index('% patients put on ventilator - Yes')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'VENTILATOR', ventilator)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS + ICH + CVT', ventilator_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', ventilator_color)

        #############################
        # RECANALIZATION PROCEDURES #
        #############################
        recanalization_procedures = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("recanalization_procedure")})

        recanalization_color = workbook1.add_format({
            'fg_color': colors.get("recanalization_procedure"),
            'text_wrap': True})


        first_index = column_names.index('# recanalization procedures - Not done')
        last_index = column_names.index('% recanalization procedures - Returned to the initial centre after recanalization procedures were performed at another centre')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'RECANALIZATION PROCEDURES', recanalization_procedures)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ischemic stroke', recanalization_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', recanalization_color)

        ################
        # MEDIAN TIMES #
        ################
        median_times = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("median_times")})

        median_times_color = workbook1.add_format({
            'fg_color': colors.get("median_times"),
            'text_wrap': True})

        first_index = column_names.index('Median DTN (minutes)')
        last_index = column_names.index('Median TBY DIDO (minutes)')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'MEDIAN TIMES (minutes)', median_times)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), '', median_times_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', median_times_color)

        #############
        # DYSPHAGIA #
        #############
        dysphagia = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("dysphagia")})

        dysphagia_color = workbook1.add_format({
            'fg_color': colors.get("dysphagia"),
            'text_wrap': True})

        first_index = column_names.index('# dysphagia screening - Guss test')
        last_index = column_names.index('% dysphagia screening - Unable to test')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DYSPHAGIA SCREENING', dysphagia)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS + ICH + CVT', dysphagia_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', dysphagia_color)

        #############
        # DYSPHAGIA #
        #############
        dysphagia = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("dysphagia")})

        dysphagia_color = workbook1.add_format({
            'fg_color': colors.get("dysphagia"),
            'text_wrap': True})

        first_index = column_names.index('# dysphagia screening time - Within first 24 hours')
        last_index = column_names.index('% dysphagia screening time - Within first 24 hours')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DYSPHAGIA TIMES', dysphagia)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # Guss test + other test', dysphagia_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', dysphagia_color)

        ###################
        # HEMICRANIECTOMY #
        ###################
        hemicraniectomy = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("hemicraniectomy")})

        hemicraniectomy_color = workbook1.add_format({
            'fg_color': colors.get("hemicraniectomy"),
            'text_wrap': True})

        first_index = column_names.index('# hemicraniectomy - Yes')
        last_index = column_names.index('% hemicraniectomy - Referred to another centre')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'HEMICRANIECTOMY', hemicraniectomy)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS', hemicraniectomy_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', hemicraniectomy_color)

        ################
        # NEUROSURGERY #
        ################
        neurosurgery = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("neurosurgery")})

        neurosurgery_color = workbook1.add_format({
            'fg_color': colors.get("neurosurgery"),
            'text_wrap': True})

        first_index = column_names.index('# neurosurgery - Not known')
        last_index = column_names.index('% neurosurgery - No')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'NEUROSURGERY', neurosurgery)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ICH', neurosurgery_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', neurosurgery_color)


        #####################
        # NEUROSURGERY TYPE #
        #####################
        neurosurgery_type = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("neurosurgery_type")})

        neurosurgery_type_color = workbook1.add_format({
            'fg_color': colors.get("neurosurgery_type"),
            'text_wrap': True})

        first_index = column_names.index('# neurosurgery type - intracranial hematoma evacuation')
        last_index = column_names.index('% neurosurgery type - Referred to another centre')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'NEUROSURGERY TYPE', neurosurgery_type)


        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ICH', neurosurgery_type_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', neurosurgery_type_color)


        ###################
        # BLEEDING REASON #
        ###################
        bleeding_reason = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("bleeding_reason")})

        bleeding_reason_color = workbook1.add_format({
            'fg_color': colors.get("bleeding_reason"),
            'text_wrap': True})

        first_index = column_names.index('# bleeding reason - arterial hypertension')
        last_index = column_names.index('% bleeding reason - more than one')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'BLEEDING REASON', bleeding_reason)


        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ICH', bleeding_reason_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', bleeding_reason_color)

        ###################
        # BLEEDING SOURCE #
        ###################
        bleeding_source = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("bleeding_source")})

        bleeding_source_color = workbook1.add_format({
            'fg_color': colors.get("bleeding_source"),
            'text_wrap': True})

        first_index = column_names.index('# bleeding source - Known')
        last_index = column_names.index('% bleeding source - Not known')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'BLEEDING SOURCE', bleeding_source)


        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ICH', bleeding_source_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', bleeding_source_color)

        ################
        # INTERVENTION #
        ################
        intervention = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("intervention")})

        intervention_color = workbook1.add_format({
            'fg_color': colors.get("intervention"),
            'text_wrap': True})

        first_index = column_names.index('# intervention - endovascular (coiling)')
        last_index = column_names.index('% intervention - more than one')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'INTERVENTION', intervention)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # SAH', intervention_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', intervention_color)

        ################
        # VT TREATMENT #
        ################
        vt_treatment = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("vt_treatment")})

        vt_treatment_color = workbook1.add_format({
            'fg_color': colors.get("vt_treatment"),
            'text_wrap': True})

        first_index = column_names.index('# VT treatment - anticoagulation')
        last_index = column_names.index('% VT treatment - more than one treatment')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'VENOUS THROMBOSIS TREATMENT', vt_treatment)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # CVT', intervention_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', intervention_color)

        #######################
        # ATRIAL FIBRILLATION #
        #######################
        atrial_fibrillation = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("afib")})

        afib_color = workbook1.add_format({
            'fg_color': colors.get("afib"),
            'text_wrap': True})

        first_index = column_names.index('# afib/flutter - Known')
        last_index = column_names.index('% other afib detection method - Yes')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'ATRIAL FIBRILLATION', atrial_fibrillation)

        first_index = column_names.index('# afib/flutter - Known')
        last_index = column_names.index('% afib/flutter - Not known')

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # ischemic + TIA', afib_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', afib_color)

        first_index = column_names.index('# afib detection method - Telemetry with monitor allowing automatic detection of aFib')
        last_index = column_names.index('# other afib detection method - Yes')

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # detected during hospitalization', afib_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', afib_color)

        worksheet.write(xl_rowcol_to_cell(1, column_names.index('% other afib detection method - Yes')), 'out of # not detected + not known', afib_color)


        ####################
        # CAROTID ARTERIES #
        ####################
        carot = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("carot")})

        carot_color = workbook1.add_format({
            'fg_color': colors.get("carot"),
            'text_wrap': True})

        first_index = column_names.index('# carotid arteries imaging - Yes')
        last_index = column_names.index('% carotid arteries imaging - Yes')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'CAROTID ARTERIES IMAGING', carot)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # alive ischemic + TIA', carot_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', carot_color)


        ###################
        # ANTITHROMBOTICS #
        ###################
        antithrombotics = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("antithrombotics")})

        antithrombotics_colors = workbook1.add_format({
            'fg_color': colors.get("antithrombotics"),
            'text_wrap': True})

        first_index = column_names.index('# patients receiving antiplatelets')
        last_index = column_names.index('% patients prescribed antithrombotics with aFib')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'ANTITHROMBOTICS', antithrombotics)

        first_index = column_names.index('# patients receiving antiplatelets')
        last_index = column_names.index('% patients receiving LMWH or heparin in full anticoagulant dose')
        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # alive ischemic + TIA + CVT', antithrombotics_colors)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', antithrombotics_colors)

        first_index = column_names.index('% patients prescribed anticoagulants with aFib')
        last_index = column_names.index('% patients prescribed antithrombotics with aFib')
        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # alive with AF+', antithrombotics_colors)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', antithrombotics_colors)

        ##########
        # STATIN #
        ##########
        statin = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("statin")})

        statin_color = workbook1.add_format({
            'fg_color': colors.get("statin"),
            'text_wrap': True})

        first_index = column_names.index('# patients prescribed statins - Yes')
        last_index = column_names.index('% patients prescribed statins - Not known')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'STATINS', statin)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS + TIA', statin_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', statin_color)


        ####################
        # CAROTID STENOSIS #
        ####################
        carotid_stenosis = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("carotid_stenosis")})

        carotid_stenosis_color = workbook1.add_format({
            'fg_color': colors.get("carotid_stenosis"),
            'text_wrap': True})

        first_index = column_names.index('# carotid stenosis - 50%-70%')
        last_index = column_names.index('% carotid stenosis - Not known')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'CAROTID STENOSIS', carotid_stenosis)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS + TIA', carotid_stenosis_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', carotid_stenosis_color)

        ##############################
        # CAROTID STENOSIS FOLLOW UP #
        ##############################
        carotid_stenosis_foll = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("carot_foll")})

        carotid_stenosis_foll_color = workbook1.add_format({
            'fg_color': colors.get("carot_foll"),
            'text_wrap': True})

        first_index = column_names.index('# carotid stenosis followup - Yes')
        last_index = column_names.index('% carotid stenosis followup - Referred to another centre')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'CAROTID STENOSIS FOLLOW UP', carotid_stenosis_foll)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # IS + TIA', carotid_stenosis_foll_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', carotid_stenosis_foll_color)

        ###############################
        # ANTIHYPERTENSIVE MEDICATION #
        ###############################
        antihypertensive = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("antihypertensive")})

        antihypertensive_color = workbook1.add_format({
            'fg_color': colors.get("antihypertensive"),
            'text_wrap': True})

        first_index = column_names.index('# prescribed antihypertensives - Not known')
        last_index = column_names.index('% prescribed antihypertensives - No')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'ANTIHYPERTENSIVE MEDICATION', antihypertensive)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients - # ichemic reffered to another centre', antihypertensive_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', antihypertensive_color)


        #####################
        # SMOKING CESSATION #
        #####################
        smoking = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("smoking")})

        smoking_color = workbook1.add_format({
            'fg_color': colors.get("smoking"),
            'text_wrap': True})

        first_index = column_names.index('# recommended to a smoking cessation program - not a smoker')
        last_index = column_names.index('% recommended to a smoking cessation program - No')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'SMOKING CESSATION', smoking)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients - # ichemic reffered to another centre', smoking_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', smoking_color)


        ##########################
        # Cerebrovascular expert #
        ##########################
        cerebrovascular = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("cerebrovascular")})

        cerebrovascular_color = workbook1.add_format({
            'fg_color': colors.get("cerebrovascular"),
            'text_wrap': True})

        first_index = column_names.index('# recommended to a cerebrovascular expert - Recommended, and appointment was made')
        last_index = column_names.index('% recommended to a cerebrovascular expert - Not recommended')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'CEREBROVASCULAR EXPERT', cerebrovascular)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients - # ichemic reffered to another centre', cerebrovascular_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', cerebrovascular_color)

        #########################
        # DISCHARGE DESTINATION #
        #########################

        discharge_destination = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("discharge_destination")})

        destination_color = workbook1.add_format({
            'fg_color': colors.get("discharge_destination"),
            'text_wrap': True})

        first_index = column_names.index('# discharge destination - Home')
        last_index = column_names.index('% discharge destination - Dead')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DISCHARGE DESTINATION', discharge_destination)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # total patients - # ichemic reffered to another centre', destination_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', destination_color)

        ##################################################
        # DISCHARGE DESTINATION - WITHIN THE SAME CENTRE #
        ##################################################

        discharge_destination_same_centre = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("discharge_destination_same_centre")})

        discharge_destination_same_centre_color = workbook1.add_format({
            'fg_color': colors.get("discharge_destination_same_centre"),
            'text_wrap': True})

        first_index = column_names.index('# transferred within the same centre - Acute rehabilitation')
        last_index = column_names.index('% transferred within the same centre - Another department')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DISCHARGE DESTINATION WITHIN THE SAME CENTRE', discharge_destination_same_centre)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # transferred within the same centre', discharge_destination_same_centre_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', discharge_destination_same_centre_color)

        #########################################################
        # DISCHARGE DESTINATION - TRANSFERRED TO ANOTHER CENTRE #
        #########################################################

        discharge_destination_another_centre = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("discharge_destination_another_centre")})

        discharge_destination_another_centre_color = workbook1.add_format({
            'fg_color': colors.get("discharge_destination_another_centre"),
            'text_wrap': True})

        first_index = column_names.index('# transferred to another centre - Stroke centre')
        last_index = column_names.index('% transferred to another centre - Another hospital')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DISCHARGE DESTINATION TRANSFERRED TO ANOTHER CENTRE', discharge_destination_another_centre)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # transferred to another centre', discharge_destination_another_centre_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', discharge_destination_another_centre_color)

        ################################################################
        # DISCHARGE DESTINATION - TRANSFERRED TO WITHIN ANOTHER CENTRE #
        ################################################################

        discharge_destination_within_another_centre = workbook1.add_format({
            'bold': 2,
            'border': 0,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': colors.get("discharge_destination_within_another_centre")})

        discharge_destination_within_another_centre_color = workbook1.add_format({
            'fg_color': colors.get("discharge_destination_within_another_centre"),
            'text_wrap': True})

        first_index = column_names.index('# department transferred to within another centre - Acute rehabilitation')
        last_index = column_names.index('% department transferred to within another centre - Another department')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'DISCHARGE DESTINATION TRANSFERRED WITHIN TO ANOTHER CENTRE', discharge_destination_within_another_centre)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), 'out of # transferred to another centre', discharge_destination_within_another_centre_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', discharge_destination_within_another_centre_color)

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

        first_index = column_names.index(self.total_patients_column)
        last_index = column_names.index('% stroke patients treated in a dedicated stroke unit / ICU')
        first_cell = xl_rowcol_to_cell(0, first_index)
        last_cell = xl_rowcol_to_cell(0, last_index)

        worksheet.merge_range(first_cell + ":" + last_cell, 'ESO ANGELS AWARDS', awards)

        for i in range(first_index, last_index+1):
            if column_names[i].startswith('%'):
                worksheet.write(xl_rowcol_to_cell(1, i), '', awards_color)
            else:
                worksheet.write(xl_rowcol_to_cell(1, i), '', awards_color)

        hidden_columns = ['# patients treated with door to thrombolysis < 60 minutes', '# patients treated with door to thrombolysis < 45 minutes', '# patients treated with door to thrombectomy < 90 minutes', '# patients treated with door to thrombectomy < 60 minutes', '# recanalization rate out of total ischemic incidence', '# suspected stroke patients undergoing CT/MRI', '# all stroke patients undergoing dysphagia screening', '# ischemic stroke patients discharged with antiplatelets', '% ischemic stroke patients discharged with antiplatelets', '# ischemic stroke patients discharged home with antiplatelets', '% ischemic stroke patients discharged home with antiplatelets', '# ischemic stroke patients discharged (home) with antiplatelets', '# afib patients discharged with anticoagulants', '% afib patients discharged with anticoagulants', '# afib patients discharged home with anticoagulants', '% afib patients discharged home with anticoagulants', '# afib patients discharged (home) with anticoagulants', '# stroke patients treated in a dedicated stroke unit / ICU']
        				
        for i in hidden_columns:
            index = column_names.index(i)
            column = xl_col_to_name(index)
            worksheet.set_column(column + ":" + column, None, None, {'hidden': True})

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

        if not self.comp:    
            # if cell contain TRUE in column > 30 patients (DR) it will be colored to green
            row = 4
            index = column_names.index(self.total_patients_column)
            while row < nrow + 2:
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
            column = xl_col_to_name(index)
            angels_awards_recan(column, coln=index)

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
            column = xl_col_to_name(index)
            angels_awards_processes(column, coln=index)

            index = column_names.index('% all stroke patients undergoing dysphagia screening')
            column = xl_col_to_name(index)
            angels_awards_processes(column, coln=index)

            index = column_names.index('% ischemic stroke patients discharged (home) with antiplatelets')
            column = xl_col_to_name(index)
            angels_awards_processes(column, coln=index)

            index = column_names.index('% afib patients discharged (home) with anticoagulants')
            column = xl_col_to_name(index)
            angels_awards_processes(column, coln=index)

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
            column = xl_col_to_name(index)
            angels_awards_hosp(column, coln=index)

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
            
        else:
            pass

        workbook1.close()