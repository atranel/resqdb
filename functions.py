import os
import time 
from datetime import datetime, date
import pandas as pd

def save_file(name, data=None, index=False):
    """ If the file already exists, first try to rename it, it renaming is succes, rename it back and resaved the file else raise error and print warning to user, wait 2 seconds and try it again. 

    :params name: name of results file
    :type name: string
    :params data: dataframe to be saved
    :type data: dataframe
    :params index: iclude index in the file
    :type index: boolean
    """
    path = os.path.join(os.getcwd(), name)
    if os.path.exists(path):
        while True:
            time.sleep(10)
            closed = False
            try: 
                os.rename(path, f'{path}_')
                closed = True
                os.rename(f'{path}_',path)
            except IOError:
                print("Couldn't save file! Please, close the file {0}!".format(name))
            
            if closed:
                if data is not None:  
                    data.to_csv(path, sep=",", encoding='utf-8', index=index)
                break
    else:
        if data is not None:
            data.to_csv(path, sep=",", encoding='utf-8', index=index)


def repeat_answer():
    """ Return True if the user would like to continue but with diferrent setting otherwise return False.
    
    :returns: True if user entered 'y' else False is returned
    :rtype: boolean
    """
    while True:
        repeat = input("Would you like to continue with different setting? (y/n)\n").lower()
        if repeat != "y" and repeat != "n":
            print("Wrong option!")
            continue
        elif repeat == "y":
            repeat_calculation = True
            break
        else:
            repeat_calculation = False
            break

    return repeat_calculation

def get_year():
    """ Funtion used to get input from user and return the selected year. 
    
    :returns: the selected year
    :rtype: str
    """
    while True:
        val = input("Please, select year: \n1) 2016\n2) 2017\n3) 2018\n4) 2019\n5) 2020\n")
        if val != "1" and val != "2" and val != "3" and val != "4" and val != "5":
            print("Wrong option!")
            continue
        elif val == "1":
            year = "2016"
            break
        elif val == "2":
            year = "2017"
            break
        elif val == "3":
            year = "2018"
            break
        elif val == "4":
            year = "2019"
            break
        elif val == "5":
            year = "2020"
            break
    return year

def get_quarter(year):
    """ Get the starting and closing date for the selected period which is selected by user as input. 
    
    :param year: the year included in the dates
    :type year: str/int
    :returns: name of period, date, date
    """
    year = int(year)
    while True:
        val = input("Please, select quarter: \n1) Q1\n2) Q2\n3) Q3\n4) Q4\n")
        if val != "1" and val != "2" and val != "3" and val != "4":
            print("Wrong option!")
            continue
        elif val == "1":
            quarter = f"Q1_{year}"
            date1 = pd.Timestamp(date(year, 1, 1))
            date2 = pd.Timestamp(date(year, 3, 31))
            break
        elif val == "2":
            quarter = f"Q2_{year}"
            date1 = pd.Timestamp(date(year, 4, 1))
            date2 = pd.Timestamp(date(year, 6, 30))
            break
        elif val == "3":
            quarter = f"Q3_{year}"
            date1 = pd.Timestamp(date(year, 7, 1))
            date2 = pd.Timestamp(date(year, 9, 30))
            break
        elif val == "4":
            quarter = f"Q4_{year}"
            date1 = pd.Timestamp(date(year, 10, 1))
            date2 = pd.Timestamp(date(year, 12, 31))
            break
    return quarter, date1, date2

def get_half(year):
    """ Get the starting and closing date for the half of the year selected by the user in the input. 

    :param year: the year included in the dates
    :type year: str/int
    :returns: name of the selected period, the first date, the second date
    :rtype: str, date, date
    """
    year = int(year)
    while True:
        val = input("Please, select half: \n1) H1\n2) H2\n")
        if val != "1" and val != "2":
            print("Wrong option!")
            continue
        elif val == "1":
            half = f'H1_{year}'
            date1 = pd.Timestamp(date(year, 1, 1))
            date2 = pd.Timestamp(date(year, 6, 30))
            break
        elif val == "2":
            half = f'H2_{year}'
            date1 = pd.Timestamp(date(year, 7, 1))
            date2 = pd.Timestamp(date(year, 12, 31))
            break
    return half, date1, date2

def get_month_number(year):
    """ Function to get month from the user input. The month should be number from 1-12. 
    
    :returns: the number of month enterd by user
    :rtype: int
    """
    year = int(year)
    while True:
        val = input("Please, enter the number of month? (1-12)\n")
        closed = False
        try: 
            month = int(val)
            if (month <= 0 and month > 12):
                continue
            closed = True
        except ValueError:
            print("Invalid number!")
            continue
        
        if closed:
            import calendar
            week_day = calendar.monthrange(year, month) # get the tuple where the first number is weekday of first day of the month and second is number of days in month

            date1 = pd.Timestamp(date(year, month, 1))
            date2 = pd.Timestamp(date(year, month, week_day[1]))

            month_name = f'{date1.strftime("%B")}_{year}' # get the name of month
            break
    
    return month_name, date1, date2

def get_time_range():
    """ Return starting and closing date based on user preferences. Based on the selection other functions are called to obtain starting and closing date used for filtration. 
    
    :returns: name of the period, the first date, the end date and type of report
    :rtype: str, date, date, str
    """
    while True:
        report = input("Please, how you want to filter data? \n1) quarterly\n2) bi-annualy\n3) annualy\n4) monthly\n5) differently \n6) all data\n")
        if report != "1" and report != "2" and report != "3" and report != "4" and report != "5":
            print("Wrong option!")
            continue

        elif report == "1":
            report_type = "quarter"
            year = get_year()
            name, date1, date2 = get_quarter(year) 
            break

        elif report == "2":
            report_type = "half"
            year = get_year()
            name, date1, date2 = get_half(year)
            break

        elif report == "3":
            report_type = "year"
            name = get_year()
            date1 = pd.Timestamp(date(int(name), 1, 1))
            date2 = pd.Timestamp(date(int(name), 12, 31))
            break

        elif report == "4":
            report_type = "month"
            year = get_year() # get the year
            name, date1, date2 = get_month_number(year) # get number of month         
            break

        elif report == "5":
            
            while True:
                date1_str = input("Please, write first date in the format - Y-M-D: ")
                date1_l = date1_str.split("-")
                closed = False
                try:
                    date1 = pd.Timestamp(date(int(date1_l[0]), int(date1_l[1]), int(date1_l[2])))
                    #date1 = pd.Timestamp(int(date1_l[0]), int(date1_l[1]), int(date1_l[2]), 0)
                    closed = True
                except ValueError:
                    print("Invalid date or date out of range!")
                
                if closed:
                    break
            
            while True:
                date2_str = input("Please, write second date in the format - Y-M-D: ")
                date2_l = date2_str.split("-")
                closed = False
                try:
                    date2 = pd.Timestamp(date(int(date2_l[0]), int(date2_l[1]), int(date2_l[2])))
                    #date2 = pd.Timstamp(int(date2_l[0]), int(date2_l[1]), int(date2_l[2]), 0)
                    closed = True
                except ValueError:
                    print("Invalid date or date out of range!")

                if closed:
                    break

            name = date1_str + "_" + date2_str
            report_type = "range"
            break

        elif report == "6":
            name = "all"
            date1 = None
            date2 = None
            report_type = "all"
            break

    return name, date1, date2, report_type

def get_angel_awards():
    """ Return True if only angels awards data should be generated. 

    :returns: True if user enterd `y` otherwise returns False
    :rtype: boolean
    """
    while True:
        angel_awards = input("Do you need only angels awards (Excel)? (y/n)\n").lower()
        if angel_awards != "y" and angel_awards != "n":
            print("Wrong option!")
            continue
        elif angel_awards == "y":
            angels = True
            break
        else:
            angels = False
            break

    return angels

def get_total_patients():
    """ Return True if only sites with >= 30 patients should be included in calculation. 
    
    :returns: True if only sites with >= 30 patients in the period should be included
    :rtype: boolean
    """
    while True:
        total_patients = input("Do you want to include only sites with >= 30 patients? (y/n)\n").lower()
        if total_patients != "y" and total_patients != "n":
            print("Wrong option!")
            continue
        elif total_patients == "y":
            tpts = True
            break
        else:
            tpts = False
            break
    
    return tpts

def get_number_of_patients():
    """ Get minimum number of patients used as criterium for angels awards. 
    
    :returns: the number of patients used as a criterion for AA calculation
    :rtype: int
    """
    while True:        
        try:
            min_tpts = int(input("How many patients should be taken as as a criterion for the Total number of patients in Angels Awards? (default value: 30) \n") or "30")
            break
        except ValueError:
            print("Invalid number!")
            continue

    return min_tpts

def get_country_site(countries, site_ids):
    """ Get setting for the report generation. The country or site and if reports per site should be generated. 
    
    :param countries: list of countries
    :type countries: list
    :param site_ids: list of sites in data
    :type site_ids: list
    :returns: country for which report should be generated, site id if provided otherwise None and True if reports should be generated per sites otherwise False
    :rtype: str, str, boolean
    """
    
    while True:
        country_site = input(
            "Do you want generate statistics for one country/site? If yes, enter \'c\' for country / \'s\' for site, space and country code/site id, else \'n\'.\n")

        if country_site != "":
            tmp = country_site.split()[0].lower()
            if tmp != "n" and tmp != "c" and tmp != "s":
                print("Wrong option!")
                continue
            else:
                if tmp == "n":
                    country = None
                    site = None
                    break
                elif tmp == "c":
                    country = country_site.split()[1].upper()
                    if country not in countries:
                        print("Country not available! Enter new country code!")
                        continue
                    else:
                        site = None
                        break
                elif tmp == "s":
                    site = country_site.split()[1].upper()
                    if site not in site_ids:
                        print("Site ID not available! Enter new site ID!")
                        continue
                    else:
                        country = None
                        break
        else:
            continue

        

    split_sites = False
    if site is None:
        while True:
            split_site = input("Do you want to generate data for each site seperately? (y/n)?\n").lower()
            if split_site != "y" and split_site != "n":
                print("Wrong option")
                continue
            if split_site == "y":
                split_sites = True
                break
            else:
                split_sites = False
                break

    return country, site, split_sites

def get_zipfile(endings, path, quarter_name):
    """ Zip the files generated per site. The results zip file will include pptx, xlxs, and preprocessed data. 
    
    :params endings: the list of extension that should be included in the zip file
    :type endings: list
    :params path: the path to the files
    :type path: str
    :params quarter_name: the name of the quarter in the file name
    :type quarter_name: str
    """
    import zipfile
    # Go through files in given path
    for root, dirs, files in os.walk(path):
        # For each file find "xlsx" file and then all files with other endings and make zip file
        for file in files:
            filename, extension = os.path.splitext(file)
            if (filename.endswith(quarter_name) and (extension == ".xlsx")):
                zipfile_name = os.path.join(root, filename) + ".zip"
                zipf = zipfile.ZipFile(zipfile_name, 'w', zipfile.ZIP_DEFLATED)
                for i in endings:
                    name = filename + i
                    path = os.path.dirname(os.path.join(root, name))
                    zipf.write(os.path.join(path, name), os.path.basename(os.path.join(path, name)))
                else:
                    continue
                zipf.close()

def get_country(countries):
    """ Get country code as input from user and check if country code in the list of countries obtained from the preprocessed data. Return country code in uppercase. 
    
    :param countries: the list of country code 
    :type countries: list   
    :returns: the entered country code
    :rtype: str
    """
    while True:
        country_input = input("Select country code which you want to compare yearly?\n").lower()
        if country_input != "n" and country_input.upper() not in countries:
            print("Wrong option!")
            continue
        else:
            if country_input == "n":
                country = None
                break
            elif country_input.upper() in countries:
                country = country_input.upper()
                break

    return country

def period_comparison(countries_list, nationally_samples):
    ''' 
    Period comparison between nationally samples x site-level samples eg. in 2019. Nationally samples are set by default but they can be changed. 
    
    :param countries_list: list of countries from the preprocessed data
    :type countries_list: list
    :param nationally_samples: list of country codes used as nationally samples, the rest from countries_lsit will be used as site-level
    :type nationally_samples: list
    :returns: True if countries should be compared otherwise false, the list of site samples and the list of nationally samples
    :rtype: bool, list, list
    '''
    tmp = countries_list # Get countries list
    while True:
        comparison = input(
            "Do you want to compare countries in one period (e.g. nationally x site-level in 2018)? (y/n) \n").lower() # Get if the user want to compare nationally samples x site-level samples in one period. 
        if comparison != "y" and comparison != "n": # Wrong option
            print("Wrong option!")
            continue
        elif comparison == "y": # Correct option
            print('Samples selected as national by default: {0}!'.format(nationally_samples)) # Show the default nationally samples
            ns_input = input('If you want to change nationally samples used in comparison, please enter country codes seperated by comma else enter "n"!\n').lower() # Change nationally samples
            ns_list = ns_input.split(",") 
            if len(ns_list) == 1 and ns_list[0] != "n":
                print("wrong option!")
                continue
            elif len(ns_list) > 1:
                ns_list = [x.upper() for x in ns_list]
                diff = set(ns_list).difference(set(countries_list))
                if len(diff) > 0:
                    print("Wrong country code/s or counry code not in the raw data: {0}! Please fix the country code.".format(list(diff)))
                    continue
                else:
                    nationally_samples = ns_list
                    print("Selected nationally samples: {0}".format(nationally_samples))
                    site_samples = [x for x in tmp if x not in nationally_samples]
                    countries_comp = True
                    break
            else:
                site_samples = [x for x in tmp if x not in nationally_samples]
                countries_comp = True
                break
        else:
            countries_comp = False
            nationally_samples = []
            site_samples = tmp
            break

    return countries_comp, site_samples, nationally_samples

def nationally_samples_comparison(nationally_samples):
    """ Function to get nationally samples to be compared in two periods. 
    
    :param nationally_samples: list of country codes to be used in comparison
    :type nationally_samples: list
    :returns: True if nationally sites should be compared in two periods else False, the list of nationally samples
    :rtype: bool, list
    """
    while True:
        # Get user's input
        comparison = input("Do you want to compare nationally sites in two periods (e.g. 2017 x 2018)? (y/n) \n").lower()
        if comparison != "y" and comparison != "n":
            print("Wrong option!")
            continue
        elif comparison == "y":
            nationally_comparison = True
            print('Samples selected as national by default: {0}!'.format(nationally_samples))
            ns_input = input('If you want to change nationally samples used in comparison, please enter country codes seperated by comma else enter "n"!\n').lower()
            ns_list = ns_input.split(",")
            if len(ns_list) == 1 and ns_list[0] != "n":
                print("wrong option!")
                continue
            elif len(ns_list) > 1:
                ns_list = [x.upper() for x in ns_list]
                diff = set(ns_list).difference(set(countries_list))
                if len(isec) > 0:
                    print("Wrong country code/s or counry code not in the raw data: {0}! Please fix the country code.".format(list(isec)))
                    continue
                else:
                    nationally_samples = ns_list
                    break       
            else:
                break
        else: 
            nationally_comparison = False
            break

    return nationally_comparison, nationally_samples


def year_comparison(countries):
    """ Function to get if user would like to generate comparison reports per years for country. 
    
    :param countries: list of country codes that are available
    :type countries: list
    :returns: True if country should be compared through all years else False, country code if countries should be compared otherwise empty string returns
    :rtype: bool, str
    """
    while True:
        comparison = input("Do you want to compare country per all years (e.g. all years in data) or in different period? (y/n)\n").lower()
        if comparison != "y" and comparison != "n":
            print("Wrong option!")
            continue
        elif comparison == "y":
            years_comp = True
            country = get_country(countries)
            break
        else:
            years_comp = False
            country = ""
            break
    return years_comp, country


def filter_by_pts(df):
    """ Function to filter provided dataframe by total patients. If site doesn't have total patients > 30, it is excluded from the dataframe. 
    
    :param df: preprocessed data to be filtered by Total patients
    :type df: DataFrame
    :returns: the dataframe with sites that do not have enough patients in period excluded
    :rtype: DataFrame
    """
    # group dataframe by Site ID
    patients_df = df.groupby(['Protocol ID']).size().reset_index(name="Total Patients")
    # get site IDs where Total patients > 30 and get list of sites
    patients_df = patients_df[patients_df['Total Patients'] >= 30]
    protocol_id = set(patients_df['Protocol ID'].tolist())
    res = df[df['Protocol ID'].isin(protocol_id)] 
    return res

def get_month():
    """ Function to get month from the user input. The month should be number from 1-12. 
    
    :returns: the number of month enterd by user
    :rtype: int
    """
    while True:
        val = input("Please, select which month should be included in reports as last? (1-12)\n")
        closed = False
        try: 
            month = int(val)
            if (month <= 0 and month > 12):
                continue
            closed = True
        except ValueError:
            print("Invalid number!")
        
        if closed:
            break
    
    return month

def get_values_for_factors(tmp, default, column_name, value, new_column_name, column):
    """ Return number of patients for selected value.

    :param tmp: temporary dataframe with all possible options and number of patients for that option grouped by site
    :type tmp: DataFrame
    :param default: dataframe grouped by stroke type
    :type default: DataFrame
    :param column_name: name of the column for which we want to calculate number of patients
    :type column_name: str
    :param value: option for which you would like to get numbers
    :type value: str/int
    :param new_column_name: the new name of the resulted column
    :type new_column_name: str
    :param column: column included in the results
    :type column: str
    :returns: new dataframe with number of patients
    :rtype: DataFrame
    """
    import numpy as np
    if (tmp[column_name].dtype != np.float64):
        value = str(value)
    else:
        value = value 
    
    tmpDf = tmp[tmp[column_name] == value].reset_index()[[column, 'count']]
    factorDf = default.merge(tmpDf, how='outer')
    factorDf.rename(columns={'count': new_column_name}, inplace=True)
    factorDf.fillna(0, inplace=True)

    return factorDf

def mrs_function(x):
    """ Return mapped mRS value. 
    
    :param x: the index of answer from the form, eg. first option of select has index 1 etc. 
    :type x: int
    :returns: the converted mRS score
    :rtype: int
    """
    x = float(x)
    if (x == 1):
        x = x - 1
    else: 
        x = x - 2 
    return x


def calculate_outcome(df):
    """ Calculate ouctome per group of patients for the latest year! 
    
    :param df: the dataframe with preprocessed and filtered data
    :type df: DataFrame
    :returns: dataframe with calculated outcome
    :rtype: DataFrame
    """
    import numpy as np
    stroke_df = df[df['STROKE_TYPE'].isin([1,2,4])].copy() # Filter dataframe by stroke type - IS, ICH and SAH
    stroke_df.fillna(0, inplace=True) # Replace NA value by 0
    outcome_stroke_df = stroke_df.groupby(['STROKE_TYPE']).size().reset_index(name="n") # Calculate total patients per stroke type
    tmp = stroke_df.groupby(['STROKE_TYPE', 'DISCHARGE_DESTINATION']).size().to_frame('count').reset_index() # Create temporary dataframe grouped by stroke type and discharge destination
    outcome_stroke_df = get_values_for_factors(tmp=tmp, default=outcome_stroke_df, column_name="DISCHARGE_DESTINATION", value=1, new_column_name='# home', column='STROKE_TYPE') # Caculated # of patients discharged home
    outcome_stroke_df['% home'] = outcome_stroke_df.apply(lambda x: round(((x['# home']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patients discharged home
    outcome_stroke_df = get_values_for_factors(tmp=tmp, default=outcome_stroke_df, column_name="DISCHARGE_DESTINATION", value=2, new_column_name='# transferred within the same centre', column='STROKE_TYPE') # Calculate # of patients transferred within the same center
    outcome_stroke_df['% transferred within the same centre'] = outcome_stroke_df.apply(lambda x: round(((x['# transferred within the same centre']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patients transferred within the same center
    outcome_stroke_df = get_values_for_factors(tmp=tmp, default=outcome_stroke_df, column_name="DISCHARGE_DESTINATION", value=3, new_column_name='# transferred to another centre', column='STROKE_TYPE') # Calculate # of patents transferred to another center
    outcome_stroke_df['% transferred to another centre'] = outcome_stroke_df.apply(lambda x: round(((x['# transferred to another centre']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patents transferred to another center
    outcome_stroke_df = get_values_for_factors(tmp=tmp, default=outcome_stroke_df, column_name="DISCHARGE_DESTINATION", value=4, new_column_name='# social care facility', column='STROKE_TYPE') # Calculate # of patients discharge to social care facility
    outcome_stroke_df['% social care facility'] = outcome_stroke_df.apply(lambda x: round(((x['# social care facility']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patients discharge to social care facility
    outcome_stroke_df = get_values_for_factors(tmp=tmp, default=outcome_stroke_df, column_name="DISCHARGE_DESTINATION", value=5, new_column_name='# dead', column='STROKE_TYPE') # Calculate # of dead patients
    outcome_stroke_df['% dead'] = outcome_stroke_df.apply(lambda x: round(((x['# dead']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of dead patients
    mrs_subset = stroke_df[~stroke_df['DISCHARGE_MRS'].isin([0])].copy() # Get subset of patients who don't have Discharge MRS = 0
    mrs_subset.fillna(0, inplace=True) # Replace NA by 0
    if mrs_subset.empty: # If mrs_subset is empty, set Median discharge mRS to 0
        outcome_stroke_df['Median discharge mRS'] = 0
    else:
        mrs_subset['DISCHARGE_MRS_ADJUSTED'] = mrs_subset.apply(lambda row: mrs_function(row['DISCHARGE_MRS']), axis=1) # Calculate score for Discharge MRS (from the dropdown) -> 1 - unknown/calculate, 2 - 0, 3 - 1, 4 - 2 etc. 
        mrs_subset['DISCHARGE_MRS_ADDED'] = mrs_subset['DISCHARGE_MRS_ADJUSTED'] + mrs_subset['D_MRS_SCORE'] # Merge calculated MRS column with adjusted MRS column
        mrs_subset.fillna(0, inplace=True)
        outcome_stroke_df = outcome_stroke_df.merge(mrs_subset.groupby(['STROKE_TYPE']).DISCHARGE_MRS_ADDED.agg(['median']).rename(columns={'median': 'Median discharge mRS'})['Median discharge mRS'].reset_index(), how='outer') # calculate median value from DISCHARGE_MRS_ADDED column
        outcome_stroke_df['Median discharge mRS'] = outcome_stroke_df['Median discharge mRS'].round() 
    outcome_stroke_df['STROKE_TYPE'] = outcome_stroke_df['STROKE_TYPE'].replace({1: "iCMP", 2: "ICH", 4: "SAK"}) # Replace number defining stroke types by stroke shortcut

    recan_df = df[df['STROKE_TYPE'].isin([1]) & df['RECANALIZATION_PROCEDURES'].isin([2,3,4])].copy() # Get patients who have undergone recanalization procedure
    recan_df.fillna(0, inplace=True) # replace NA values by 0
    outcome_recan_df = recan_df.groupby(['RECANALIZATION_PROCEDURES']).size().reset_index(name="n") # Total patients per type of recanalization
    tmp = recan_df.groupby(['RECANALIZATION_PROCEDURES', 'DISCHARGE_DESTINATION']).size().to_frame('count').reset_index() # Create temporary dataframe grouped by recanalization procedure and by discharge destination
    outcome_recan_df = get_values_for_factors(tmp=tmp, default=outcome_recan_df, column_name="DISCHARGE_DESTINATION", value=1, new_column_name='# home', column='RECANALIZATION_PROCEDURES') # Caculated # of patients discharged home 
    outcome_recan_df['% home'] = outcome_recan_df.apply(lambda x: round(((x['# home']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Caculated % of patients discharged home
    outcome_recan_df = get_values_for_factors(tmp=tmp, default=outcome_recan_df, column_name="DISCHARGE_DESTINATION", value=2, new_column_name='# transferred within the same centre', column='RECANALIZATION_PROCEDURES') # Calculate # of patients transferred within the same center
    outcome_recan_df['% transferred within the same centre'] = outcome_recan_df.apply(lambda x: round(((x['# transferred within the same centre']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patients transferred within the same center
    outcome_recan_df = get_values_for_factors(tmp=tmp, default=outcome_recan_df, column_name="DISCHARGE_DESTINATION", value=3, new_column_name='# transferred to another centre', column='RECANALIZATION_PROCEDURES') # Calculate # of patents transferred to another center
    outcome_recan_df['% transferred to another centre'] = outcome_recan_df.apply(lambda x: round(((x['# transferred to another centre']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patents transferred to another center
    outcome_recan_df = get_values_for_factors(tmp=tmp, default=outcome_recan_df, column_name="DISCHARGE_DESTINATION", value=4, new_column_name='# social care facility', column='RECANALIZATION_PROCEDURES') # Calculate # of patients discharge to social care facility
    outcome_recan_df['% social care facility'] = outcome_recan_df.apply(lambda x: round(((x['# social care facility']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of patients discharge to social care facility
    outcome_recan_df = get_values_for_factors(tmp=tmp, default=outcome_recan_df, column_name="DISCHARGE_DESTINATION", value=5, new_column_name='# dead', column='RECANALIZATION_PROCEDURES') # Calculate # of dead patients
    outcome_recan_df['% dead'] = outcome_recan_df.apply(lambda x: round(((x['# dead']/x['n']) * 100), 2) if x['n'] > 0 else 0, axis=1) # Calculate % of dead patients
    mrs_subset = recan_df[~recan_df['DISCHARGE_MRS'].isin([0])].copy(0) # Filter out DISCHARGE_MRS = 0
    mrs_subset.fillna(0, inplace=True)  # Replace NA by 0
    if mrs_subset.empty:  # If mrs_subset is empty, set Median discharge mRS to 0
        outcome_recan_df['Median discharge mRS'] = 0
    else:
        mrs_subset['DISCHARGE_MRS_ADJUSTED'] = mrs_subset.apply(lambda row: mrs_function(row['DISCHARGE_MRS']), axis=1) # Calculate score for Discharge MRS (from the dropdown) -> 1 - unknown/calculate, 2 - 0, 3 - 1, 4 - 2 etc. 
        mrs_subset['DISCHARGE_MRS_ADDED'] = mrs_subset['DISCHARGE_MRS_ADJUSTED'] + mrs_subset['D_MRS_SCORE'] # Merge calculated MRS column with adjusted MRS column
        mrs_subset.fillna(0, inplace=True)
        outcome_recan_df = outcome_recan_df.merge(mrs_subset.groupby(['RECANALIZATION_PROCEDURES']).DISCHARGE_MRS_ADDED.agg(['median']).rename(columns={'median': 'Median discharge mRS'})['Median discharge mRS'].reset_index(), how='outer') # calculate median value  from DISCHARGE_MRS_ADDED column
        outcome_recan_df['Median discharge mRS'] = outcome_recan_df['Median discharge mRS'].round() # Replace number defining stroke types by stroke shortcut
    
    outcome_recan_df['RECANALIZATION_PROCEDURES'] = outcome_recan_df['RECANALIZATION_PROCEDURES'].replace({2: "IV tPA", 3: "IV tPA + TBY", 4: "TBY"}) # Replace number defining recanalization procedure by recanalization type

    outcome_df = outcome_recan_df.append(outcome_stroke_df, ignore_index=True) # Merge these two temproary dataframes
    
    outcome_df['Patient Group'] = outcome_df['STROKE_TYPE'].fillna('') + outcome_df['RECANALIZATION_PROCEDURES'].fillna('') # Create Patient Group column
    #outcome_df['Type'] = outcome_df[['STROKE_TYPE', 'RECANALIZATION_PROCEDURES']].apply(lambda x: ''.join(str(x)), axis=1)
    cols = ['Patient Group', 'n', 'Median discharge mRS', '# home', '% home', '# transferred within the same centre', '% transferred within the same centre', '# transferred to another centre', '% transferred to another centre', '# social care facility', '% social care facility', '# dead', '% dead'] # Filter columns
    outcome_df = outcome_df[cols]
    outcome_df.to_csv("outcome.csv", sep=",", index=False) # Save to csv

    return outcome_df

