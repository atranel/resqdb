import os
import time 
from datetime import datetime, date

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
    """ Return True if the user would like to continue but with diferrent setting otherwise return False."""
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
    
    :returns: string
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
    year_str = str(year)
    while True:
        val = input("Please, select quarter: \n1) Q1\n2) Q2\n3) Q3\n4) Q4\n")
        if val != "1" and val != "2" and val != "3" and val != "4":
            print("Wrong option!")
            continue
        elif val == "1":
            quarter = "Q1_" + year_str
            date1 = date(year, 1, 1)
            date2 = date(year, 3, 31)
            break
        elif val == "2":
            quarter = "Q2_" + year_str
            date1 = date(year, 4, 1)
            date2 = date(year, 6, 30)
            break
        elif val == "3":
            quarter = "Q3_" + year_str
            date1 = date(year, 7, 1)
            date2 = date(year, 9, 30)
            break
        elif val == "4":
            quarter = "Q4_" + year_str
            date1 = date(year, 10, 1)
            date2 = date(year, 12, 31)
            break
    return quarter, date1, date2

def get_half(year):
    """ Get the starting and closing date for the half of the year selected by the user in the input. 

    :param year: the year included in the dates
    :type year: str/int
    :returns: name of period, date, date
    """
    year = int(year)
    year_str = str(year)
    while True:
        val = input("Please, select half: \n1) H1\n2) H2\n")
        if val != "1" and val != "2":
            print("Wrong option!")
            continue
        elif val == "1":
            half = "H1_" + year_str
            date1 = date(year, 1, 1)
            date2 = date(year, 6, 30)
            break
        elif val == "2":
            half = "H2_" + year_str
            date1 = date(year, 7, 1)
            date2 = date(year, 12, 31)
            break
    return half, date1, date2
        
def get_time_range():
    """ Return starting and closing date based on user preferences. Based on the selection other functions are called to obtain starting and closing date used for filtration. 
    
    :returns: name of period, starting date, closing date, type of report
    """
    while True:
        report = input("Please, how you want to filter data? \n1) quarterly\n2) bi-annualy\n3) annualy\n4) differently\n5) all data\n")
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
            date1 = date(int(name), 1, 1)
            date2 = date(int(name), 12, 31)
            break

        elif report == "4":
            
            while True:
                date1_str = input("Please, write first date in the format - Y-M-D: ")
                date1_l = date1_str.split("-")
                closed = False
                try:
                    date1 = date(int(date1_l[0]), int(date1_l[1]), int(date1_l[2]))
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
                    date2 = date(int(date2_l[0]), int(date2_l[1]), int(date2_l[2]))
                    #date2 = pd.Timstamp(int(date2_l[0]), int(date2_l[1]), int(date2_l[2]), 0)
                    closed = True
                except ValueError:
                    print("Invalid date or date out of range!")

                if closed:
                    break

            name = date1_str + "_" + date2_str
            report_type = "range"
            break

        elif report == "5":
            name = "all"
            date1 = None
            date2 = None
            report_type = "all"
            break

    return name, date1, date2, report_type

def get_angel_awards():
    """ Return True if only angels awards data should be generated. 

    :returns: boolean
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
    
    :returns: int
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
    
    :returns: int
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
    :returns: country, site, split_sites
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

def get_country_site(countries):
    """ Get country code as input from user and check if country code in the list of countries obtained from the preprocessed data. Return country code in uppercase. 
    
    :param countries: the list of country code 
    :type countries: list   
    :returns: country code
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
    :returns: bool, list
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
    :returns: bool, string
    """
    while True:
        comparison = input("Do you want to compare country per all years (e.g. all years in data) or in different period? (y/n)\n").lower()
        if comparison != "y" and comparison != "n":
            print("Wrong option!")
            continue
        elif comparison == "y":
            years_comp = True
            country = get_country_site(countries)
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
    :returns: df
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
    
    :returns: month
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

