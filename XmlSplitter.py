import xml.etree.ElementTree as ET
import sys, os
import logging
import pandas as pd
import time
from multiprocessing import Process
from threading import Thread
import itertools

class XmlSplitter:
    """ Call this module to convert XML file with data from forms in the combined csv file. """

    def __init__(self, xml_file, nprocess = 1):

        self.xml_file = xml_file

        # Create log file in the working folder
        log_file = os.path.join(os.getcwd(), 'debug.log')
        logging.basicConfig(filename=log_file,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        logging.info('XMLSplitter')  

        self.skiped_study_oids = ['S_RESQ', 'S_DEMO_SIT', 'S_UA_DEMO', 'S_UA_DEMO_5834', 'S_CZ_DEMO', 'S_QASC_DEM']

        self.root, self.xmlns, self.openclinica, self.date = self.get_root(self.xml_file)

        self.studies = self.get_studies(self.root, self.xmlns)
        study_ids = self.get_study_oids(self.root, self.xmlns)

        self.items, self.column_names = self.get_items(self.root, self.xmlns, self.openclinica)

        sort_pts, total_patients = self.get_number_of_patient_per_site(self.root, self.xmlns, study_ids)

        # Create number of lists with sites based on number of processes
        if nprocess != 1:
            lstudies = self.split_list(sort_pts, total_patients, parts=nprocess)
            names = list(lstudies.keys())
        else:
            lstudies = study_ids
              
        
        # Create empty dataframe with column names
        if nprocess > 0:
            self.df1 = pd.DataFrame(columns=self.column_names)
        if nprocess > 1:
            self.df2 = pd.DataFrame(columns=self.column_names)
        if nprocess > 2:
            self.df3 = pd.DataFrame(columns=self.column_names)
        if nprocess > 3:
            self.df4 = pd.DataFrame(columns=self.column_names)
        if nprocess > 4:
            self.df5 = pd.DataFrame(columns=self.column_names)
        if nprocess > 5:
            self.df6 = pd.DataFrame(columns=self.column_names)
        if nprocess > 6:
            self.df7 = pd.DataFrame(columns=self.column_names)
        if nprocess > 7:
            self.df8 = pd.DataFrame(columns=self.column_names)
        if nprocess > 8:
            self.df9 = pd.DataFrame(columns=self.column_names)
        if nprocess > 9:
            self.df10 = pd.DataFrame(columns=self.column_names)

        
        # Run subprocesses
        threads = []
        if nprocess == 1:
            self.convert_xml_to_df(self.root, self.xmlns, self.openclinica, study_ids, 'Process') 
        else:
            for i in range(0, nprocess):
                process = Thread(target=self.convert_xml_to_df, args=(self.root, self.xmlns, self.openclinica, lstudies[names[i]], i))
                process.start()
                threads.append(process)
            
            for process in threads:
                process.join()
            
        
        logging.info("All threads completed.")
        print("All thread completed")
        
        # Create one dataframe by appending all dataframes to one
        if nprocess == 1:
            self.df = self.df1
        if nprocess > 1:
            self.df = self.df1.append(self.df2, sort=False)
        if nprocess > 2:
            self.df = self.df.append(self.df3, sort=False)
        if nprocess > 3:
            self.df = self.df.append(self.df4, sort=False)
        if nprocess > 4:
            self.df = self.df.append(self.df5, sort=False)
        if nprocess > 5:
            self.df = self.df.append(self.df6, sort=False)
        if nprocess > 6:
            self.df = self.df.append(self.df7, sort=False)
        if nprocess > 7:
            self.df = self.df.append(self.df8, sort=False)
        if nprocess > 8:
            self.df = self.df.append(self.df9, sort=False)
        if nprocess > 9:
            self.df = self.df.append(self.df10, sort=False)
        self.df.reset_index(inplace=True)
        
    def get_root(self, xml_file):
        """ Get root of xml file and additional necessary values (schema and openclinica link). """
        file = open(xml_file, 'r')
        tree = ET.parse(file)

        root = tree.getroot()
        date = root.get('CreationDateTime')
        xmlns = "{http://www.cdisc.org/ns/odm/v1.3}"
        openclinica = "{http://www.openclinica.org/ns/odm_ext_v130/v3.1}"

        logging.info('Date: {0}, xmlns: {1}, openclinica: {2}'.format(date, xmlns, openclinica))

        return root, xmlns, openclinica, date

    def trim_name(self, val):
        """ Remove RES-Q from text if present. 
        
        Params: 
            val: string 
        Returns: 
            Shorten string if RES-Q was in the value.
        """
        if val.startswith("RES-Q"):
            return val[8:]
        else:
            return val

    def trim_var_name(self, var):
        """ Cut language variation, eg. EN, CS from the name of variable. """
        if var.endswith('RU_2') or var.endswith('CS_2') or var.endswith('HY_2') or var.endswith('RU_2') or var.endswith('CZ_2'):
            return var[:-5]
        elif var.endswith("CZ") or var.endswith("EN") or var.endswith("HY") or var.endswith("ES") or var.endswith("CS") or var.endswith("RO") or var.endswith("UA") or var.endswith("RU"):
            return var[:-3]
        else:
            return var

    def get_studies(self, root, xmlns):
        """ Get all studies in xml file, to each study also assign study name and protocol ID. 

        Params:
            root: The root of xml file.
            xmlns: The base schema.
        Returns:
            New dictionary with all studies as keyword. 
        """
        studies = {}
        for study in root.findall(xmlns + 'Study'):
            study_item = {}
            for global_variables in study.iter(xmlns + 'GlobalVariables'):
                for study_name in global_variables.iter(xmlns + 'StudyName'):
                    study_item['study_name'] = self.trim_name(study_name.text)
                for protocol_name in global_variables.iter(xmlns + 'ProtocolName'):
                    study_item['protocol_name'] = self.trim_name(protocol_name.text)
            studies[study.get('OID')] = study_item
        
        logging.info('Study names and Protocol IDs were shorten.')
        return studies

    def get_number_of_patient_per_site(self, root, xmlns, study_ids):
        """ Get list of sites sorted by number of sites. """
        pts = {}
        total_patients = 0
        for clinical_data in root.findall(xmlns + 'ClinicalData'):
            if clinical_data.get('StudyOID') in study_ids:
                total_patients += len(clinical_data.findall(xmlns + 'SubjectData'))
                pts[clinical_data.get('StudyOID')] = len(clinical_data.findall(xmlns + 'SubjectData'))
        
        sort_pts = sorted(pts.items(), key=lambda x: x[1], reverse=True)
        return sort_pts, total_patients
        
    def get_study_oids(self, root, xmlns):
        """ Get all Study OIDs present in data. 

        Params: 
            root: The root of xml file.
            xmlns: The base schema.
        Returns: 
            List of available IDs.
        """
        study_oids = []
        for clinical_data in root.findall(xmlns + 'ClinicalData'):
            study_oid = clinical_data.get('StudyOID')
            if study_oid not in self.skiped_study_oids:
                study_oids.append(study_oid)

        return study_oids
    
    def split_list(self, data, total_patients, parts=2):
        """ Return number of lists equaled to processes and site ids in the list. Each process will get some sites. """
        pts_per_parts = []
        for i in range(1, parts + 1):
            pts_per_parts.append(total_patients//parts*i)
        
        count = 0
        index = 0
        p_ix = 0
        res = {}

        site_ids = list(list(zip(*data))[0])
        total_patients = list(list(zip(*data))[1])

        for i in range(0, len(total_patients)):
            count += total_patients[i]
            if count > pts_per_parts[index]:
                name = "list" + str(index)
                index += 1
                res[name] = site_ids[p_ix:i+1]
                p_ix = i+1
            if i == (len(total_patients) - 1):
                name = "list" + str(index)
                res[name] = site_ids[p_ix:]
            else:
                continue
        
        return res

    def get_items(self, root, xmlns, openclinica):
        """ Get all columns names in the xml file, to each column name get shorten name, version of form and comment. 
        
        Params:
            root: The root of xml file.
            xmlns: The base schema.
            openclinica: The base schema for openclinica.
        Returns:
            New dictionary with all columns as keyword.     
        """
        items = {}
        column_names = ['Subject ID', 'Protocol ID', 'Site Name', 'StartDate', 'EndDate', 'FormOID']
        for study in root.findall(xmlns + 'Study'):
            if study.get('OID') == "S_RESQ":
                for metadata in study.iter(xmlns + 'MetaDataVersion'):
                    for item_def in metadata.iter(xmlns + 'ItemDef'):
                        item = {}
                        item['name'] = item_def.get('Name')
                        shorten_name = self.trim_var_name(item_def.get('Name'))
                        item['shorten_name'] = shorten_name
                       # form_oids = item_def.get(openclinica + 'FormOID')
                       # print(form_oids)
                        #item['form_oid'] = item_def.get(openclinica + 'FormOIDs')
                        #if shorten_name not in column_names and "RESQV12" not in form_oids and "ATALAIA" not in form_oids:
                          #  column_names.append(shorten_name)
                        item['comment'] = item_def.get('Comment')
                        items[item_def.get('OID')] = item
        return items, column_names

    
    def refactor_values(self, row):

        res = {}
        res['RECURRENT_STROKE'] = '-999'
        res['DEPARTMENT_TYPE'] = '-999'
        res['VENTILATOR'] = '-999'
        res['BLEEDING_REASON'] = '-999'
        res['BLEEDING_SOURCE'] = '-999'
        res['INTERVENTION'] = '-999'
        res['CEREBROVASCULAR_EXPERT'] = '-999'
        res['DISCHARGE_OTHER_FACILITY_O1'] = '-999'
        res['DISCHARGE_OTHER_FACILITY_O2'] = '-999'
        
        for key, value in row.items():
            if key == 'Site Name':
                res['Site Name'] = value
            elif key == 'Subject ID':
                res['Subject ID'] = value
            elif key == 'EndDate':
                res['EndDate'] = value
            elif key == 'StartDate':
                res['StartDate'] = value
            elif key == 'Protocol ID':
                res['Protocol ID'] = value
            elif key == 'FormOID':
                res['FormOID'] = value
            elif key == "SEX":
                res['SEX'] = '2' if value == '1' else '1'
            elif key == 'VISDAT':
                res['VISIT_DATE'] = value
            elif key == 'VISTIM':
                res['VISIT_TIME'] = value
            elif key == 'HOSDAT':
                res['HOSPITAL_DATE'] = value
            elif key == 'HOSTIM':
                res['HOSPITAL_TIME'] = value
            elif key == 'HOSSTRK':
                res['HOSPITAL_STROKE'] = value
            elif key == 'HOSP':
                res['HOSPITALIZED_IN'] = '3' if value == '2' else value
            elif key == 'PTASS':
                res['ASSESSED_FOR_REHAB'] = value
            elif key == 'STRKTYP':
                if value == '1':
                    value = '4'
                elif value == '2':
                    value = '1'
                elif value == '3':
                    value = '2'
                    res['NEUROSURGERY'] = '3'
                elif value == '4':
                    value = '6'
                else:
                    value = value
                res['STROKE_TYPE'] = value
            elif key == 'CONSLVL':
                res['CONSCIOUSNESS_LEVEL'] = '5' if value == '4' else value
            elif key == 'NIHSSSCR':
                res['NIHSS_SCORE'] = value
            elif key == 'CT':
                res['CT_MRI'] = value
            elif key == 'CTTIM':
                res['CT_TIME'] = value
            elif key == 'RECANPROC':
                res['RECANALIZATION_PROCEDURES'] = value
            elif key == 'IVTPA1':
                res['IVT_ONLY'] = value
            elif key == 'NDLTIM1':
                res['IVT_ONLY_NEEDLE_TIME'] = value
            elif key == 'ADM1':
                res['IVT_ONLY_ADMISSION_TIME'] = value
            elif key == 'BOLUS1':
                res['IVT_ONLY_BOLUS_TIME'] = value
            elif key == 'IVTPA2':
                res['IVT_TBY'] = value
            elif key == 'NDLTIM2':
                res['IVT_TBY_NEEDLE_TIME'] = value
            elif key == 'GROIN2':
                res['IVT_TBY_GROIN_TIME'] = value
            elif key == 'ADM2':
                res['IVT_TBY_ADMISSION_TIME'] = value
            elif key == 'BOLUS2':
                res['IVT_TBY_BOLUS_TIME'] = value
            elif key == 'GROINTIM2':
                res['IVT_TBY_GROIN_PUNCTURE_TIME'] = value
            elif key == 'GROIN3':
                res['TBY_ONLY_GROIN_PUNCTURE_TIME'] = value
            elif key == 'DYPSH':
                if value == '1':
                    value = '4'
                elif value == '3':
                    value = '6'
                else:
                    value = value
                res['DYSPHAGIA_SCREENING'] = value
            elif key == 'DYSPHTIM':
                res['DYSPHAGIA_SCREENING_TIME'] = value
            elif key == 'FIBRTIM':
                if value == '1' or value == '2':
                    value = '3'
                res['AFIB_DETECTION_METHOD'] = value
            elif key == 'FIBR':
                if value == '3':
                    value = '5'
                res['AFIB_FLUTTER'] = value
            elif key == 'AFIB':
                if value == '1':
                    value = '3'
                elif value == '2':
                    value = '4'
                elif value == '3':
                    value = '5'
                else:
                    value = value
                res['AFIB_FLUTTER'] = value
            elif key == 'CRTD':
                res['CAROTID_ARTERIES_IMAGING'] = '2' if value == '1' else '1'
            elif key == 'HMCRN':
                if value == '3':
                    value = '2'
                else: 
                    value = value
                res['HEMICRANIECTOMY'] = value
            elif key == 'THRMBTST':
                if value == '6':
                    value = '9'
                elif value == '7':
                    value = '10'
                else:
                    value = value
                res['ANTITHROMBOTICS'] = value
            elif key == 'STTN':
                res['STATIN'] = value
            elif key == 'STNSS':
                res['CAROTID_STENOSIS'] = '2' if value == '1' else '3'
            elif key == 'STNSSTIM': 
                res['CAROTID_STENOSIS_FOLLOWUP'] = value
            elif key == 'TNSV':
                res['ANTIHYPERTENSIVE'] = value
            elif key == 'SMKR' or key == 'SMKR_2':
                res['SMOKING_CESSATION'] = value
            elif key == 'DEST':
                if value == '3':
                    value = '4'
                elif value == '4':
                    value = '3'
                else:
                    value = value
                res['DISCHARGE_DESTINATION'] = value
                res['DISCHARGE_SAME_FACILITY'] = '1' if value == '2' else '-999'
                res['DISCHARGE_OTHER_FACILITY'] = '3' if value == '3' else '-999'
                res['DISCHARGE_OTHER_FACILITY_O3'] = '4' if value == '3' else '-999'
            elif key == 'DISCDAT':
                res['DISCHARGE_DATE'] = value
            else:
                continue
        return res
            


    def convert_xml_to_df(self, root, xmlns, openclinica, lstudy, n):
        """ Find each element "ClinicalData" in xml file and converted each subject to the row in the dataframe. If two versions of the form was filled, select IVT_TBY instead of RESQv2.0, and select RESQv2.0 instead of RESQv1.2. 

        Params: 
            root: The root of xml file.
            xmlns: The base schema.
            openclinica: The base schema for openclinica.
        Returns:
            A new converted dataframe. 
        """
        process ='Process' + str(n)

        # Total number of patients
        total_patients = 0
        for clinical_data in root.findall(xmlns + 'ClinicalData'):
            if clinical_data.get('StudyOID') in lstudy:
                total_patients += len(clinical_data.findall(xmlns + 'SubjectData'))

        count = 0
        LOG_EVERY_N = 100
        for clinical_data in root.findall(xmlns + 'ClinicalData'):
            study_oid = clinical_data.get('StudyOID')
            """
            if study_oid in self.skiped_study_oids:
                study_pts = len(clinical_data.findall(xmlns + 'SubjectData'))
                count += study_pts
                logging.info('{0}: {1} patients were skipped due to belonging to the demo site - {2}'.format(process, study_pts, study_oid))
            """
            if study_oid in lstudy:
                logging.info('{0}: Adding patients for study: {1}'.format(process, study_oid))
                # Get study name and protocol ID
                study = self.studies[clinical_data.get('StudyOID')]
                study_name = study["study_name"]
                protocol_id = study["protocol_name"]
                # Go through each subject in study
                for subject_data in clinical_data.iter(xmlns + 'SubjectData'):
                    subject_id = subject_data.get(openclinica + 'StudySubjectID')
                    row = {'Subject ID' : subject_id, 'Protocol ID': protocol_id, 'Site Name': study_name}
                    # Get StartDate and EndData
                    for study_event in subject_data.iter(xmlns + 'StudyEventData'):
                        start_date = study_event.get(openclinica + 'StartDate')
                        row['StartDate'] = start_date
                        end_date = study_event.get(openclinica + 'EndDate')
                        row['EndDate'] = end_date
                    # Get all forms, if both v1.2 and v2.0 are filled, select v2.0
                    form_oids = []
                    for form_data in subject_data.iter(xmlns + 'FormData'):
                        form_oids.append(form_data.get('FormOID'))
                    
                    # If more than one form is filled in, select v2.0
                    if len(form_oids) > 1:
                        for form_data in subject_data.iter(xmlns + 'FormData'):
                            form_oid = form_data.get('FormOID')
                            if "RESQV20" in form_oid and any("IVT_TBY" in val for val in form_oids) == False:
                                row['FormOID'] = form_oid
                                for item_data in form_data.iter(xmlns + 'ItemData'):
                                    shorten_name = self.items[item_data.get('ItemOID')]['shorten_name']
                                    row[shorten_name] = item_data.get('Value')
                            elif "IVT_TBY" in form_oid:
                                row['FormOID'] = form_oid
                                for item_data in form_data.iter(xmlns + 'ItemData'):
                                    shorten_name = self.items[item_data.get('ItemOID')]['shorten_name']
                                    row[shorten_name] = item_data.get('Value')
                    else:
                        row['FormOID'] = form_oids[0]
                        if "RESQV12" in form_oids[0]:
                            for item_data in subject_data.iter(xmlns + 'ItemData'):
                                shorten_name = self.items[item_data.get('ItemOID')]['shorten_name']
                                row[shorten_name] = item_data.get('Value')
                            row = self.refactor_values(row)
                        else:
                            for item_data in subject_data.iter(xmlns + 'ItemData'):
                                shorten_name = self.items[item_data.get('ItemOID')]['shorten_name']
                                row[shorten_name] = item_data.get('Value')
                        
                    
                    if n == 0:
                        self.df1 = self.df1.append(row, ignore_index=True)
                    elif n == 1:
                        self.df2 = self.df2.append(row, ignore_index=True)
                    elif n == 2:
                        self.df3 = self.df3.append(row, ignore_index=True)
                    elif n == 3:
                        self.df4 = self.df4.append(row, ignore_index=True)
                    elif n == 4:
                        self.df5 = self.df5.append(row, ignore_index=True)
                    elif n == 5:
                        self.df6 = self.df6.append(row, ignore_index=True)
                    elif n == 6:
                        self.df7 = self.df7.append(row, ignore_index=True)
                    elif n == 7:
                        self.df8 = self.df8.append(row, ignore_index=True)
                    elif n == 8:
                        self.df9 = self.df9.append(row, ignore_index=True)
                    elif n == 9:
                        self.df10 = self.df10.append(row, ignore_index=True)
                    
                    count += 1

                    if (count % LOG_EVERY_N) == 0:
                        percentage = round(count/total_patients*100, 2)
                        logging.info('{0}: Number of already converted patients: {1}/{2} - {3}%'.format(process,count, total_patients, percentage))
                    if count == total_patients:
                        percentage = count/total_patients*100
                        logging.info('{0}: The conversion has been finished: {1}/{2} - {3}%'.format(process, count, total_patients, percentage))
                    
                else:
                    pass
            
        return True

    






