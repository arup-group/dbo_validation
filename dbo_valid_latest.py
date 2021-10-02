import csv
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import time
from config_local import SPREADSHEET_ID, READ_FROM_FILE_FLAG, OUTPUT_IN_LOCAL_FILE_FLAG , GSHEET_OUTPUT_FLAG, LOCAL_POINTNAMES_FILE, SECRET_FILENAME
from datetime import datetime
import requests

SLEEP_TIME = 3 #  be > 3  otherwise the gsheet API breaks for many requests "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'
DATETIME_FORMAT = "%Y%m%d_%H-%M-%S" # to add in local output file


scope = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SECRET_FILENAME, scopes=scope)


class Validator:
    def __init__(self):
        self.subfields_dict ={}
        self.results_dict = {}
        self.results_list=[]
        # file = open(LOCAL_POINTNAMES_FILE)
        # csvreader = csv.reader(file)
        # rows = []
        # for row in csvreader:
        #         rows.append(row)
        # # print(rows)
        # self.b = [i[0] for i in rows]
        # # print(self.b)
        try :
            self.read_from_github()
            print('Reading from github')
        except:
            self.read_from_yaml()
            print('Reading from local yaml file')

        # self.subfields_dict = {
        # 'point_type' : {
        # 'accumulator': "The total accumulated quantity (e.g. total energy accumulated).",
        # 'alarm': "A point that interprets some input values qualitatively (e.g. as good or bad, normal or in alarm, etc.). Alarms are always binary.",
        # 'capacity': "A design parameter quantity. Ex: design motor power capacity. Is always a maximum limit.",
        # 'counter': "Special case of accumulator that assumes integer values and non-dimensional units",
        # 'command': "The signal given to make an action happen. Defaults to multistate unless given a measurement type",
        # 'count': "Total count of actions or requests.",
        # 'label': "Identifying alias for component or system.",
        # 'mode': "Distinct mode of operation within system. Common example is economizer mode (enabled or disabled).",
        # 'requirement': "A lower limit design parameter (e.g. minimum flowrate requirement). Is always a lower limit.",
        # 'sensor': "Component used to measure some quality of a system or process. Can be feedback for an analog command.",
        # 'setpoint': "Control target of process or system.",
        # 'status': "The multistate value indicating an observed state in a piece of equipment, often indicating if a command was effected. It is a neutral observation (e.g. no quality judgment of 'good' or 'bad'). It also has no units of measurement (therefore if combined with a measurement subfield, it will indicate that the field is the directional status based on some measurement of that type, e.g. power_status equates to an on/off value based on some inference of power).",
        # 'specification': "The specified design value for a particular operating condition (differential pressure specification).",
        # 'timestamp': "An instant in time, represented as a numeric offset from the epoch.",
        # }
        # }

    def validate_point_type(self,word):
        cur_point_type = word.split('_')[-1] # validate last word against point types
        if cur_point_type.isnumeric():
            cur_point_type = word.split('_')[-2] # skip numeric values e.g status_2
        # print(cur_point_type)
        if cur_point_type not in self.subfields_dict['point_type']:
            result = 'NOT a valid point type'
        else:
            result = 'OK'
        # self.results_dict[cur_point_type] = result #creates a dictionary with results, good solution as a conclusion
        output = '%s - %s'%(cur_point_type, result)
        self.results_dict[cur_point_type] = result
        self.results_list.append(output)
        return output
    
    def read_from_github(self):
        # URLRAW = 'https://raw.githubusercontent.com/google/digitalbuildings/master/ontology/yaml/resources/subfields/subfields.yaml'
        URL = 'https://github.com/google/digitalbuildings/blob/master/ontology/yaml/resources/subfields/subfields.yaml'

        symbols='<,>' # to check against tags like <html
        page = requests.get(URL)
        start_tag = '<span class=\"pl-ent\">'
        end_tag = '</span>:'
        data=page.text
        count = 0
        new_pos = 0
        words_list = []
        while True:
            start_pos = data.find(start_tag, new_pos) +len(start_tag)
            end_pos = data.find(end_tag, start_pos)
            data_word = data[start_pos: end_pos]
            data_first_letter = data_word[0]
            if data_first_letter not in symbols:
                # print(data_word)
                if data_word not in words_list:
                    words_list.append(data_word)
                else: # if word encountered for 2nd time, break loop
                    break
            new_pos = end_pos
            count +=1
            if count >= 1000:
                break
        # print(count)
        keys = ['aggregation','component','descriptor', 'measurement','measurement_descriptor', 'point_type']
        key = 'initialize'
        for i in words_list:
            if i in keys:
                self.subfields_dict[i] = []
                key=i
            else:
                self.subfields_dict[key].append(i)
        # print(self.subfields_dict)
        return None

    def read_from_yaml(self):
        list_of_lines =[]
        yaml_file = 'subfields.yaml'
        print('Reading %s ..' %yaml_file)
        with open (yaml_file) as f:
            file_str = f.readlines()
            for line in file_str:
                lines=line.strip()
                if not lines.startswith('#'):
                    list_of_lines.append(lines.split(':'))
                    # print(list_of_lines)
        key = 'initialise'
        for i in list_of_lines:
            if len(i) > 1 and i[1] == '':
                self.subfields_dict[i[0]] = []
                key = i[0]
            elif len(i) > 1 and i[1] != '':
                self.subfields_dict[key].append(i[0])
                # print(key, i[0])
        # print(self.subfields_dict)
        return None

    def write_results_list(self, filename, row):
        with open(filename , mode='a') as res_file:
            res_file_writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE)
            res_file_writer.writerow(row)
        return None

    def write_results_dict(self, filename, row):
        with open(filename, mode='a') as res_file:
            list_row = list(row.split('-'))
            res_file_writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE)
            res_file_writer.writerow(list_row)

    def read_write_gsheet(self):
        client = gspread.authorize(creds)
        google_sh = client.open_by_key(SPREADSHEET_ID)
        worksheet_list = google_sh.worksheets() # get list of worksheets
        print(worksheet_list)

        for i , s in enumerate(worksheet_list):
            sheet = google_sh.get_worksheet(i)
            print(sheet)
            # print("no. of rows:", len(sheet.col_values(3)))
            for c in range(len(sheet.col_values(3))):  # c is number of row , col_value(3) is the 3rd column that contains tha pointnames
                time.sleep(SLEEP_TIME)
                c=c+3 # to skip header and 0 value , number of row - to be inluded it in range(3, len)
                # print(c)
                print(sheet.cell(c,3).value)
                if sheet.cell(c,3).value is not None:
                    result = self.validate_point_type(sheet.cell(c,3).value)
                    if  GSHEET_OUTPUT_FLAG and 'OK' not in result: # skip writing if point is valid or GSHEET_OUTPUT_FLAG == False
                        sheet.update_cell(c, 5, result) # write the result in 5th column, column E
                else:
                    pass
                

def main():
    print('Output in local file flag : %s'%OUTPUT_IN_LOCAL_FILE_FLAG )
    print('Output in googlesheet flag : %s'%GSHEET_OUTPUT_FLAG )
    valid = Validator()
    valid.read_write_gsheet()

    # point_type_list = valid.subfields_dict['point_type']
    # valid.validate_point_type()
    # print(valid.results_list)
    # print('ok')

    if OUTPUT_IN_LOCAL_FILE_FLAG:
        print('write to local file')
        now = datetime.now()
        date_time_str = now.strftime(DATETIME_FORMAT)
        list_filename = 'results_list__%s.csv'%date_time_str
        dict_filename = 'results_dict__%s.csv'%date_time_str
        ## f = open(LOCAL_RESULTS_FILE, mode='w+') # to clear the output file if datetime not used
        ## f.truncate()
        ## f.close()

        for li in valid.results_list:
            row = li
            # print(row)
            valid.write_results_list(list_filename, [row])
        
        for d in valid.results_dict.keys():
            row ='%s - %s'%(d,valid.results_dict[d])
            print(row)
            valid.write_results_dict(dict_filename, row)
        pass
    else:

        pass 
    





# write_results()
# valid.read_from_yaml()


if __name__ == '__main__':
    main()