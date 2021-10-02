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
LOCAL_YAML_FILE = 'subfields.yaml' # if reading from github fails, reads from local yaml file

scope = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SECRET_FILENAME, scopes=scope)


class Validator:
    def __init__(self):
        self.subfields_dict ={}
        self.results_dict = {}
        self.results_list=[]

        try :
            self.read_from_github()
            print('Reading from github')
        except:
            self.read_from_yaml()

    def validate_point_type(self,word):
        cur_point_type = word.split('_')[-1] # validate last word against point types
        if cur_point_type.isnumeric():
            cur_point_type = word.split('_')[-2] # skip numeric values e.g status_2
        # print(cur_point_type)
        if cur_point_type not in self.subfields_dict['point_type']:
            result = 'NOT a valid point type'
        else:
            result = 'OK'
        output = '%s - %s'%(cur_point_type, result)
        if 'OK' not in result:
            self.results_dict[cur_point_type] = result
            self.results_list.append(output)
        else:
            self.results_list.append(' ')
        return output
    
    def read_from_github(self):
        #creates a dictionary self.subfields_dict
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
        yaml_file = LOCAL_YAML_FILE
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

    if  OUTPUT_IN_LOCAL_FILE_FLAG == False and GSHEET_OUTPUT_FLAG == False:
        print('Warning: OUTPUT_IN_LOCAL_FILE_FLAG and GSHEET_OUTPUT_FLAG are False')

    valid = Validator()
    
    if READ_FROM_FILE_FLAG:
        print('Reading points from local file')
        file = open(LOCAL_POINTNAMES_FILE)
        csvreader = csv.reader(file)
        rows = []
        for row in csvreader:
            word=row[0]
            rows.append(word)
            valid.validate_point_type(word)
        print(rows)
    else:
        valid.read_write_gsheet()


    if OUTPUT_IN_LOCAL_FILE_FLAG:
        print('write to local file')
        now = datetime.now()
        date_time_str = now.strftime(DATETIME_FORMAT)
        list_filename = 'results_%s_list.csv'%date_time_str
        dict_filename = 'results_%s_dict.csv'%date_time_str
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


if __name__ == '__main__':
    main()