#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import gspread
from google.oauth2.service_account import Credentials
# import pandas as pd
import time
from config_local import *
#imports SPREADSHEET_ID, 
# READ_FROM_FILE_FLAG, 
# OUTPUT_IN_LOCAL_FILE_FLAG , 
# GSHEET_OUTPUT_FLAG, 
# LOCAL_POINTNAMES_FILE, 
# SECRET_FILENAME,
# IGNORE_FIRST_WORD_FLAG

from datetime import datetime
import requests

__author__ = "Gerasimos Kounadis"
__license__ = "MIT"
__url__ = "https://github.com/arup-group/dbo_validation"
__version__ = "0.1"
__email__ = "gerasimoskounadis@gmail.com"


SLEEP_TIME = 3 #  be > 3  otherwise the gsheet API breaks for many requests "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'
DATETIME_FORMAT = "%Y%m%d_%H%M%S" # to add in local output file
LOCAL_YAML_FILE = 'subfields.yaml' # if reading from github fails, reads from local yaml file
if READ_FROM_FILE_FLAG : 
    GSHEET_OUTPUT_FLAG = False #no sense if reading pointnames from file to write in gsheets



scope = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
if not READ_FROM_FILE_FLAG:
    creds = Credentials.from_service_account_file(SECRET_FILENAME, scopes=scope)


class Validator:
    def __init__(self):
        self.subfields_dict ={}
        self.subfields_list =[]
        self.results_dict = {}
        self.results_list=[]
        self.field_order_dict = {
            'aggregation_descriptor' : 0,
            'aggregation': 1, # optional * 1
            'descriptor': 2, # optional * 1
            'component': 3, # optional * 10
            'measurement_descriptor': 4, # optional * 1
            'measurement':  5, # optional * 1
            'point_type': 6 # required * 1           
        }
        #(<agg_desc>_)?(<agg>_)?(<descr>_)*(<component>_)?(<meas. descr>_)?(<meas>_)?<pointtype>(_<num> )*
        
        try :
            self.read_from_github()
            print('Reading from github')
        except:
            self.read_from_yaml()

    def words_to_ordering_number(self, pointname_list):
        num_pointname =[]
        for i, word in enumerate(pointname_list):
            print(i, word)
        return num_pointname

    def _validate_order(self, order_list, pointname_list):
        '''returns list of strings'''
        output =[]        
        # if order_list[-1] != self.field_order_dict['point_type']:
        #     # print(f'{order_list[-1]}, {self.subfields_dict["point_type"]}')
        #     output.append(f'{pointname_list[-1]} is not a point_type')
        # else:
        #     output.append('Point type ok')
        sorted_list = sorted(order_list)
        pointname_in_order = pointname_list.copy()
        pointname_order_dict = {}
        if sorted_list == order_list:
            print('order is ok')
            # output.append('order is ok')
        else:
            for i,v in enumerate(pointname_list):
                if order_list[i] != 2:
                    correct_index = sorted_list.index(order_list[i])
                    # print(v, i, order_list[i], sorted_list[i], correct_index)
                    pointname_order_dict[correct_index] = v
                else:
                    pointname_order_dict[i] = v
            for i in pointname_order_dict.keys():
                pointname_in_order[i] = pointname_order_dict[i]

            correct_order_string = '_'.join(pointname_in_order)
            output.append(f'correct order is: {correct_order_string}')


        if len(output) > 0:
            pointname_string = '_'.join(pointname_list)
            self.results_dict[pointname_string] = correct_order_string
            return output
        

    def validate_pointname_order(self, pointname_list):
        order_list = self._create_order_list(pointname_list)
        print(order_list)
        print(order_list)
        if len(order_list) == len(pointname_list):
            output = self._validate_order(order_list, pointname_list)
        else:
            output = ['not valid subfield']
        # print(pointname_list)
        return output
                    

    def _create_order_list(self, pointname_list):
        order_list =[]
        handler = ''
        for subfield in pointname_list:
            print(subfield)
            for field in self.subfields_dict.keys():
                # print(field)
                if subfield in self.subfields_dict[field]:
                    order_list.append(self.field_order_dict[field])
        # print(order_list)
        return order_list

    def _remove_prefix(self, pointname_list):
        pointname_list.pop(0) # BDNS device, remove from pointlist ['run','status','1']
        return pointname_list # validate last word against point types
    
    def _check_remove_suffix(self, last_subfield, pointname_list):
        if last_subfield.isnumeric() or '-' in last_subfield: # isnumeric doesnt get negative numbers
            pointname_list.pop(-1)  # remove numeric value from pointname_list
        return pointname_list

    def _split_string(self, pointname):
        pointname_list = pointname.split('_') # ['DB-13','run','status','1']
        return pointname_list
    
    def validate_pointname(self, pointname):
        pointname_list = self._split_string(pointname)
        if IGNORE_FIRST_WORD_FLAG:
            pointname_list = self._remove_prefix(pointname_list)

        last_subfield = pointname_list[-1]

        self._check_remove_suffix(last_subfield, pointname_list)

        for subfield in pointname_list:
            output =[]
            string_result = self.validate_in_subfields_list(subfield)
            if 'OK' not in string_result:
                self.results_dict[subfield] = string_result
                temp_output = '%s - %s'%(subfield, string_result)
                output.append(temp_output)
                self.results_list.append(output)
            else:
                self.results_list.append('')
        
        self.validate_point_type(pointname)

        self.validate_pointname_order(pointname_list)


    def validate_in_subfields_list(self, subfield):
        if subfield not in self.subfields_list:
            string_result = 'NOT a valid subfield'
        else:
            string_result = 'OK'
        return string_result

    def validate_point_type(self, pointname):
        #pointname = max_temperature_sensor_1
        output =[]
        pointname_list = pointname.split('_') # ['DB-13','run','status','1']
        # print(pointname_list)

        if IGNORE_FIRST_WORD_FLAG:
            pointname_list.pop(0) # BDNS device, remove from pointlist ['run','status','1']

        cur_point_type = pointname_list[-1] # validate last word against point types

        if cur_point_type.isnumeric() or '-' in cur_point_type: # isnumeric doesnt get negative numbers
            pointname_list.pop(-1)  # remove numeric value from pointname_list
            if int(cur_point_type) <= 0:
                result = 'Negative number'
                temp_output = '%s - %s'%(cur_point_type, result)
                print(temp_output)
                output.append(temp_output)
                self.results_dict[cur_point_type] = result
                print(output)
            # print(pointname_list)
            cur_point_type = pointname_list[-1] # skip numeric values e.g status_2

        if cur_point_type not in self.subfields_dict['point_type']:
            result = 'NOT a valid point type'
        else:
            result = 'OK'
        temp_output = '%s - %s'%(cur_point_type, result)
        #here writes output to list and dictionary
        if 'OK' not in result:
            self.results_dict[cur_point_type] = result
            output.append(temp_output)
            self.results_list.append(output)
        else:
            self.results_list.append(' ')
        # print(output)
        #output is a list
        print(output)
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
        keys = ['aggregation','aggregation_descriptor','component','descriptor', 'measurement','measurement_descriptor', 'point_type']
        key = 'initialize'
        for i in words_list:
            if i in keys:
                self.subfields_dict[i] = []
                key=i
            else:
                self.subfields_dict[key].append(i)
            if i not in keys:
                self.subfields_list.append(i)
        print(self.subfields_dict)
        print(self.subfields_list)
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
        #row is list
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
                    output = self.validate_point_type(sheet.cell(c,3).value)
                    result = ' , '.join(output) #since output is a list
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
        # rows = []
        for row in csvreader:
            pointname = row[0]
            # print(pointname)
            # rows.append(pointname)
            valid.validate_pointname(pointname)
        # print(rows)
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
            valid.write_results_list(list_filename, row)
        
        for d in valid.results_dict.keys():
            row = '%s - %s'%(d,valid.results_dict[d])
            # print(row)
            valid.write_results_dict(dict_filename, row)


if __name__ == '__main__':
    main()