import csv
# a = ['PS-1_input_disconnector', 'PS-1_medium_voltage_transformer_breaker', 'PS-1_transformer_temperature_alarm', 'PS-1_voltage_lacking_relay']
POINTNAMES_FILE = 'pointnames.csv'
RESULTS_FILE = 'results.csv'

class Validator:
    def __init__(self):
        self.subfields_dict ={}
        file = open(POINTNAMES_FILE)
        csvreader = csv.reader(file)
        rows = []
        for row in csvreader:
                rows.append(row)
        # print(rows)
        self.b = [i[0] for i in rows]
        # print(self.b)
        self.read_from_yaml()

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

    def validate_point_type(self):
        self.results_list =[]
        self.results_dict ={}
        for i in self.b:
            cur_point_type = i.split('_')[-1]
            if cur_point_type.isnumeric():
                cur_point_type = i.split('_')[-2]
            # print(cur_point_type)

            if cur_point_type not in point_type_list:
                result = 'NOT a valid point type'
            else:
                result = 'OK'
            self.results_dict[cur_point_type] = result
            self.results_list.append([i, cur_point_type, result])
        return (self.results_dict, self.results_list)
    
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

    def write_results_list(self, row):
        with open(RESULTS_FILE, mode='a') as res_file:
            res_file_writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE)
            res_file_writer.writerow(row)

# def write_results_dict(row):
#     with open(RESULTS_FILE, mode='a') as res_file:
#         list_row = list(row.split('-'))
#         res_file_writer = csv.writer(res_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE)
#         res_file_writer.writerow(list_row)



valid = Validator()
point_type_list = valid.subfields_dict['point_type']
valid.validate_point_type()
print(valid.results_list)
print('ok')

f = open(RESULTS_FILE, mode='w+') # to clear the output file
f.truncate()
f.close()

for li in valid.results_list:
    row = li
    print(row)
    valid.write_results_list(row)
# for d in valid.results_dict.keys():
#     row ='%s - %s'%(d,valid.results_dict[d])
#     write_results_dict(row)




# write_results()
# valid.read_from_yaml()


