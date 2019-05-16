import json
import feature_manipulation

def load_from_file(file_name, columnDefs):
    data=[]
    with open(file_name, 'r') as f:
        for line in f:
            if len(line) <=1:
                continue
            else:
                data.append(feature_manipulation.charToColumns(json.loads(line), columnDefs))
    return data

def load_from_files(file_names, columnDefs):
    data=[]
    for file_name in file_names:
        data.extend(load_from_file(file_name, columnDefs))
    return data
