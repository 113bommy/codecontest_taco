import os
import copy
import json
import random
import argparse
import gzip
from tqdm import tqdm
from transformers import RobertaTokenizer

def read_jsonl_to_list(jsonl_file):
    data_list = []
    with open(jsonl_file, 'r') as file:
        for line in file:
            data_list.append(json.loads(line))
    return data_list
    
def save_dict_list_to_jsonl_gz(data_list, filename):
    with gzip.open(filename, 'wt', encoding='utf-8') as file:
        for entry in data_list:
            json_line = json.dumps(entry)
            file.write(json_line + '\n')
            
def read_jsonl_gz_to_list(jsonl_file):
    """Read a gzipped JSONL file and return a list of dictionaries."""
    data_list = []
    with gzip.open(jsonl_file, 'rt', encoding='utf-8') as file:
        for line in file:
            data_list.append(json.loads(line))
    return data_list

def parse_trace_string(trace_string):
    """
    parse trace data
    Input = trace string (Formatted as "[1: {a: 20, b: 40} | 2: {a: 20, b: 40, c: 60} | 4: ]")
    Output = True / False (False if the trace data length is 0, otherwise True)
    """
    trace_content = trace_string.split('[', 1)[1].rsplit(']', 1)[0]
    result_dict = {}
    # Return False if trace string is empty
    if trace_string == '[]':
        return False

    try:
        for item in trace_content.split(' | '):
            key, value = item.split(':', 1)
            key = key.strip()
            # check each trace data has valid data
            if '{' in value and '}' in value:
                value_content = value.strip().strip('{}')
                sub_dict = {}
                for pair in value_content.split(' , '):
                    pair = pair.strip()
                    if len(pair) == 0:
                        continue
                    k, v = pair.split(':', 1)
                    sub_dict[k.strip()] = v.strip()
                result_dict[key] = sub_dict
                # Valid trace data - Return True
                return True
            else:
                result_dict[key] = None 
    except ValueError:
        return False
            
    # There is no valid trace data
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold', type = int, default = 4096)
    parser.add_argument('--depth', type = str, default = 'small')
    parser.add_argument('--data_type', type = str, default = 'hard')
    args = parser.parse_args()
    threshold = args.threshold
    depth = args.depth
    data_type = args.data_type
    
    base_path = os.getcwd()
    print(f'{data_type} data filtering started')
    single_case_data_path = os.path.join(base_path, f'./data/{data_type}_filtered_single_tc.jsonl.gz')
    all_cases_data_path = os.path.join(base_path, f'./data/{data_type}_filtered_all_tc.jsonl.gz')
    
    # Loading Trace added python data path
    path_dict = {
        'single' : single_case_data_path,
        'all' : all_cases_data_path
    }
    
    for d_t, single_path in tqdm(path_dict.items(), desc = 'single / all', leave = True):
        try:
            # Loading Trace added python data
            json_data = read_jsonl_gz_to_list(single_path)
        except FileNotFoundError:
            print(f"No File {single_case_data_path}")
            continue

        save_list = []
        final_data_list = []
        
        for single_data in tqdm(json_data, desc = f'{d_t}', leave = True):
            
            # Parse expected / actual / trace data
            target_data = single_data['input_expected_actual_trace']
            input_data = target_data.split('@Input = [')[-1].split('] @Expected = [')[0].strip()
            expected_data = target_data.split('@Expected = [')[-1].split('] @Actual = [')[0].strip()
            actual_data = target_data.split('] @Actual = [')[-1].split(']')[0].strip()
            trace_data = target_data.split('] @Trace = ')[-1].strip()
    
            # Check whether each trace data has useful information
            if parse_trace_string(trace_data):
                # Find trace part and preprocess data
                whole_trace_part = trace_data
                whole_trace_parsed = whole_trace_part[1:-1].split(' | ')

                temp_list = []
                
                for parsed_data in whole_trace_parsed:
                    try:
                        line_number = parsed_data.split(':', 1)[0].strip()
                        variable_data = parsed_data.split(':', 1)[1].strip()
                        if variable_data == '':
                            continue
                    except IndexError:
                        continue
                    temp_list.append(parsed_data)
                
                updated_trace_part = '[' + ' | '.join(temp_list) + ']'
                
                # Choose data only if actual result and expected result are different
                if d_t == 'single':  
                    save_key = f"{single_data['pid']}_{single_data['code_index']}"
                    if save_key in save_list:   
                        continue
                    else:
                        single_data['input_expected_actual_trace'] = single_data['input_expected_actual_trace'].replace(whole_trace_part, updated_trace_part)
                        final_data_list.append(single_data)
                        save_list.append(save_key)
                else:
                    single_data['input_expected_actual_trace'] = single_data['input_expected_actual_trace'].replace(whole_trace_part, updated_trace_part)
                    final_data_list.append(single_data)
            
        tqdm.write(f'{d_t} data length = {len(json_data)}')
        tqdm.write(f'filtered {d_t} data length = {len(final_data_list)}')
        save_dict_list_to_jsonl_gz(final_data_list, os.path.join(base_path, f'python_{data_type}_final_filtered_{d_t}.jsonl.gz'))

if __name__ == '__main__':
    main()