import os
import copy
import json
import random
import argparse
import gzip
from tqdm import tqdm
from transformers import RobertaTokenizer

def read_json(path):
    with open(path, 'r') as f:
        json_data = json.load(f)
    return json_data

def save_json(file, path):
    with open(path, 'w') as f:
        json.dump(file, f, indent = 4)

def read_jsonl_gz_to_list(jsonl_file):
    """Read a gzipped JSONL file and return a list of dictionaries."""
    data_list = []
    with gzip.open(jsonl_file, 'rt', encoding='utf-8') as file:
        for line in file:
            data_list.append(json.loads(line))
    return data_list

def save_dict_list_to_jsonl_gz(data_list, filename):
    """Save a list of dictionaries to a gzipped JSONL file."""
    with gzip.open(filename, 'wt', encoding='utf-8') as file:
        for entry in data_list:
            json_line = json.dumps(entry)
            file.write(json_line + '\n')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--single_all', type = str, default = 'all')
    parser.add_argument("--level", default = 'very_hard', type=str)
    
    args = parser.parse_args()
    single_all = args.single_all
    level = args.level
    
    base_path = os.getcwd()
    python_data_path = os.path.join(base_path, 'python_data')
    data_path = os.path.join(python_data_path, f'{level}_filtered_tc_cov.jsonl.gz')
    
    json_data = read_jsonl_gz_to_list(data_path)
    tqdm.write(f'len(data) {len(json_data)}')

    save_list = []
    final_data_list = []
    
    for single_data in tqdm(json_data, desc = 'Progress', leave = True):

        if single_all == 'single':
            save_key = f"{single_data['pid']}_{single_data['code_index']}"
            if save_key in save_list:
                continue
            else:
                final_data_list.append(single_data)
                save_list.append(save_key)
                
        elif single_all == 'all':
            final_data_list.append(single_data)

    tqdm.write(f'final len {len(final_data_list)}')
    if single_all == 'single':
        save_dict_list_to_jsonl_gz(
            final_data_list,
            os.path.join(
                python_data_path, 
                f'{level}_filtered_{single_all}.jsonl.gz'))
    elif single_all == 'all':
        save_dict_list_to_jsonl_gz(
            final_data_list,
            os.path.join(
                python_data_path, 
                f'{level}_filtered_{single_all}.jsonl.gz'))

if __name__ == '__main__':
    main()