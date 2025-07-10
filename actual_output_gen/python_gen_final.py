import json
import os
import copy
import gzip
import argparse
from tqdm import tqdm

def read_jsonl_gz(filename):
    with gzip.open(filename, 'rt', encoding='utf-8') as file:
        return [json.loads(line) for line in file]
            
def save_dict_list_to_jsonl_gz(data_list, filename):
    with gzip.open(filename, 'wt', encoding='utf-8') as file:
        for entry in data_list:
            json_line = json.dumps(entry)
            file.write(json_line + '\n')
            
def save_list_to_jsonl(data_list, jsonl_file):
    # Open and write the JSONL file
    with open(jsonl_file, 'w') as file:
        for value in data_list:
            # Write each JSON object to a new line
            file.write(json.dumps(value) + '\n')
            
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--single_all', type = str, default = 'all')
    parser.add_argument("--level", default = 'very_hard', type=str)
    args = parser.parse_args()
    
    raw_file = os.path.join(os.getcwd(), f'python_{args.level}_final_filtered_all.jsonl.gz')
    raw_data_list = read_jsonl_gz(raw_file)

    parse_by_key = {}
    
    for single_data in tqdm(raw_data_list, leave=True):
        pid = single_data['pid']
        code_index = single_data['code_index']
        key = f'{pid}_{code_index}'
    
        if key not in parse_by_key:
            parse_by_key[key] = []
        
        parse_by_key[key].append(single_data)
        
    sbfl_able_data_list = []
    for code_index, single_code in tqdm(enumerate(parse_by_key.values()), leave=True):
        no_exist = False
        yes_exist = False
        correct_check = 0
        incorrect_check = 0
        for single_data in single_code:
            expected_actual = single_data['input_expected_actual']
            expected_actual = expected_actual.split('] @Expected =')[1]
            expected, actual = expected_actual.split(' @Actual = ')
            expected = expected.strip()
            actual = actual.strip()

            expected = expected[1:-1].split(', ')
            actual = actual[1:-1].split(', ')

            if expected == actual:
                single_data['true_false'] = 'True'
                no_exist = True
                correct_check += 1
            elif expected != actual:
                single_data['true_false'] = 'False'
                yes_exist = True
                incorrect_check += 1
        
        # Filter Only SBFL able data (Correct Test Case exists, Incorrect Test Case exist)
        if (no_exist and yes_exist) and incorrect_check >= 5:
            sbfl_able_data_list.append(copy.deepcopy(single_code))
            
    if len(sbfl_able_data_list) > 200:
        sbfl_able_data_list = sbfl_able_data_list[:200]
    elif len(sbfl_able_data_list) < 200:
        print(f'Warning: Data is less than 200, only {len(sbfl_able_data_list)} found.')
        
    print(f'Before Data filtering: {len(raw_data_list)}')
    print(f'After Data filtering: {len(sbfl_able_data_list) * 10}')
    
    save_dict_list_to_jsonl_gz(sbfl_able_data_list, os.path.join(os.getcwd(), f'python_data/{args.level}_data.jsonl.gz'))
    
if __name__ == "__main__":
    main()