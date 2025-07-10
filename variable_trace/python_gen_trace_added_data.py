import os
import json
import gzip
import shutil
import random
import argparse
import copy
from multiprocessing import Pool, cpu_count
from transformers import AutoTokenizer
from tqdm import tqdm


def read_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def save_json(file, path):
    with open(path, 'w') as f:
        json.dump(file, f, indent=4)


def open_gz(data_path: str) -> dict:
    with gzip.open(data_path, 'rt', encoding='utf-8') as f:
        return json.load(f)

def compare_dict(dict1, dict2):
    key_set = set(dict1.keys()).union(set(dict2.keys()))

    differences = {}
    
    for key in key_set:
        if key not in dict1:
            differences[key] = f'{dict2[key]}'
        elif key not in dict2:
            differences[key] = f'returned'
        elif dict1[key] != dict2[key]:
            differences[key] = f'{dict1[key]} -> {dict2[key]}'
    
    return differences

def compress_file(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

            
def detect_complete_loops(parsed_code):
    lines = parsed_code.split('|||')
    in_loop = False
    loop_structure = []
    detected_loops = [] 

    for line in lines:
        line = line.strip()
    
        if line:  
            parts = line.split(' ', 1)
            
            if len(parts) == 2:
                line_number, statement = parts
                line_number = line_number.strip()
                statement = statement.rstrip()
                if statement.startswith("for ") or statement.startswith("while "):
                    in_loop = True 
                    loop_structure.append(int(line_number))
                elif in_loop:
                    if statement.startswith("  ") or statement.startswith('\t'):  
                        loop_structure.append(int(line_number))
                    else:
                        in_loop = False
                        if loop_structure:
                            detected_loops.append(loop_structure)
                            loop_structure = [] 
            else:
                pass
        else:
            if in_loop:
                continue
    
    if in_loop and loop_structure:
        detected_loops.append(loop_structure)

    return detected_loops

def track_final_changes(differences_list):
    final_changes = {}

    for differences in differences_list:
        for key, change in differences.items():
            if '->' in change:
                new_value = change.split('->')[-1].strip()
                final_changes[key] = new_value
            else:
                final_changes[key] = change
    
    return final_changes

def group_consecutive_numbers(nums):
    if not nums:
        return []
    nums = sorted(nums)
    
    grouped = [[nums[0]]]

    for i in range(1, len(nums)):
        if nums[i] == nums[i - 1] + 1:
            grouped[-1].append(nums[i])  
        else:
            grouped.append([nums[i]]) 

    return grouped

def find_data_in_nested_list(nested_list, target):
    for inner_list in nested_list:
        if target in inner_list:
            return inner_list  
    return None 


def compress_trace_coverage(trace_data: dict, loop_detect: list) -> str:
    trace_data_list = []
    difference_data_list = []
    coverage_data_list = []
    trace_string_list = []

    for step, trace in trace_data.items():
        line_number = int(trace['line'])
        variable = trace['variables']
        trace_data_list.append((line_number, variable))

    for index in range(len(trace_data_list) - 1):
        previous_lineno, previous_data = trace_data_list[index]
        new_lineno, new_data = trace_data_list[index + 1]
        differences = compare_dict(previous_data, new_data)
        difference_data_list.append((previous_lineno - 1, differences))
        coverage_data_list.append(previous_lineno - 1)

    coverage_data_list = coverage_data_list[1:]

    loop_dict = {}

    for index, (lineno, diff_data) in enumerate(difference_data_list):
        for single_loop in loop_detect:
            if lineno in single_loop:
                if str(single_loop) not in loop_dict.keys():
                    loop_dict[str(single_loop)] = [index]
                else:
                    loop_dict[str(single_loop)].append(index)
                break
 
    grouped_dict = {}

    for loop_name, loop_list in loop_dict.items():
        grouped_dict[loop_name] = group_consecutive_numbers(loop_list)


    final_index = 0

    while(1):
        if final_index >= len(difference_data_list):
            break

        lineno, diff_data = difference_data_list[final_index]
        if lineno == 0:
            final_index += 1
            continue

        find_data = None  
        
        for loop_name, single_nested_list in grouped_dict.items():
            find_data = find_data_in_nested_list(single_nested_list, final_index)
            if find_data is not None:
                break

        if find_data is not None:
            single_loop_data = []
            for i in find_data:
                _, diff = difference_data_list[i]
                single_loop_data.append(diff)

            compressed_loop = track_final_changes(single_loop_data)
            trace_string_list.append(f'{loop_name}: ' + '{' + ' , '.join([f'{k}: {v}' for k, v in compressed_loop.items()]) + '}')
            final_index = find_data[-1] + 1
        else:
            if len(diff_data.keys()) == 0:
                trace_string_list.append(f'{lineno}: ')
            else:
                trace_string_list.append(f'{lineno}: ' + '{' + ' , '.join([f'{k}: {v}' for k, v in diff_data.items()]) + '}')

            final_index += 1


    return coverage_data_list, ' | '.join(trace_string_list)


def process_code(args):
    pid_index, incorrect_data, pid_split_dict = args
    if pid_index not in pid_split_dict.keys():
        return None

    split_index = os.path.basename(incorrect_data).split('_')
    code_index = split_index[4]
    case_index = split_index[5].split('.json')[0]

    # Load data from original incorrect data
    stored_data = pid_split_dict[pid_index][int(code_index)]
    stored_index, full_data = stored_data
    assert int(code_index) == int(stored_index)

    try:
        # Make trace comment added code
        input_data = full_data['test_case']['input'][int(case_index)]
        output_data = full_data['test_case']['output'][int(case_index)]
    except IndexError:
        return None

    loop_detect_data = full_data['incorrect_code']
    loop_detect = detect_complete_loops(loop_detect_data)

    # Generate trace compressed data
    single_incorrect_trace = open_gz(incorrect_data)
    
    coverage_data, compressed_trace = compress_trace_coverage(single_incorrect_trace, loop_detect)

    full_comment = ' # @Input = [' + input_data +  '] @Expected = [' + output_data + '] @Trace = [' + compressed_trace + ']'

    trace_added_code = full_data['incorrect_code'] + full_comment
    full_data['trace_code'] = trace_added_code
    full_data['code_index'] = int(code_index)
    full_data['coverage_data'] = coverage_data
    
    if 'def main' in full_data['incorrect_code']:
        return None

    # Save the full data
    return copy.deepcopy(full_data)

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
    parser.add_argument("--level", default = 'very_hard', type=str)
    
    args = parser.parse_args()
    level = args.level
    
    base_path = os.getcwd()
    trace_path = os.path.join(base_path, 'python_trace')
    
    pid_split_dict = {}
    all_trace_added_list = []
    
    trace_type_path = os.path.join(trace_path, f'{level}', 'python_incorrect')
    data_list = os.listdir(trace_type_path)
    json_file_path = os.path.join(base_path, 'python_data', f'{level}_filtered.jsonl.gz')
    raw_json = read_jsonl_gz_to_list(json_file_path)
    
    # Split the data based on problem id
    for single_storage in raw_json:
        pid_index = single_storage['pid']
        if pid_index in pid_split_dict.keys():
            pid_split_dict[pid_index].append((len(pid_split_dict[pid_index]), single_storage))
        else:
            pid_split_dict[pid_index] = [(0, single_storage)]
            
    # Prepare arguments for multiprocessing
    args_list = []
    for pid_index in data_list:
        pid_path = os.path.join(trace_type_path, pid_index)
        incorrect_list = os.listdir(pid_path)

        for single_incorrect in incorrect_list:
            incorrect_data = os.path.join(pid_path, single_incorrect)
            args_list.append((pid_index, incorrect_data, pid_split_dict))

    # Use multiprocessing Pool
    with Pool(120) as pool:  # Use one less CPU than available
        results = list(
            tqdm(pool.imap_unordered(process_code, args_list, chunksize=1), 
                total=len(args_list), 
                desc="Processing Codes")
        )
    
    # Add results to the list
    for result in results:
        if result is not None:
            all_trace_added_list.append(result)
            
    # Save the results
    save_dict_list_to_jsonl_gz(
        all_trace_added_list, 
        os.path.join(
            base_path, 
            'python_data', 
            f'{level}_filtered_tc_cov.jsonl.gz'))

if __name__ == '__main__':
    main()
