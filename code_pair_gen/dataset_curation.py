from datasets import load_dataset
from tqdm import tqdm
from multiprocessing import Pool
import subprocess
import tempfile
import json
from rapidfuzz.distance import Levenshtein as rlev
import re
import copy
import argparse
import os
import autopep8
import orjson
import multiprocessing as mp
from black import FileMode, format_file_contents
THRESHOLD = 35          # 원하는 최대 거리

_BLACK_MODE = FileMode(
    target_versions=set(),  
    line_length=88,         
)

def data_align(description, public_case, private_case, gen_case, solution, incor_solution):
    data_dict = {}
    for index, (des, pub, priv, gen, sol, incor) in enumerate(zip(description, public_case, private_case, gen_case, solution.values(), incor_solution.values())):
        data_dict[index] = {
            'description': des,
            'test_case': {
                'public': pub,
                'private': priv,
                'generated': gen
            },
            'correct': sol,
            'incorrect': incor
        }
        
    return data_dict

def write_jsonl(path, iterator):
    with open(path, 'wb') as f:
        for obj in iterator:
            f.write(orjson.dumps(obj))
            f.write(b'\n')

def remove_extra_newlines(text):
    return re.sub(r'\n\s*\n+', '\n', text)

def preprocess_cpp_code(cpp_code):
    comment_removed_code = remove_cpp_comments(cpp_code).strip()
    comment_removed_code = remove_extra_newlines(comment_removed_code)
    formatted_code = format_cpp_code(comment_removed_code)
    return formatted_code if formatted_code == None else formatted_code.strip()

def preprocess_python_code(python_code):
    comment_removed_code = remove_python_comments(python_code).strip()
    comment_removed_code = remove_extra_newlines(comment_removed_code)
    formatted_code = format_python_code(comment_removed_code)
    return formatted_code if formatted_code == None else formatted_code.strip()

def remove_cpp_comments(code):
    code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
    
    lines = code.splitlines()
    cleaned_lines = []
    
    for line in lines:
        if '//' in line:
            line = line.split('//', 1)[0].rstrip()
        
        if line.strip():
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)


def remove_python_comments(python_code):
    # Remove Multi-line comments & Single Line comments along with newlines
    code = re.sub(r'\'\'\'.*?\'\'\'|\"\"\".*?\"\"\"\s*', '',
                  python_code, flags=re.DOTALL)  # Remove multi-line comments
    # Remove single-line comments along with the newline
    code = re.sub(r'#.*\n?', '', code)
    return code


def format_cpp_code(cpp_code):
    try:
        # Create a temporary file to store the C++ code
        with tempfile.NamedTemporaryFile(suffix=".cpp", delete=False) as temp_file:
            temp_file.write(cpp_code.encode('utf-8'))
            temp_file_path = temp_file.name

        # Run clang-format with -style=google and -i
        process = subprocess.run(
            ['clang-format', '-style=google', '-i', temp_file_path],
            stderr=subprocess.PIPE
        )

        # Check if there was any error
        if process.stderr:
            print("Error occurred:", process.stderr.decode('utf-8'))
            return None

        # Read the formatted C++ code from the file
        with open(temp_file_path, 'r') as formatted_file:
            formatted_code = formatted_file.read()
            
        os.remove(temp_file_path)

        return formatted_code

    except FileNotFoundError:
        print("clang-format not found.")
        return None
    
_autopep8_fix = autopep8.fix_code


def format_python_code(code: str) -> str:

    # try:
    #     options = {
    #         'aggressive': 0,        
    #         'indent_size': 4,       
    #         'max_line_length': 79,  
    #         'experimental': False,
    #         'exclude': [],
    #         'pep8_passes': -1,      
    #         'pep8_diff': False,
    #         'python': 2,            
    #     }
    #     return autopep8.fix_code(code, options=options)
    # except Exception:
    #     return code   
    
    try:
        return format_file_contents(code, fast=True, mode=_BLACK_MODE)
    except Exception:
        return code

def find_matching_pairs(d):
    correct   = d.pop('correct')
    incorrect = d.pop('incorrect')

    inc_len = [len(code) for code in incorrect]

    pairs = []

    for c in correct:
        len_c = len(c)

        for inc_code, len_i in zip(incorrect, inc_len):
            if abs(len_c - len_i) > THRESHOLD:      
                continue

            dist = rlev.distance(c, inc_code, score_cutoff=THRESHOLD)

            if 0 < dist <= THRESHOLD:               
                pairs.append((c, inc_code))

    d['code_pair'] = pairs
    return d

def process_solution(index, pair_data, language):
    cpp_save = []
    python_save = []
    if any(lang in [2, 3] for lang in pair_data['language']):
        for (lang, sol) in zip(pair_data['language'], pair_data['solution']):
            if (lang == 2) and (language == 'cpp'):
                cpp_save.append(preprocess_cpp_code(sol))
            elif (lang == 3) and (language == 'python'):
                python_save.append(preprocess_python_code(sol))
                
    return index, cpp_save if language == 'cpp' else python_save

def process_item(item):
    (key, value), code_type = item
    return key, find_matching_pairs(value)

def make_pair_dic(data_dict, code_type, jsonl_path):
    listed_data = list(data_dict.items())             
    items       = [(single, code_type) for single in listed_data]

    pool_size   = min(mp.cpu_count() * 2, 32)

    with Pool(pool_size) as pool:
        result_iter = pool.imap_unordered(
            process_item, items, chunksize=64
        )

        with open(jsonl_path, 'wb') as fp, tqdm(
            total=len(items), desc='matching-pairs'
        ) as pbar:
            for key, result in result_iter:
                data_dict[str(key)] = result       
                fp.write(orjson.dumps({str(key): result}))
                fp.write(b'\n')
                pbar.update()

    return data_dict       

def process_solution_wrapper(args):
    return process_solution(*args)

def process_with_multiprocessing(data, language, pool_size=100, tqdm_desc="Processing"):
    results = []
    with Pool(pool_size) as pool:
        for result in tqdm(pool.imap_unordered(
                process_solution_wrapper, 
                [(index, pair_data, language) for index, pair_data in enumerate(data)]
            ), total=len(data), desc=tqdm_desc):
            results.append(result)
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type = str, help = 'cpp or python')
    parser.add_argument('--data_split', type = str, help = 'valid. test, train')
    parser.parse_args()
    args = parser.parse_args()
    
    language = args.language
    data_split = args.data_split

    ds = load_dataset("deepmind/code_contests")
        
    # Choose data by data_split
    if data_split == 'test':
        description_test = ds['test']['description']
        public_case_test = ds['test']['public_tests']
        private_case_test = ds['test']['private_tests']
        gen_case_test = ds['test']['generated_tests']
            
        correct_results = process_with_multiprocessing(ds['test']['solutions'], language=language, tqdm_desc = 'Processing Correct data')
        incorrect_results = process_with_multiprocessing(ds['test']['incorrect_solutions'], language=language, tqdm_desc = 'Processing Incorrect data')
        
        # sorting by index
        correct_results.sort(key=lambda x: x[0])
        incorrect_results.sort(key=lambda x: x[0])
                
        if language == 'cpp':
            # Save as dictionary
            cpp_test_sol = {index: solution for index, solution in correct_results}
            cpp_test_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            cpp_test = data_align(description_test, public_case_test,
                            private_case_test, gen_case_test, cpp_test_sol, cpp_test_incor_sol)
            print(f'# of C++ test data description: {len(cpp_test)}')
            
            # Compare paires and save json files
            pair_cpp_test = make_pair_dic(cpp_test, 1)
            with open('./cpp_data/cpp_test_refine.json', 'w') as f:
                json.dump(pair_cpp_test, f, indent=4)
                
        elif language == 'python':
            # Save as dictionary
            python_test_sol = {index: solution for index, solution in correct_results}
            python_test_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            python_test = data_align(description_test, public_case_test, private_case_test,
                            gen_case_test, python_test_sol, python_test_incor_sol)
            print(f'# of Python test data description: {len(python_test)}')
            
            # Compare paires and save json files
            pair_python_test = make_pair_dic(python_test,
                                                2,
                                                f'./python_data/python_test_refine_{THRESHOLD}.jsonl'
                                                )
                
    elif data_split == 'valid':
        description_valid = ds['valid']['description']
        public_case_valid = ds['valid']['public_tests']
        private_case_valid = ds['valid']['private_tests']
        gen_case_valid = ds['valid']['generated_tests']
            
        correct_results = process_with_multiprocessing(ds['valid']['solutions'], language=language, tqdm_desc = 'Processing Correct data')
        incorrect_results = process_with_multiprocessing(ds['valid']['incorrect_solutions'], language=language, tqdm_desc = 'Processing Incorrect data')
        
        # sorting by index
        correct_results.sort(key=lambda x: x[0])
        incorrect_results.sort(key=lambda x: x[0])
                
        if language == 'cpp':
            # Save as dictionary
            cpp_valid_sol = {index: solution for index, solution in correct_results}
            cpp_valid_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            cpp_valid = data_align(description_valid, public_case_valid,
                            private_case_valid, gen_case_valid, cpp_valid_sol, cpp_valid_incor_sol)
            print(f'# of C++ valid data description: {len(cpp_valid)}')
            
            # Compare paires and save json files
            pair_cpp_valid = make_pair_dic(cpp_valid, 1)
            with open('./cpp_data/cpp_valid_refine.json', 'w') as f:
                json.dump(pair_cpp_valid, f, indent=4)
                
        elif language == 'python':
            # Save as dictionary
            python_valid_sol = {index: solution for index, solution in correct_results}
            python_valid_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            python_valid = data_align(description_valid, public_case_valid, private_case_valid,
                            gen_case_valid, python_valid_sol, python_valid_incor_sol)
            print(f'# of Python valid data description: {len(python_valid)}')
            
            # Compare paires and save json files
            pair_python_valid = make_pair_dic(python_valid,
                                              2,
                                              f'./python_data/python_valid_refine_{THRESHOLD}.jsonl'
                                              )

    elif data_split == 'train':
        description_train = ds['train']['description']
        public_case_train = ds['train']['public_tests']
        private_case_train = ds['train']['private_tests']
        gen_case_train = ds['train']['generated_tests']
            
        correct_results = process_with_multiprocessing(ds['train']['solutions'], language=language, tqdm_desc = 'Processing Correct data')
        incorrect_results = process_with_multiprocessing(ds['train']['incorrect_solutions'], language=language, tqdm_desc = 'Processing Incorrect data')
        
        # sorting by index
        correct_results.sort(key=lambda x: x[0])
        incorrect_results.sort(key=lambda x: x[0])
                
        if language == 'cpp':
            # Save as dictionary
            cpp_train_sol = {index: solution for index, solution in correct_results}
            cpp_train_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            cpp_train = data_align(description_train, public_case_train,
                            private_case_train, gen_case_train, cpp_train_sol, cpp_train_incor_sol)
            print(f'# of C++ train data description: {len(cpp_train)}')
            
            # Compare paires and save json files
            pair_cpp_train = make_pair_dic(cpp_train, 1)
            with open('./cpp_data/cpp_train_refine.json', 'w') as f:
                json.dump(pair_cpp_train, f, indent=4)
                
        elif language == 'python':
            # Save as dictionary
            python_train_sol = {index: solution for index, solution in correct_results}
            python_train_incor_sol = {index: solution for index, solution in incorrect_results}
            
            # Make paires
            python_train = data_align(description_train, public_case_train, private_case_train,
                            gen_case_train, python_train_sol, python_train_incor_sol)
            print(f'# of Python train data description: {len(python_train)}')
            
            # Compare paires and save json files
            pair_python_train = make_pair_dic(
                python_train, 
                2,
                f'./python_data/python_train_refine_{THRESHOLD}.jsonl'
            )

if __name__ == '__main__':
    main()