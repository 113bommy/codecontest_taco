from datasets import load_dataset
import gzip as gz
import json
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt

def read_jsonl_gz_to_list(data_path):
    """
    Reads a gzipped JSONL file and returns a list of dictionaries.
    """
    import gzip
    data = []
    with gzip.open(data_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def read_json_to_list(data_path):
    """
    Reads a JSON file and returns a list of dictionaries.
    """
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def save_dict_list_to_jsonl_gz(data, data_path):
    """
    Saves a list of dictionaries to a gzipped JSONL file.
    """
    with gz.open(data_path, 'wt', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def read_jsonl_gz_to_list(data_path):
    """
    Reads a gzipped JSONL file and returns a list of dictionaries.
    """
    data = []
    with gz.open(data_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data

total_data = {}
dataset = load_dataset("deepmind/code_contests")
dt = ['train', 'valid', 'test']
target_data = 0
for d in dt:
    data_type = dataset[d]
    code_contests_data = {}
    dummy_description = None
    for index, single_data in tqdm(enumerate(data_type)):
        source = single_data['source']
        assert type(source) == int, f"Source should be an integer, got {type(source)}"
        if (source == 2) or (source == 1) :
            target_data += 1
            save_dict = {
                'name': single_data['name'],
                'description': single_data['description'],
                'source': single_data['source'],
                'difficulty': single_data['difficulty'],
                'cf_contest_id': single_data['cf_contest_id'],
                'cf_index': single_data['cf_index'],
                'p_index': f'{d}_{index}',
                'incorrect_length': single_data['incorrect_solutions']['language'].count(3),
                'correct_length': single_data['solutions']['language'].count(3),
            }
            if source == 2:
                code_contests_data[f'{single_data["cf_contest_id"]}_{single_data["cf_index"]}'] = save_dict
            elif source == 1:
                code_contests_data[single_data['name'].lower().strip()] = save_dict
                
    # Save Codeforces data to a JSON file
    with open(f'codeforces_{d}.json', 'w', encoding='utf-8') as f:
        json.dump(code_contests_data, f, ensure_ascii=False, indent=4)
    print(f"Processed {d} data: {len(code_contests_data)} entries")
    for key, value in code_contests_data.items():
        if key in total_data.keys():
            raise ValueError(f"Duplicate key found: {key}")
        total_data[key] = value
print(len(total_data.keys()))
print(f"Total target data: {target_data}")


"""
남겨야 할 데이터
description
source
difficulty
cf_contest_id
cf_index
위 4가지 데이터를 따로 저장해서 code contest matching information 생성
"""

"""
TACO 데이터셋에서 필요한 정보를 불러옴,
URL을 바탕으로 어떤 index에 해당하는 것인지 파악해야 함.
"""

taco_test_path = 'taco_test.jsonl.gz'
taco_train_path = 'taco_train.jsonl.gz'
taco_test_data = read_jsonl_gz_to_list(taco_test_path)
taco_train_data = read_jsonl_gz_to_list(taco_train_path)
taco_data = taco_test_data + taco_train_data
print(len(taco_data))
print(len(taco_test_data))
print(len(taco_train_data))

code_contest_dict = {}

for single_taco in taco_data:
    if single_taco['dataset'] == 'codeforces':
        url = single_taco['url']
        url = url.split('/')
        level = url[-1]
        cid = url[-2]
        key = f'{cid}_{level}'
    else:
        url = single_taco['url']
        problem_id = url.split('problems/')[-1]
        key = problem_id.lower().strip()

    if key in total_data.keys():
        pid = total_data[key]['p_index']
        code_contest_dict[pid] = {
            'taco_question': single_taco['question'],
            'taco_tags': single_taco['tags'],
            'taco_skill_types': single_taco['skill_types'],
            'taco_input_output': single_taco['input_output'],
            'taco_difficulty': single_taco['difficulty'],
            'incorrect_length': total_data[key]['incorrect_length'],
            'correct_length': total_data[key]['correct_length']
        }
        
print(len(code_contest_dict.keys()))

diff_dict = {}
for single_code in code_contest_dict.values():
    if single_code['taco_difficulty'] not in diff_dict:
        diff_dict[single_code['taco_difficulty']] = {
            'incorrect_length': [],
            'correct_length': []
        }
    diff_dict[single_code['taco_difficulty']]['incorrect_length'].append(single_code['incorrect_length'])
    diff_dict[single_code['taco_difficulty']]['correct_length'].append(single_code['correct_length'])
    
print(f"Total Code Contest PIDs: {len(code_contest_dict.keys())}")
print(f'plotting diff_dict: {diff_dict.keys()}')


# Plotting the difficulty distribution
for difficulty, lengths in diff_dict.items():
    plt.figure()
    plt.title(f"Difficulty: {difficulty}")
    plt.boxplot([lengths['incorrect_length'], lengths['correct_length']], labels=['Incorrect', 'Correct'])
    plt.ylabel("Length")
    plt.savefig(f"boxplot_{difficulty}.png")
    plt.close()

total_data_path = 'raw_deepmind_check.jsonl.gz'

our_single_data = read_jsonl_gz_to_list(total_data_path)
single_data = []

for ss_data in our_single_data:
    pid = ss_data['pid']
    if pid in code_contest_dict.keys():
        ss_data['taco_skill_types'] = code_contest_dict[pid]['taco_skill_types']
        ss_data['taco_difficulty'] = code_contest_dict[pid]['taco_difficulty']
        ss_data['taco_tags'] = code_contest_dict[pid]['taco_tags']
        ss_data['taco_input_output'] = code_contest_dict[pid]['taco_input_output']
        ss_data['question'] = code_contest_dict[pid]['taco_question']
        single_data.append(ss_data)

print(len(code_contest_dict.keys()))
print(len(our_single_data))
print(len(single_data))

THRESHOLD = 50

save_dict_list_to_jsonl_gz(single_data, f'./python_raw_deepmind_{THRESHOLD}.jsonl.gz')
print(f"Data saved to python_raw_deepmind_{THRESHOLD}.jsonl.gz")
print("Code contest matching information generation completed.")

