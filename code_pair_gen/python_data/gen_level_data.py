import json
import gzip
import random

THRESHOLD = 50

def read_jsonl_gz_to_list(data_path):
    """
    Reads a gzipped JSONL file and returns a list of dictionaries.
    """
    data = []
    with gzip.open(data_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def save_dict_list_to_jsonl_gz(data, data_path):
    """
    Saves a list of dictionaries to a gzipped JSONL file.
    """
    with gzip.open(data_path, 'wt', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

single_data_path = f'./python_raw_deepmind_{THRESHOLD}.jsonl.gz'
single_data = read_jsonl_gz_to_list(single_data_path)

pid_list = []

no_repeat_pid_dict = {}
for s_data in single_data:
    pid = s_data['pid']
    problem_type = s_data['taco_difficulty']
    if pid not in pid_list:
        pid_list.append(pid)
        if problem_type not in no_repeat_pid_dict:
            no_repeat_pid_dict[problem_type] = []
        no_repeat_pid_dict[problem_type].append(s_data)
print(f'pid_list: {len(pid_list)}')
print(f'no_repeat_pid_list: {len(no_repeat_pid_dict.keys())}')

for key, value in no_repeat_pid_dict.items():
    print(f'Problem Type: {key}, Data Size: {len(value)}')
    skill_types_list = {}
    for item in value:
        if item['taco_skill_types'] not in skill_types_list:
            skill_types_list[item['taco_skill_types']] = []
        skill_types_list[item['taco_skill_types']].append(item['pid'])
    print(f'Skill Types: {len(skill_types_list)}')
    for skill_type, pids in skill_types_list.items():
        print(f'Skill Type: {skill_type}, PIDs: {len(pids)}')
    
total_data = []

"""
Very Hard 18
Hard 200
Medium Hard 300
Medium 300
Easy 182
"""

random.seed(42)  # For reproducibility
total_data.extend(random.sample(no_repeat_pid_dict['VERY_HARD'], 200))
total_data.extend(random.sample(no_repeat_pid_dict['HARD'], 200))
total_data.extend(random.sample(no_repeat_pid_dict['MEDIUM_HARD'], 200))
total_data.extend(random.sample(no_repeat_pid_dict['MEDIUM'], 200))
total_data.extend(random.sample(no_repeat_pid_dict['EASY'], 200))

print(f'Total Data Size: {len(total_data)}')


very_hard_path = f'./python_raw_deepmind_{THRESHOLD}_very_hard.jsonl.gz'
hard_path = f'./python_raw_deepmind_{THRESHOLD}_hard.jsonl.gz'
medium_hard_path = f'./python_raw_deepmind_{THRESHOLD}_medium_hard.jsonl.gz'
medium_path = f'./python_raw_deepmind_{THRESHOLD}_medium.jsonl.gz'
easy_path = f'./python_raw_deepmind_{THRESHOLD}_easy.jsonl.gz'
output_path = f'./python_raw_deepmind_{THRESHOLD}_sample_1000.jsonl.gz'

save_dict_list_to_jsonl_gz(no_repeat_pid_dict['VERY_HARD'], very_hard_path)
save_dict_list_to_jsonl_gz(no_repeat_pid_dict['HARD'], hard_path)
save_dict_list_to_jsonl_gz(no_repeat_pid_dict['MEDIUM_HARD'], medium_hard_path)
save_dict_list_to_jsonl_gz(no_repeat_pid_dict['MEDIUM'], medium_path)
save_dict_list_to_jsonl_gz(no_repeat_pid_dict['EASY'], easy_path)
save_dict_list_to_jsonl_gz(total_data, output_path)
print(f'Saved very hard data to {very_hard_path}')
print(f'Saved hard data to {hard_path}')
print(f'Saved medium hard data to {medium_hard_path}')
print(f'Saved medium data to {medium_path}')
print(f'Saved easy data to {easy_path}')        