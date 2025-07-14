from datasets import load_dataset
import ast
import json
import copy
import gzip
from typing import List, Any

YOUR_HF_TOKEN = 'xxxx'  # Replace with your Hugging Face token
taco = load_dataset('BAAI/TACO', token=YOUR_HF_TOKEN)
train_data = taco['train']
test_data = taco['test']

def save_list_to_jsonl_gz(data_list, file_path):
    """
    Saves a list of dictionaries to a gzipped JSONL file.
    """
    with gzip.open(file_path, 'wt', encoding='utf-8') as f:
        for item in data_list:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    print(f"Saved {len(data_list)} items to {file_path}")

def str_list_to_list(list_str: str) -> List[Any]:
    try:
        data = ast.literal_eval(list_str)  # 파이썬 리터럴 전용 파서(안전함)
        if isinstance(data, list):
            return data
        raise ValueError("주어진 문자열이 리스트 리터럴이 아닙니다.")
    except (SyntaxError, ValueError) as e:
        raise ValueError(f"리스트 변환 실패: {e}") from None
    
test_data_list = []
train_data_list = []

code_contest_count = 0
proper_count = 0

difficulty_dict = {}
codechef_count = 0
codeforces_count = 0
for single_data in test_data:
    source = single_data['source']
    if source.strip() == 'codechef':
        codechef_count += 1
    elif source.strip() == 'codeforces':
        codeforces_count += 1
    else:
        continue
    
    if single_data['difficulty'] not in difficulty_dict:
        difficulty_dict[single_data['difficulty']] = 0
    difficulty_dict[single_data['difficulty']] += 1

    save_dict = {
        'dataset' : source.strip(),
        'question': single_data['question'], 
        'tags': single_data['tags'], 
        'skill_types': single_data['skill_types'], 
        'url': single_data['url'], 
        'input_output': single_data['input_output'], 
        'solutions': single_data['solutions'], 
        'difficulty': single_data['difficulty']
    }
    test_data_list.append(copy.deepcopy(save_dict))
            
for single_data in train_data:
    source = single_data['source']
    if source.strip() == 'codechef':
        codechef_count += 1
    elif source.strip() == 'codeforces':
        codeforces_count += 1
    else:
        continue
    
    if single_data['difficulty'] not in difficulty_dict:
        difficulty_dict[single_data['difficulty']] = 0
    difficulty_dict[single_data['difficulty']] += 1
    save_dict = {
        'dataset' : source.strip(),
        'question': single_data['question'], 
        'tags': single_data['tags'], 
        'skill_types': single_data['skill_types'], 
        'url': single_data['url'], 
        'input_output': single_data['input_output'], 
        'solutions': single_data['solutions'], 
        'difficulty': single_data['difficulty']
    }
    train_data_list.append(copy.deepcopy(save_dict))

print(f'Codechef Count: {codechef_count}')
print(f'Codeforces Count: {codeforces_count}')

print(len(test_data_list))
print(len(train_data_list))

print(json.dumps(difficulty_dict, indent=4, ensure_ascii=False))
total_key = 0
for value in difficulty_dict.values():
    total_key += value
print(f'Total Codeforces Questions: {total_key}')

save_list_to_jsonl_gz(test_data_list, 'taco_test.jsonl.gz')
save_list_to_jsonl_gz(train_data_list, 'taco_train.jsonl.gz')
