import json
import gzip
import random

def read_jsonl_to_list(file_path):
    data_list = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            data_list.append(json.loads(line.strip()))
    return data_list
THRESHOLD = 50 

data_type = ['test', 'train', 'valid']
total_data = {}
for dt in data_type:
    data_path = f'python_{dt}_refine_{THRESHOLD}.jsonl'
    data_list = read_jsonl_to_list(data_path)
    for single_data in data_list:
        for key, value in single_data.items():
            total_data[f'{dt}_{key}'] = value

final_data = []
for key, value in total_data.items():
    save_dict = {}
    if len(value['code_pair']) != 0:
        save_dict['pid'] = key
        save_dict['code_pair'] = value['code_pair']
        test_case = value['test_case']
        test_case_list = []
        
        public_tc = test_case['public']
        private_tc = test_case['private']
        generated_tc = test_case['generated']
        
        public_input, public_output = public_tc['input'], public_tc['output']
        private_input, private_output = private_tc['input'], private_tc['output']
        generated_input, generated_output = generated_tc['input'], generated_tc['output']
        
        if (public_input != 0) and (public_output != 0):
            for pub_s_in, pub_s_out in zip(public_input, public_output):
                test_case_list.append((pub_s_in, pub_s_out))
                
        if (private_input != 0) and (private_output != 0):
            for priv_s_in, priv_s_out in zip(private_input, private_output):
                test_case_list.append((priv_s_in, priv_s_out))
                
        if (generated_input != 0) and (generated_output != 0):
            for gen_s_in, gen_s_out in zip(generated_input, generated_output):
                test_case_list.append((gen_s_in, gen_s_out))
                
        save_dict['test_case'] = test_case_list
        final_data.append(save_dict)

with gzip.open('raw_deepmind_check.jsonl.gz', 'wt', encoding='utf-8') as f:
    for item in final_data:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')
print(f'Total data saved: {len(final_data)}')
print(f'Saved to raw_deepmind_check.jsonl.gz')