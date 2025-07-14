# 📦 Google DeepMind Code Contest + TACO Dataset

---

## 1️⃣ Code Pair 생성

- **Code Formatting**: `Black` 포맷터 사용
- **Code Pairing 기준**: Edit Distance ≤ **35**
- **Code Pair Filtering 조건**:
  - `cor_pass ∩ incor_fail` ≥ 5  
  - `cor_pass ∩ incor_pass` ≥ 5

### ✅ 실행 방법

```bash
cd code_pair_gen

# 데이터셋 전처리
python dataset_curation.py

# TACO 교집합 데이터 생성
cd ./python_data
python process_taco.py
python process_deepmind.py
python process_intersection.py
python gen_level_data.py

# Edit Distance & Test Case기반 필터링
cd ..
python dataset_filter.py --threshold 50 --level <level>

사용 가능한 <level> 값:
easy, medium, medium_hard, hard, very_hard
```

## 2️⃣ 변수 Trace 생성
### 📁 준비
```bash 
cd ../variable_trace
cp ../code_pair_gen/python_data/*_filtered.jsonl.gz ./python_data
```
### ✅ 실행
```bash
python python_variable_trace.py --data_type <level>
python python_gen_trace_added_data.py --level <level>
```

## 3️⃣ Actual Output 생성 및 최종 정제
### 📁 준비

```bash
cd ../actual_output_gen
cp ../variable_trace/python_data/*_filtered_tc_cov.jsonl.gz ./python_data
```

```bash
python save_actual_output.py --data_type <level>
python python_data_filter.py --data_type <level>
python python_gen_final.py --level <level>
```

## 📌 최종 데이터 저장 경로
`<level>_data.jsonl.gz`