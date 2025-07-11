from __future__ import annotations

import argparse
import copy
import gzip
import json
import multiprocessing as mp
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple

from tqdm import tqdm

BASE_CODE_DIR: Path = Path("./python_code")
BASE_ERR_DIR: Path = Path("./python_error")
BASE_CODE_DIR.mkdir(parents=True, exist_ok=True)
BASE_ERR_DIR.mkdir(parents=True, exist_ok=True)

def read_jsonl_gz(fn: Path) -> List[Dict[str, Any]]:
    """gzip-압축 JSONL => List[dict]"""
    with gzip.open(fn, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def save_jsonl_gz(objs: List[Dict[str, Any]], fn: Path) -> None:
    """List[dict] => gzip-압축 JSONL 저장"""
    fn.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(fn, "wt", encoding="utf-8") as f:
        for obj in objs:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_code_file(path: Path, code: str) -> None:
    """잘못된(raw_incorrect) 파이썬 코드를 파일로 저장"""
    path.write_text(code, encoding="utf-8")


def _normalize_stdin(raw: str) -> str:
    """
    trace_code 에서 뽑아온 입력 문자열에는 \\n, \\t 같은
    '이스케이프 시퀀스'가 글자로 들어있다.
    -> 실제 제어문자로 변환.

    예)  "1 2\\n3 4"  =>  "1 2\n3 4"
    """
    try:
        norm = codecs.decode(raw, "unicode_escape")
    except Exception:
        norm = raw  # 못 바꾸면 원본 그대로
    if not norm.endswith("\n"):
        norm += "\n"
    return norm


def run_python(file_path: str, stdin: str, timeout: int = 20) -> str:
    """
    `python file_path` 를 실행하고 stdout(또는 stderr)을 문자열로 돌려준다.
    - stdin 은 trace 에서 추출한 뒤 _normalize_stdin() 으로 보정.
    - 예외 상황에서도 항상 str 반환 => 후속 로직 안전.
    """
    # stdin = _normalize_stdin(stdin)

    try:
        proc = subprocess.run(
            ["python", file_path],
            input=stdin,
            text=True,
            capture_output=True,
            errors="replace",
            timeout=timeout,
        )
        # stdout 이 비어 있으면 stderr 라도 돌려준다 (종종 print 가 아닌 예외 메시지만 있는 경우)
        output = proc.stdout if proc.stdout.strip() else proc.stderr
        return output.rstrip("\n")
    except subprocess.TimeoutExpired as e:
        return (e.stdout or "").rstrip("\n")
    except Exception as exc:
        err_log = BASE_ERR_DIR / "exec_error.log"
        print(f"Error executing {file_path}: {exc}", file=sys.stderr)
        return ""

def process_sample(
    args: Tuple[
        Dict[str, Any],  
        str,             
        str,             
        str,             
        List[str],       
    ]
) -> Dict[str, Any]:
    single, test_input, header_str, var_trace, stmts = args

    py_path = BASE_CODE_DIR / f"python_{single['pid']}_{single['code_index']}.py"
    write_code_file(py_path, single["raw_incorrect"])

    actual_output = run_python(str(py_path), test_input)
    actual_output = actual_output.replace("\n", " ").replace("\t", " ").strip()

    exp_part = (
        header_str.split("@Expected = [", 1)[1]
        .split("]", 1)[0]
        .replace("\n", " ")
        .strip()
    )

    io_prefix = header_str.split("@Expected = [", 1)[0]
    io_full = f"{io_prefix}@Expected = [{exp_part}] @Actual = [{actual_output}]"

    gold_lines = ", ".join(re.findall(r"^\d+", "\n".join(stmts), flags=re.M))

    new_data = copy.deepcopy(single)
    new_data["input_expected_actual"] = io_full
    new_data["input_expected_actual_gold"] = f"{io_full} @Location = [{gold_lines}]"
    new_data["input_expected_actual_trace"] = f"{io_full} @Trace = {var_trace}"

    with open(f'./python_error/{single["pid"]}.txt', "w", encoding='utf-8') as err_log_file:
        err_log_file.write(f"INPUT:\n{repr(test_input)}\nOUTPUT:\n{repr(actual_output)}\n{repr(io_full)}\n{repr(new_data['input_expected_actual_trace'])}")
    return new_data


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("single", "all"),
        default="all",
        help="저장 파일 이름 분기용 태그",
    )
    parser.add_argument(
        '--data_type',
        default='hard',
        type=str)
    args = parser.parse_args()
    
    global data_type
    data_type = args.data_type

    DATA_PATH: Path = Path(f"./python_data/{data_type}_filtered_tc_cov.jsonl.gz")

    raw_records = read_jsonl_gz(DATA_PATH)
    tasks: List[
        Tuple[Dict[str, Any], str, str, str, List[str]]
    ] = []  # (sample, input, header, var_trace, statements)

    for rec in raw_records:
        trace_str: str = rec["trace_code"]

        # Input / Expected / Trace 분리
        test_input = trace_str.split("# @Input = [", 1)[-1].split("] @Expected = [", 1)[0]
        header_str = trace_str.split("] @Trace = ", 1)[0]
        var_trace = trace_str.split("] @Trace = ", 1)[-1]

        tasks.append((rec, test_input, header_str, var_trace, rec["statement"]))

    cpu_cnt = max(1, min(32, os.cpu_count() or 1))
    results: List[Dict[str, Any]] = []
    with mp.Pool(cpu_cnt) as pool:
        for item in tqdm(
            pool.imap_unordered(process_sample, tasks, chunksize=16),
            total=len(tasks),
            ncols=70,
            desc="Processing",
        ):
            results.append(item)

    out_name = (
        f"{data_type}_filtered_single_tc.jsonl.gz"
        if args.mode == "single"
        else f"{data_type}_filtered_all_tc.jsonl.gz"
    )
    save_jsonl_gz(results, Path("./python_data") / out_name)

    print(f"\nOriginal : {len(tasks):,}")
    print(f"Saved    : {len(results):,}")


if __name__ == "__main__":
    main()
