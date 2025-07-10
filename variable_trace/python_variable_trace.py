from types import FrameType, TracebackType
from debuggingbook.StackInspector import StackInspector
from io import TextIOBase as TextIO
from tqdm import tqdm
from datetime import datetime
from multiprocessing import Pool
from typing import Callable, Optional, TextIO, Any
from contextlib import redirect_stdout, redirect_stderr

import threading
import re
import sys
import os
import json
import builtins
import copy
import types
import io
import gzip
import ast
import signal
import argparse


class MaxTraceOrderExceededException(Exception):
    """Exception raised when the trace order exceeds the maximum allowed."""
    pass

class TimeoutException(Exception):
    pass

class Tracer:
    def __init__(self, *, path, user_def_function, file: TextIO = sys.stdout, max_trace_order: int = 3000,
                 timeout: int = 10) -> None:
        self.original_trace_function: Optional[Callable] = None
        self.file = file
        self.file_path = path
        self.trace_data = {}
        self.trace_order = 0
        self.user_def_function = user_def_function
        self.max_trace_order = max_trace_order
        self.timeout = timeout

    def traceit(self, frame: FrameType, event: str, arg: Any) -> None:
        if self.trace_order >= self.max_trace_order:
            self.save_json()
            self.compress_json_file()
            raise Exception(f"Trace order exceeded the maximum limit of {self.max_trace_order}.")

        self.user_def_function.append("trace_func")
        # self.user_def_function.append("<lambda>")

        copied_locals = {}
        for key, value in frame.f_locals.items():
            try:
                json.dumps(value, cls=CustomEncoder)  # Check if JSON serializable
                copied_locals[key] = copy.deepcopy(value)
            except TypeError:
                copied_locals[key] = str(value)  # Convert to string if not serializable

        if frame.f_code.co_name in self.user_def_function:
            self.trace_order += 1
            self.trace_data[self.trace_order] = {'event': event,
                                                 'function': frame.f_code.co_name,
                                                 'line': frame.f_lineno,
                                                 'variables': copied_locals}

    def _traceit(self, frame: FrameType, event: str, arg: Any) -> Callable:
        self.traceit(frame, event, arg)
        return self._traceit

    def compress_json_file(self):
        if not os.path.exists(self.file_path):
            print(f"File {self.file_path} does not exist.")
            return

        compressed_file_path = f"{self.file_path}.gz"
        with open(self.file_path, 'rb') as f_in, gzip.open(compressed_file_path, 'wb') as f_out:
            f_out.writelines(f_in)

        os.remove(self.file_path)
        print(f"Compressed {self.file_path} to {compressed_file_path} and removed the original file.")

    def save_json(self):
        with open(self.file_path, 'w') as f:
            json.dump(self.trace_data, f, indent=4, cls=CustomEncoder)

    def timeout_handler(self, signum, frame):
        raise TimeoutException("The block of code took too long to execute.")

    def __enter__(self):
        self.original_trace_function = sys.gettrace()
        sys.settrace(self._traceit)

        if self.timeout is not None and threading.active_count() == 1:
            signal.signal(signal.SIGALRM, self.timeout_handler)
            signal.alarm(self.timeout)

        return self

    def __exit__(self, exc_tp, exc_value: BaseException, exc_traceback: Any):
        sys.settrace(self.original_trace_function)

        if self.timeout is not None:
            signal.alarm(0)  # Disable the alarm

        # Save JSON and compress file before exiting
        self.save_json()
        self.compress_json_file()

        # Reraise exceptions if they are not internal errors
        if exc_tp is not None:
            if isinstance(exc_value, TimeoutException):
                print("Timeout occurred during tracing.")
            return False  # Re-raise exception
        return None  # All ok


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, types.ModuleType):
            return f"Module: {obj.__name__}"
        elif isinstance(obj, types.FunctionType):
            return f"Function: {obj.__name__}"
        elif isinstance(obj, types.MethodType):
            return f"Method: {obj.__name__}"
        elif callable(obj):
            return f"Callable: {obj}"
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return str(obj)


class MockInput:
    def __init__(self, inputs, read_line):
        self.inputs = inputs
        self.index = 0
        self.check_read_line = read_line
        if self.check_read_line:
            self.inputs = "\n".join(inputs) + "\n"

    def input(self, prompt=None):
        if self.index < len(self.inputs):
            response = self.inputs[self.index]
            self.index += 1
            return response
        else:
            raise ValueError("No more input data available")


def _make_stdin(inputs) -> io.TextIOWrapper:
    raw_bytes = ("\n".join(inputs) + "\n").encode()
    byte_stream = io.BytesIO(raw_bytes)
    text_stream = io.TextIOWrapper(byte_stream, encoding='utf-8')
    return text_stream


def create_function_from_file(code):
    read_line = False
    func_code = f"def trace_func():\n"
    for line in code.splitlines():
        if 'open(0)' in line:
            line = line.replace('open(0)', 'input()')
        if ('stdin.readline' in line) or ('stdin.buffer.readline' in line):
            read_line = True
        func_code += "    " + line + "\n"

    func_dict = {}
    try:
        exec(func_code, globals(), func_dict)
    except SyntaxError as e:
        return False, None
    return read_line, func_dict['trace_func']


def extract_definitions(code):
    function_pattern = re.compile(r'^\s*def\s+(\w+)\s*\(', re.MULTILINE)
    class_pattern = re.compile(r'^\s*class\s+(\w+)\s*:', re.MULTILINE)

    user_def = []

    functions = function_pattern.findall(code)
    classes = class_pattern.findall(code)

    user_def = functions + classes

    return user_def


def trace_variable(inputs, function_curated, file_path, read_line, user_def_function):
    original_input = builtins.input

    original_stdout, original_stderr = sys.stdout, sys.stderr
    with open(os.devnull, "w") as dn, \
            redirect_stdout(dn), redirect_stderr(dn):

        sys.stdin = _make_stdin(inputs)

        # For code that uses input() directly we still install MockInput
        # so prompt-less reads behave exactly as before
        if not read_line:
            mock_input = MockInput(inputs, read_line)
            builtins.input = mock_input.input

        try:
            with Tracer(path=file_path, user_def_function=user_def_function, timeout=50):
                function_curated()

        except MaxTraceOrderExceededException as e:

            file_name = file_path.split('/')[3]
            split_list = file_path.split('.json')[0].split('_')

            try:
                os.makedirs(f'./python_error/{data_type}/{file_name}', exist_ok=True)
            except FileExistsError:
                pass
            with open(f"./python_error/{data_type}/{file_name}/{split_list[-4]}_error_{split_list[-3]}_{split_list[-2]}_{split_list[-1]}.txt",
                      'a') as total_txt:
                total_txt.write(f'{file_path} : MaxTraceOrderExceededException\n')

        except Exception as e:

            file_name = file_path.split('/')[3]
            split_list = file_path.split('.json')[0].split('_')
            try:
                os.makedirs(f'./python_error/{data_type}/{file_name}', exist_ok=True)
            except FileExistsError:
                pass
            with open(f"./python_error/{data_type}/{file_name}/{split_list[-4]}_error_{split_list[-3]}_{split_list[-2]}_{split_list[-1]}.txt",
                      'a') as total_txt:
                total_txt.write(f'{file_path} : {e}\n')

        finally:
            # Restore stdout, stderr, and input
            sys.stdout, sys.stderr = original_stdout, original_stderr
            sys.stdin = original_input
            builtins.input = original_input


def read_jsonl_gz_to_list(jsonl_file):
    data_list = []
    with gzip.open(jsonl_file, 'rt', encoding='utf-8') as file:
        for line in file:
            data_list.append(json.loads(line))
    return data_list


def trace_code_pair(args):
    code, input_data, filename, is_correct = args
    # user_def_function = extract_definitions(code)
    user_def_function = []
    read_line, function_gen = create_function_from_file(code)
    if function_gen is not None:
        for input_index, inputs in enumerate(input_data):
            code_input = inputs.split('\n')
            code_filepath = f"{filename}_{input_index}.json"
            trace_variable(code_input, function_gen, code_filepath, read_line, user_def_function)


def setup_tracing(data, is_correct):
    pid_save = {}
    for single_code_data in data:

        pid = single_code_data['pid']
        test_case_input = single_code_data['test_case']['input']

        try:
            if is_correct:
                os.makedirs(f'./python_trace/{data_type}/python_correct/{pid}', exist_ok=True)
            else:
                os.makedirs(f'./python_trace/{data_type}/python_incorrect/{pid}', exist_ok=True)
        except FileExistsError:
            pass

        tasks = []

        correct_code = single_code_data['raw_correct']
        incorrect_code = single_code_data['raw_incorrect']

        if pid not in pid_save.keys():
            pid_save[pid] = 0
        elif pid in pid_save.keys():
            pid_save[pid] = pid_save[pid] + 1

        if is_correct:
            filename = f'./python_trace/{data_type}/python_correct/{pid}/python_correct_{pid}_{pid_save[pid]}'
            tasks.append((correct_code, test_case_input, filename, is_correct))
        else:
            filename = f'./python_trace/{data_type}/python_incorrect/{pid}/python_incorrect_{pid}_{pid_save[pid]}'
            tasks.append((incorrect_code, test_case_input, filename, is_correct))

        yield tasks

def make_folders():
    base_dirs = [
        f'./python_error',
        f'./python_error/{data_type}',
        f'./python_error/{data_type}/python_incorrect',
        f'./python_trace',
        f'./python_trace/{data_type}',
        f'./python_trace/{data_type}/python_incorrect'
    ]

    for dir_path in base_dirs:
        os.makedirs(dir_path, exist_ok=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_type", default='very_hard', type=str)
    args = parser.parse_args()
    dt = args.data_type

    global data_type
    data_type = dt

    data_path = f'./python_data/{data_type}_filtered.jsonl.gz'
    input_data = read_jsonl_gz_to_list(data_path)

    print(len(input_data))
    make_folders()

    with Pool(120) as pool:
        # correct_tasks = list(setup_tracing(input_data, is_correct=True))
        incorrect_tasks = list(setup_tracing(input_data, is_correct=False))

        # Flatten task list
        # all_tasks = [task for sublist in (correct_tasks + incorrect_tasks) for task in sublist]
        all_tasks = [task for sublist in incorrect_tasks for task in sublist]

        # Process tasks
        list(tqdm(pool.imap_unordered(trace_code_pair, all_tasks), total=len(all_tasks)))


if __name__ == '__main__':
    main()
