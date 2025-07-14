from __future__ import annotations
import argparse, concurrent.futures as cf, gzip, json, os, sys, tempfile, uuid, multiprocessing as mp
from collections import deque
from math import isclose
from typing import Dict, List, Tuple
from tqdm import tqdm
import wandb  

def read_jsonl_gz_to_dict_list(p: str) -> List[dict]:
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return [json.loads(l) for l in fh]

def save_jsonl_gz(data: List[dict], path: str):
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for row in data:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"\nSaved {len(data)} items => {path}", file=sys.stderr)

def is_float(s: str) -> bool:
    try: 
        float(s)
        return True
    except ValueError: 
        return False

def compare_outputs(a: str, e: str) -> bool:
    yes_no = {"yes","no"}
    a_list, e_list = a.strip().split(), e.strip().split()
    if len(a_list) != len(e_list): 
        return False
    for x, y in zip(a_list,e_list):
        if is_float(x) and is_float(y):
            if abs(float(x)-float(y)) > 1e-6*max(abs(float(x)), abs(float(y))):
                return False
        elif x.lower() in yes_no and y.lower() in yes_no:
            if x.lower() != y.lower(): return False
        else:
            if x!=y: return False
    return True

def _run_python(pid:int, idx:int, src:str, stdin:str, limit:int, tmp:str)->str:
    import subprocess, textwrap
    fp = os.path.join(
        tmp,
        f"{pid}_{idx}_{uuid.uuid4().hex}.py"
        )
    with open(fp, "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(src))
    try:
        res=subprocess.run([sys.executable,fp],
                           input=stdin,text = True,
                           capture_output = True,
                           timeout = limit,
                           errors = "replace")
        return res.stdout
    except subprocess.TimeoutExpired as e:
        return e.stdout or ""
    finally:
        os.remove(fp)

def _evaluate_pair(task:Tuple[int,int,str,str,List[str],List[str],int,str]
                   )->Tuple[int,int,Dict[str,List[int]],Dict[str,List[int]]]:
    pid, idx, cor, inc, ins, outs, lim, tmp=task
    def collect(code:str):
        p, f = [], []
        for i, stdin in enumerate(ins):
            ok = compare_outputs(
                _run_python(pid,idx,code,stdin,lim,tmp).strip().replace("\\n"," ").replace("\\t"," "),
                outs[i].strip().replace("\\n"," ").replace("\\t"," ")
                )
            if ok:p.append(i)
            else: f.append(i)
        return {"pass":p,"fail":f}
    return pid, idx, collect(cor), collect(inc)

def meets_filter(cor: Dict[str,List[int]], 
                 inc: Dict[str,List[int]]) -> bool:
    correct_pass = set(cor['pass'])
    incorrect_pass = set(inc['pass'])
    cor_pass_incor_fail = set(inc["fail"])
    return len(correct_pass) >= cp and len(incorrect_pass) >= ip and len(cor_pass_incor_fail) >= i_f

def build_item(pid:int, 
               idx:int, 
               cor_r:Dict, 
               inc_r:Dict, 
               row:dict) -> dict:
    t_io = json.loads(row["taco_input_output"])
    if row["test_case"]:
        ins,outs = zip(*row["test_case"])
        all_in = list(ins)+t_io["inputs"]
        all_out = list(outs)+t_io["outputs"]
    else:
        all_in, all_out = t_io["inputs"], t_io["outputs"]

    incor_pass = list(set(inc_r["pass"]))[:ip]
    common_fail = list(set(inc_r["fail"]))[:i_f]

    tc={
        "input":[all_in[i] for i in common_fail]+[all_in[i] for i in incor_pass],
        "output":[all_out[i] for i in common_fail]+[all_out[i] for i in incor_pass]
        }

    cor_src, inc_src = row["code_pair"][idx]
    cor_lines=[f"{n+1} {l.strip()}" for n,l in enumerate(cor_src.splitlines())]
    inc_lines=[f"{n+1} {l.strip()}" for n,l in enumerate(inc_src.splitlines())]
    mod=[]
    
    for i in range(max(len(cor_lines),len(inc_lines))):
        if i>=len(cor_lines): 
            mod.append(f"{i+1} ")
        elif i>=len(inc_lines) or cor_lines[i]!=inc_lines[i]: mod.append(cor_lines[i])

    return {
        "description":row["question"],
        "pid":row["pid"],
        "test_case":tc,
        "raw_correct":cor_src,
        "raw_incorrect":inc_src,
        "correct_code":" ||| ".join(cor_lines),
        "incorrect_code":" ||| ".join(inc_lines),
        "statement":mod,
    }

def main()->None:
    ap=argparse.ArgumentParser()
    ap.add_argument("--language",default="python")
    ap.add_argument("--threshold",type=int,default=10)
    ap.add_argument("--level",default="very_hard")
    ap.add_argument("--workers",type=int,default=mp.cpu_count())
    ap.add_argument("--target_size",type=int,default=500)
    ap.add_argument("--timeout",type=int,default=10)
    ap.add_argument('--cp', type=int, default=20)
    ap.add_argument('--ip', type=int, default=3)
    ap.add_argument('--i_f', type=int, default=3)
    args=ap.parse_args()

    global cp
    global ip
    global i_f
    
    cp = args.cp
    ip = args.ip
    i_f = args.i_f
    
    base=os.getcwd()
    src = os.path.join(
        base,
        f"{args.language}_data/{args.language}_raw_deepmind_{args.threshold}_{args.level}.jsonl.gz"
        )
    rows = read_jsonl_gz_to_dict_list(src)
    print(f"[✓] Loaded {len(rows)} raw problems from {src}")
    tmp_root = tempfile.mkdtemp(prefix="eval_tmp_")

    pid_iters = {
        pid:iter(enumerate(r["code_pair"])) for pid,r in enumerate(rows)
        }
    
    io_cache = {}
    
    for pid, r in enumerate(rows):
        t = json.loads(r["taco_input_output"])
        if r["test_case"]:
            ins,outs=zip(*r["test_case"])
            io_cache[pid] = (list(ins) + t["inputs"], list(outs) + t["outputs"])
        else:
            io_cache[pid] = (t["inputs"], t["outputs"])

    rr_queue:deque[int]=deque(pid_iters.keys())
    inflight:set[int]=set()
    finished:set[int]=set()
    next_idx:Dict[int,int]={pid:0 for pid in pid_iters}

    final_items=[]; fut_to_pid={}
    ctx=mp.get_context("spawn")
    with cf.ProcessPoolExecutor(max_workers=args.workers,mp_context=ctx) as pool:

        def submit(pid:int):
            if pid in inflight or pid in finished: 
                return
            try:
                idx, (cor, inc) = next(pid_iters[pid])
                while idx < next_idx[pid]:
                    idx, (cor, inc) = next(pid_iters[pid])
            except StopIteration:
                return
            ins, outs = io_cache[pid]
            tqdm.write(f"[SUBMIT] PID={pid:<4} idx={idx:<3} queued",file=sys.stderr)
            f = pool.submit(_evaluate_pair, 
                            (pid,idx,cor,inc,ins,outs,args.timeout,tmp_root)
                            )
            fut_to_pid[f]=pid
            inflight.add(pid)

        while rr_queue and len(inflight) < args.workers:
            submit(rr_queue.popleft())

        bar = tqdm(
            total=args.target_size,
            desc="Collecting",
            ncols=80
            )
        try:
            while (rr_queue or inflight) and len(final_items) < args.target_size:
                done, _ = cf.wait(
                    fut_to_pid.keys(),
                    return_when = cf.FIRST_COMPLETED
                    )
                for fut in done:
                    pid=fut_to_pid.pop(fut)
                    inflight.discard(pid)
                    try:
                        pid_, idx_, cor_r, inc_r = fut.result()
                    except Exception as e:
                        # tqdm.write(f"[!] Worker error PID={pid}: {e}",file=sys.stderr)
                        continue

                    cor_p, cor_f = len(cor_r["pass"]), len(cor_r["fail"])
                    inc_p, inc_f = len(inc_r["pass"]), len(inc_r["fail"])
                    passed = meets_filter(cor_r,inc_r)
                    tqdm.write(f"[DONE]   PID={pid_:<4} idx={idx_:<3} "
                               f"cor {cor_p}/{cor_f} | inc {inc_p}/{inc_f} -> "
                               f"{'PASS' if passed else 'fail'}",file=sys.stderr)

                    if passed:
                        final_items.append(build_item(pid_,idx_,cor_r,inc_r,rows[pid_]))
                        finished.add(pid_); bar.update(1)
                    else:
                        next_idx[pid_]+=1
                        rr_queue.append(pid_)

                    while rr_queue and len(inflight) < args.workers:
                        submit(rr_queue.popleft())
        finally:
            procs = list(pool._processes.values())  
            pool.shutdown(wait=False, cancel_futures=True)

            for p in procs:
                if p.is_alive():
                    p.terminate()      

            for p in procs:
                p.join()

    out=os.path.join(base,f"{args.language}_data/{args.level}_filtered.jsonl.gz")
    save_jsonl_gz(final_items,out)

if __name__=="__main__":
    mp.freeze_support(); main()
