import argparse
import time
import pandas as pd
import subprocess
import psutil
import sysv_ipc
import ctypes
import numpy as np

from scipy.optimize import curve_fit

RAND_WRITE_SPEED = 2500
RAND_READ_SPEED = 2500
TERM_PROB = 0.7


class ProcLatencyEstimator:
    def __init__(self, rand_write_speed, rand_read_speed, num_join, input_cardinality, current_time, end_time):
        self.rand_write_speed = rand_write_speed
        self.rand_read_speed = rand_read_speed
        self.num_join = num_join
        self.input_cardinality = input_cardinality
        self.current_time = current_time
        self.end_time = current_time

    def func_persistence_size(self, x1, x2, x3, A, B, C):
        return 1 / (A * x1 + B * x2 + C * x3 + B)

    def fit_curve(self):
        opt, cov = curve_fit(self.func_persistence_size, x1, x2, x3, y)


class PipelineLatencyEstimator:
    def __init__(self, persistence_size, rand_write_speed, rand_read_speed):
        self.persistence_size = persistence_size
        self.rand_write_speed = rand_write_speed
        self.rand_read_speed = rand_read_speed

    def suspend_latency_estimation(self):
        return self.persistence_size / self.rand_write_speed

    def resume_latency_estimation(self):
        return self.persistence_size / self.rand_read_speed


def profile_hardware():
    hw_threads_num = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory()[0] / 1000000000
    available_memory_gb = psutil.virtual_memory()[1] / 1000000000

    print(f"Number of Hardware Threads: {hw_threads_num}")
    print(f"Total Memory Size (GB): {total_memory_gb}")
    print(f"Available Memory Size (GB): {available_memory_gb}")

    return hw_threads_num, total_memory_gb, available_memory_gb


def assemble_execution_cmd():
    parser = argparse.ArgumentParser()

    parser.add_argument("-b", "--benchmark", type=str, action="store", required=True,
                        choices=["vanilla", "tpch", "tpcds"],
                        help="indicate the benchmark for evaluation")

    parser.add_argument("-q", "--query_id", type=str, action="store", required=True,
                        help="indicate the query id")

    parser.add_argument("-d", "--database", type=str, action="store", default="memory",
                        help="indicate the database location, memory or other location")

    parser.add_argument("-df", "--data_folder", type=str, action="store", required=True,
                        help="indicate the TPC-H dataset for Vanilla Queries, such as <exp/dataset/tpch/parquet-sf1>")

    parser.add_argument("-tmp", "--tmp_folder", type=str, action="store", default="tmp",
                        help="indicate the tmp folder for DuckDB, such as <exp/tmp>")

    parser.add_argument("-sl", "--suspend_location", type=str, action="store",
                        help="indicate the file or folder for suspending query")

    parser.add_argument("-ts", "--termination_start", type=int, action="store",
                        help="indicate the starting point of termination time window")

    parser.add_argument("-te", "--termination_end", type=int, action="store",
                        help="indicate the starting point of termination time window")

    args = parser.parse_args()

    benchmark = args.benchmark
    qid = args.query_id
    data_folder = args.data_folder
    sloc = f"{benchmark}/{args.suspend_location}"
    ts = args.termination_start
    te = args.termination_end

    benchmark_arg = f"{benchmark}/ratchet_{benchmark}.py"
    if args.database is None:
        db_arg = f"{benchmark}/{benchmark}.db"
    else:
        db_arg = f"{benchmark}/{args.database}.db"

    ratchet_cmd = f"python3 {benchmark_arg} -q {qid} -d {db_arg} -df {data_folder} -s -sl {sloc}"

    return ratchet_cmd, ts, te


def get_shm_variable():
    # Match the keyfile with the C++ program
    shm_cost_model_flag_keyfile = "/tmp/shm_cost_model_flag_keyfile"
    shm_strategy_keyfile = "/tmp/shm_strategy_keyfile"
    shm_persistence_size_keyfile = "/tmp/shm_persistence_size_keyfile"

    # Get shared memory key based on the name of keyfile
    shm_cost_model_flag_key = ctypes.CDLL(None).ftok(shm_cost_model_flag_keyfile.encode(), ord('R'))
    shm_strategy_key = ctypes.CDLL(None).ftok(shm_strategy_keyfile.encode(), ord('R'))
    shm_persistence_size_key = ctypes.CDLL(None).ftok(shm_persistence_size_keyfile.encode(), ord('R'))

    # it seems like time.sleep(1) is necessary
    time.sleep(0.5)

    shm_cost_model_flag = sysv_ipc.SharedMemory(shm_cost_model_flag_key, ctypes.sizeof(ctypes.c_uint16), 0 | 0o666)
    shm_strategy = sysv_ipc.SharedMemory(shm_strategy_key, ctypes.sizeof(ctypes.c_uint16), 0 | 0o666)
    shm_persistence_size = sysv_ipc.SharedMemory(shm_persistence_size_key, ctypes.sizeof(ctypes.c_uint64), 0 | 0o666)

    return shm_cost_model_flag, shm_strategy, shm_persistence_size


def detach_shm_variable(*args):
    for v in args:
        v.detach()
    print(f'Detached shared memory variable from Ratchet')


def latency_proc_estimation(num_join, persistence_size):
    pass


def cost_model(pt, end_time, current_time,
               latency_ppl_suspend,
               latency_ppl_resume,
               latency_proc_suspend,
               latency_proc_resume):
    cost_redo = pt * end_time
    cost_ppl = (1 - pt) * (latency_ppl_suspend + latency_ppl_resume) + (pt * current_time)
    cost_proc = (1 - pt) * (latency_proc_suspend + latency_proc_resume) + pt * current_time
    cost_list = [cost_redo, cost_proc, cost_ppl]

    return np.where(cost_list == np.min(cost_list))[0]


def main():
    # Set the output format
    pd.set_option('display.float_format', '{:.1f}'.format)

    # Get start time point
    start_time = time.perf_counter()

    # Get the execution command, termination window start and end
    ratchet_cmd, ts, te = assemble_execution_cmd()

    # Execute the query through subprocess
    ratchet_proc = subprocess.Popen([ratchet_cmd], shell=True)

    # Get shared memory for IPC
    shm_cost_model_flag, shm_strategy, shm_persistence_size = get_shm_variable()

    while True:
        # Convert share memory to variables
        cost_model_flag = ctypes.c_uint16.from_buffer(shm_cost_model_flag).value
        persistence_size = ctypes.c_uint64.from_buffer(shm_persistence_size).value

        if cost_model_flag == 1:
            print("[Python] Cost model is running...")
            print(f"[Python] Size of intermediate states: {persistence_size}")
            break

        time.sleep(0.5)

    # Get end time point
    end_time = time.perf_counter()
    # Get time duration
    time_duration = end_time - start_time

    # Create an estimator for pipeline latency estimation
    ppl_estimator = PipelineLatencyEstimator(persistence_size, RAND_WRITE_SPEED, RAND_READ_SPEED)
    latency_ppl_suspend = ppl_estimator.suspend_latency_estimation()
    latency_ppl_resume = ppl_estimator.resume_latency_estimation()

    # Create an estimator for process latency estimation

    latency_proc_suspend = 0
    latency_proc_resume = 0

    strategy_id = cost_model(TERM_PROB, end_time, time_duration,
                             latency_ppl_suspend, latency_ppl_resume,
                             latency_proc_suspend, latency_proc_resume) + 1

    # 1: Redo Strategy
    # 2: Process-level Strategy
    # 3: Pipeline-level Strategy
    print(f"[Python] Select Strategy: {strategy_id}")

    cost_model_flag_new = 0
    cost_model_flag_to_send = cost_model_flag_new.to_bytes(ctypes.sizeof(ctypes.c_uint16), byteorder='little')
    shm_cost_model_flag.write(cost_model_flag_to_send)
    print("[Python] Cost model is finished")

    strategy_new = strategy_id
    strategy_to_send = strategy_new.to_bytes(ctypes.sizeof(ctypes.c_uint16), byteorder='little')
    shm_strategy.write(strategy_to_send)
    print("[Python] Using Pipeline-level Suspension")

    # Wait until the process is finished
    ratchet_proc.wait()
    return_code = ratchet_proc.returncode
    print(f'[Python] Ratchet exited with return code: {return_code}')

    detach_shm_variable(shm_cost_model_flag, shm_strategy, shm_persistence_size)

    '''
    ratchet_cmd = 'python3 ./vanilla/ratchet_vanilla.py -q q1 -d vanilla/vanilla.db -df dataset/tpch/parquet-tiny -tmp tmp -s -st 0 -se 0 -sl vanilla/sum.ratchet'    
    '''


if __name__ == "__main__":
    main()
