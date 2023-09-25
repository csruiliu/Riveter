import argparse
import time
import pandas as pd
import subprocess
import psutil
import sysv_ipc
import ctypes
import numpy as np
import matplotlib.pyplot as plt

from scipy.optimize import curve_fit

# Constants 
RAND_WRITE_SPEED = 2500
RAND_READ_SPEED = 2500
TERM_PROB = 0.7


class ProcLatencyEstimator:
    def __init__(self,
                 rand_write_speed,
                 rand_read_speed,
                 num_join_array,
                 input_cardinality_array,
                 suspension_point_array,
                 persistence_size_array):
        # rand r/w speed of hardware storage
        self.rand_write_speed = rand_write_speed
        self.rand_read_speed = rand_read_speed

        # the data for regression
        self.num_join_array = num_join_array
        self.input_cardinality_array = input_cardinality_array
        self.suspension_point_array = suspension_point_array

        # the results/label for regression
        self.persistence_size_array = persistence_size_array

        # the param estimated from regression
        self.param = None

    @staticmethod
    def func_persistence_size(para, a, b, c, d):
        result = 1 / (a * para[0] + b * para[1] + c * para[2] + d)
        return result.ravel()

    def fit_curve(self):
        assert len(self.num_join_array) == len(self.input_cardinality_array) == len(self.suspension_point_array)
        x = np.column_stack(self.num_join_array, self.input_cardinality_array, self.suspension_point_array)
        y = self.persistence_size_array
        self.param, _ = curve_fit(self.func_persistence_size, x, y)

    def persistence_size_estimation(self, num_join, input_card, suspension_point):
        return num_join * self.param[0] + input_card * self.param[1] + suspension_point * self.param[2]

    def suspend_latency_estimation(self, num_join, input_card, suspension_point):
        return self.persistence_size_estimation(num_join, input_card, suspension_point) / self.rand_write_speed

    def resume_latency_estimation(self, num_join, input_card, suspension_point):
        return self.persistence_size_estimation(num_join, input_card, suspension_point) / self.rand_read_speed


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

    parser.add_argument("-tu", "--time_unit", type=int, action="store",
                        help="indicate the time unit for moving forward when estimating latency for proc-level ")

    args = parser.parse_args()

    benchmark = args.benchmark
    qid = args.query_id
    data_folder = args.data_folder
    sloc = f"{benchmark}/{args.suspend_location}"
    ts = args.termination_start
    te = args.termination_end
    time_step = args.time_unit

    benchmark_arg = f"{benchmark}/ratchet_{benchmark}.py"
    if args.database is None:
        db_arg = f"{benchmark}/{benchmark}.db"
    else:
        db_arg = f"{benchmark}/{args.database}.db"

    ratchet_cmd = f"python3 {benchmark_arg} -q {qid} -d {db_arg} -df {data_folder} -s -sl {sloc}"

    return ratchet_cmd, ts, te, time_step


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


def func_persistence_size(x, a, b, c, d):
    r = a * x[0] + b * x[1] + c * x[2] + d
    return r.ravel()


def main():
    # Set the output format
    pd.set_option('display.float_format', '{:.1f}'.format)

    # Get start time point
    start_time = time.perf_counter()

    # Get the execution command, termination window start and end
    ratchet_cmd, ts, te, time_step = assemble_execution_cmd()

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
    num_join_array = None
    input_cardinality_array = None
    suspension_point_array = None
    persistence_size_array = None

    proc_estimator = ProcLatencyEstimator(RAND_WRITE_SPEED, RAND_READ_SPEED,
                                          num_join_array, input_cardinality_array,
                                          suspension_point_array, persistence_size_array)
    proc_estimator.fit_curve()

    suspend_time = start_time
    latency_proc_suspend = float('inf')
    latency_proc_resume = float('inf')

    while suspend_time <= te:
        suspend_time += time_step
        latency_proc_suspend_est = proc_estimator.suspend_latency_estimation(num_join, input_card, suspend_time)
        if latency_proc_suspend_est < latency_proc_suspend:
            latency_proc_suspend = latency_proc_suspend_est
        latency_proc_resume_est = proc_estimator.resume_latency_estimation(num_join, input_card, suspend_time)
        if latency_proc_resume_est < latency_proc_resume:
            latency_proc_resume = latency_proc_resume_est
    
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


if __name__ == "__main__":
    main()
