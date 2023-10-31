import argparse
import subprocess
import ctypes
import sysv_ipc
import json
import yaml
import toml
import time
import pandas as pd
import numpy as np

from pathlib import Path
from datetime import datetime

from cost_model import ProcLatencyEstimator, PipelineLatencyEstimator

# Constants
RAND_WRITE_SPEED = 2500
RAND_READ_SPEED = 2500
CRIU_CMD="/opt/criu/sbin/criu"
CKPT_PATH="./criu-ckpt"


class PropertyUtils:
    @staticmethod
    def load_property_file(properties_file):
        if isinstance(properties_file, str):
            properties_file = Path(properties_file)

        with open(properties_file) as fp:
            if properties_file.suffix == ".json":
                properties = json.load(fp)

            elif properties_file.suffix == ".yaml":
                properties = yaml.load(fp, Loader=yaml.SafeLoader)

            elif properties_file.suffix == ".toml":
                properties = toml.load(fp)
            else:
                raise ValueError(f'Error loading {properties_file.name}. {properties_file.suffix} is not supported')

        return properties


def observe_term_point(term_start, term_end, term_prob):
    np.random.seed(int(datetime.now().timestamp()))
    if term_prob == 1:
        term_point = round(np.random.uniform(term_start, term_end, 1).flat[0])
        print(f"== The query execution will be terminated at {term_point} within [{term_start}, {term_end}] ==")

    else:
        random_dice = np.random.rand()
        # This mainly checks if the current pipeline breaker is already over termination
        if random_dice > term_prob:
            term_point = round(np.random.uniform(term_start, term_end, 1).flat[0])
            print(f"== The query execution will be terminated at {term_point} within [{term_start}, {term_end}] ==")
        else:
            term_point = np.inf
            print("== The query execution will not be terminated ==")

    return term_point


def get_current_term_prob(term_end, current_time, term_prob):
    if term_prob == 1:
        return (term_end - current_time) / term_end
    else:
        return term_prob


def demo_proc_latency_estimation():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ef", "--estimation_file", type=str, action="store", required=True,
                        help="indicate the file stored historical data for estimation")
    args = parser.parse_args()
    estimation_file = args.estimation_file

    start = time.perf_counter()

    # Load json file for estimation
    query_json = PropertyUtils.load_property_file(properties_file=estimation_file)
    query_executions = query_json["query_executions"]

    num_join_list = list()
    num_groupby_list = list()
    input_card_list = list()
    suspension_point_list = list()
    persist_size_list = list()

    for qe in query_executions:
        num_join_list.append(qe["num_join"])
        num_groupby_list.append(qe["num_groupby"])
        input_card_list.append(qe["input_cardinality"])
        suspension_point_list.append(qe["suspension_point"])
        persist_size_list.append(qe["persistence_size"])

    ple = ProcLatencyEstimator(RAND_WRITE_SPEED, RAND_READ_SPEED,
                               num_join_list, num_groupby_list,
                               input_card_list, suspension_point_list, persist_size_list)
    ple.fit_curve()

    # features for estimation
    input_num_join = 1
    input_num_groupby = 2
    input_cardinality = 75100030
    input_suspension_point = 0.5

    persisted_data_size = ple.persist_size_estimation(input_num_join,
                                                      input_num_groupby,
                                                      input_cardinality,
                                                      input_suspension_point)

    print("Estimated Persisted Data Size: {}".format(persisted_data_size))
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))


def demo_e2e():
    parser = argparse.ArgumentParser()
    # Options for query execution
    parser.add_argument("-b", "--benchmark", type=str, action="store", required=True,
                        choices=["vanilla", "tpch", "tpcds"],
                        help="indicate the benchmark for evaluation")
    parser.add_argument("-q", "--query_id", type=str, action="store", required=True,
                        help="indicate the query id")
    parser.add_argument("-d", "--database", type=str, action="store", default="memory",
                        help="indicate the database location, memory or other location")
    parser.add_argument("-df", "--data_folder", type=str, action="store", required=True,
                        help="indicate the TPC-H dataset for queries, such as <dataset/tpch/parquet-sf1>")
    parser.add_argument("-tmp", "--tmp_folder", type=str, action="store", default="tmp",
                        help="indicate the tmp folder for DuckDB, such as <exp/tmp>")
    parser.add_argument("-pl", "--persistence_location", type=str, action="store",
                        help="indicate the persisted data or folder during suspension and resumption")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads for query execution")

    # Options for cost model estimation
    parser.add_argument("-nj", "--number_join", type=int, action="store",
                        help="indicate the number of join operator in a query plan")
    parser.add_argument("-ng", "--number_groupby", type=int, action="store",
                        help="indicate the cardinality of input dataset")
    parser.add_argument("-ic", "--input_cardinality", type=int, action="store",
                        help="indicate the cardinality of input dataset")
    parser.add_argument("-ef", "--estimation_file", type=str, action="store", required=True,
                        help="indicate the file stored historical data for estimation")

    # Options for termination time window
    parser.add_argument("-ts", "--termination_start", type=float, action="store",
                        help="indicate the start point of termination time window (second)")
    parser.add_argument("-te", "--termination_end", type=float, action="store",
                        help="indicate the end point of termination time window (second)")
    parser.add_argument("-tp", "--termination_prob", type=float, action="store",
                        help="indicate the probability of termination happened in the window")
    parser.add_argument("-tu", "--time_unit", type=int, action="store",
                        help="indicate the time unit for moving forward when estimating latency for proc-level")
    args = parser.parse_args()

    # Get options for query execution
    benchmark = args.benchmark
    qid = args.query_id
    database = args.database
    input_data = args.data_folder
    tmp = args.tmp_folder
    ploc = f"{benchmark}/{args.persistence_location}"
    td = args.thread

    # Get options for cost model estimation
    num_join = args.number_join
    num_groupby = args.number_groupby
    input_card = args.input_cardinality
    estimation_file = args.estimation_file

    # Get options for termination time window
    term_start = args.termination_start
    term_end = args.termination_end
    term_prob = args.termination_prob
    time_unit = args.time_unit

    # Get benchmark python command
    benchmark_arg = f"{benchmark}/ratchet_{benchmark}.py"

    # Observe the termination for query execution
    term_point = observe_term_point(term_start, term_end, term_prob)

    # CMD of query execution with potential suspension
    # The execution only need to notify Riveter when it reaches pipeline breaker and transfer some variables through SHM
    exec_cmd = f"python3 {benchmark_arg} -q {qid} -d {database} -df {input_data} -tmp {tmp} -td {td} -pl {ploc}"

    # Mark the start time of query execution
    execution_start = time.perf_counter()

    # Execute the query through subprocess
    ratchet_proc = subprocess.Popen([exec_cmd], shell=True)

    # Match the keyfile with the C++ program
    shm_cost_model_flag_keyfile = "/tmp/shm_cost_model_flag_keyfile"
    shm_strategy_keyfile = "/tmp/shm_strategy_keyfile"
    shm_persistence_size_keyfile = "/tmp/shm_persistence_size_keyfile"

    # Get shared memory key based on the name of keyfile
    shm_cost_model_flag_key = ctypes.CDLL(None).ftok(shm_cost_model_flag_keyfile.encode(), ord('R'))
    shm_strategy_key = ctypes.CDLL(None).ftok(shm_strategy_keyfile.encode(), ord('R'))
    shm_persistence_size_key = ctypes.CDLL(None).ftok(shm_persistence_size_keyfile.encode(), ord('R'))

    # it seems like time.sleep() is necessary
    time.sleep(0.5)

    shm_cost_model_flag = sysv_ipc.SharedMemory(shm_cost_model_flag_key, ctypes.sizeof(ctypes.c_uint16), 0 | 0o666)
    shm_strategy = sysv_ipc.SharedMemory(shm_strategy_key, ctypes.sizeof(ctypes.c_uint16), 0 | 0o666)
    shm_persistence_size = sysv_ipc.SharedMemory(shm_persistence_size_key, ctypes.sizeof(ctypes.c_uint64), 0 | 0o666)

    # Wait until the query execution reach a pipeline breaker and mark `cost_model_flag = 1`
    while True:
        execution_check = time.perf_counter()

        # Convert share memory to variables
        cost_model_flag = ctypes.c_uint16.from_buffer(shm_cost_model_flag).value
        persistence_size = ctypes.c_uint64.from_buffer(shm_persistence_size).value

        if cost_model_flag == 1:
            print("[Python] Cost model is running...")
            print(f"[Python] Size of intermediate states: {persistence_size}")
            break

        time.sleep(0.5)

    ##########################
    # Cost Model Preparation
    ##########################
    # Get the execution time of reaching current
    pipeline_breaker = time.perf_counter()
    current_exec_time = pipeline_breaker - execution_start

    # Check if the query should be terminated
    if current_exec_time > term_point:
        print("The query has been terminated")
        exit(0)

    # Create an estimator for latency of pipeline-level strategy
    ppl_estimator = PipelineLatencyEstimator(persistence_size, RAND_WRITE_SPEED, RAND_READ_SPEED)

    # Load json file for estimation
    query_json = PropertyUtils.load_property_file(properties_file=estimation_file)
    query_executions = query_json["query_executions"]

    # Create an estimator for latency of process-level strategy
    num_join_list = list()
    num_groupby_list = list()
    input_card_list = list()
    suspension_point_list = list()
    persist_size_list = list()

    for qe in query_executions:
        num_join_list.append(qe["num_join"])
        num_groupby_list.append(qe["num_groupby"])
        input_card_list.append(qe["input_cardinality"])
        suspension_point_list.append(qe["suspension_point"])
        persist_size_list.append(qe["persistence_size"])

    proc_estimator = ProcLatencyEstimator(RAND_WRITE_SPEED, RAND_READ_SPEED,
                                          num_join_list, num_groupby_list,
                                          input_card_list, suspension_point_list, persist_size_list)
    proc_estimator.fit_curve()

    ######################
    # Cost Model Decision
    ######################
    # Calculate redo strategy cost
    cost_redo = get_current_term_prob(term_end, current_exec_time, term_prob) * current_exec_time

    # Calculate pipeline-level strategy cost
    latency_ppl_suspend = ppl_estimator.suspend_latency_estimation()
    latency_ppl_resume = ppl_estimator.resume_latency_estimation()
    if current_exec_time + latency_ppl_suspend > term_start:
        prob_ppl_term = get_current_term_prob(term_end, current_exec_time, term_prob)
    else:
        prob_ppl_term = 0
    cost_ppl = latency_ppl_suspend + latency_ppl_resume + (prob_ppl_term * current_exec_time)

    # Calculate process-level strategy cost
    # Probe the "best" proc suspension time based on latency
    cost_proc = np.inf
    proc_suspension_probe = current_exec_time
    proc_suspension_point = current_exec_time
    while proc_suspension_probe <= term_end:
        latency_proc_suspend_est = proc_estimator.suspend_latency_estimation(num_join, num_groupby,
                                                                             input_card, proc_suspension_probe)

        latency_proc_resume_est = proc_estimator.resume_latency_estimation(num_join, num_groupby,
                                                                           input_card, proc_suspension_probe)

        if current_exec_time + latency_proc_suspend_est > term_start:
            prob_proc_term = get_current_term_prob(term_end, current_exec_time, term_prob)
        else:
            prob_proc_term = 0

        cost_proc_new = latency_proc_suspend_est + latency_proc_resume_est + (prob_proc_term * proc_suspension_probe)

        if cost_proc_new < cost_proc:
            cost_proc = cost_proc_new
            proc_suspension_point = proc_suspension_probe

        proc_suspension_probe += time_unit

    cost_list = [cost_redo, cost_proc, cost_ppl]
    strategy_id = np.where(cost_list == np.min(cost_list))[0]

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

    if strategy_id == 1:
        print("[Python] Using Redo Suspension")
        # Wait until the process is finished
        ratchet_proc.wait()
        return_code = ratchet_proc.returncode
        print(f'[Python] Ratchet exited with return code: {return_code}')
    elif strategy_id == 2:
        print("[Python] Using Pipeline-level Suspension")
        # Wait until the process is finished
        ratchet_proc.wait()
        return_code = ratchet_proc.returncode
        print(f'[Python] Ratchet exited with return code: {return_code}')
    else:
        print("[Python] Using Process-level Suspension")
        time.sleep(proc_suspension_point)
        proc_suspend_cmd = f"sudo {CRIU_CMD} dump -D {CKPT_PATH}/ckpt_{qid}_{ratchet_proc.pid} -t {ratchet_proc.pid} --file-locks --shell-job"
        # Trigger process-level suspension through subprocess
        proc_suspend = subprocess.Popen([proc_suspend_cmd], shell=True)

    shm_cost_model_flag.detach()
    shm_strategy.detach()
    shm_persistence_size.detach()
    print(f'Detached shared memory variable from Ratchet')


def main():
    # Set the output format
    pd.set_option('display.float_format', '{:.1f}'.format)

    # Demo for Estimating Latency of Process-level Strategy
    demo_proc_latency_estimation()

    # Demo for End-to-End Pipeline
    demo_e2e()


if __name__ == "__main__":
    main()
