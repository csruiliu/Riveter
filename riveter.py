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

from cost_model import ProcLatencyEstimator, PipelineLatencyEstimator

# Constants
RAND_WRITE_SPEED = 2500
RAND_READ_SPEED = 2500
TERM_PROB = 0.7


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
                        help="indicate the time unit for moving forward when estimating latency for proc-level")
    parser.add_argument("-nj", "--number_join", type=int, action="store",
                        help="indicate the number of join operator in a query plan")
    parser.add_argument("-ng", "--number_groupby", type=int, action="store",
                        help="indicate the cardinality of input dataset")
    parser.add_argument("-ic", "--input_cardinality", type=int, action="store",
                        help="indicate the cardinality of input dataset")
    parser.add_argument("-ef", "--estimation_file", type=str, action="store", required=True,
                        help="indicate the file stored historical data for estimation")
    args = parser.parse_args()

    benchmark = args.benchmark
    qid = args.query_id
    data_folder = args.data_folder
    sloc = f"{benchmark}/{args.suspend_location}"
    ts = args.termination_start
    te = args.termination_end
    time_step = args.time_unit
    num_join = args.number_join
    num_groupby = args.number_groupby
    input_card = args.input_cardinality
    estimation_file = args.estimation_file

    benchmark_arg = f"{benchmark}/ratchet_{benchmark}.py"
    if args.database is None:
        db_arg = f"{benchmark}/{benchmark}.db"
    else:
        db_arg = f"{benchmark}/{args.database}.db"

    # Get start time point
    e2e_start_time = time.perf_counter()

    ratchet_cmd = f"python3 {benchmark_arg} -q {qid} -d {db_arg} -df {data_folder} -s -sl {sloc}"

    # Execute the query through subprocess
    ratchet_proc = subprocess.Popen([ratchet_cmd], shell=True)

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

    while True:
        # Convert share memory to variables
        cost_model_flag = ctypes.c_uint16.from_buffer(shm_cost_model_flag).value
        persistence_size = ctypes.c_uint64.from_buffer(shm_persistence_size).value

        if cost_model_flag == 1:
            print("[Python] Cost model is running...")
            print(f"[Python] Size of intermediate states: {persistence_size}")
            break

        time.sleep(0.5)

    # Get pipeline suspension time
    ppl_suspension_time = time.perf_counter()

    ppl_estimator = PipelineLatencyEstimator(persistence_size, RAND_WRITE_SPEED, RAND_READ_SPEED)
    latency_ppl_suspend = ppl_estimator.suspend_latency_estimation()
    latency_ppl_resume = ppl_estimator.resume_latency_estimation()

    # Load json file for estimation
    query_json = PropertyUtils.load_property_file(properties_file=estimation_file)
    query_executions = query_json["query_executions"]

    # Create an estimator for process latency estimation
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

    proc_suspension_time = ppl_suspension_time
    latency_proc_suspend = float('inf')
    latency_proc_resume = float('inf')

    # Probe the "best" proc suspension time based on latency
    while proc_suspension_time <= te:
        proc_suspension_time += time_step
        latency_proc_suspend_est = proc_estimator.suspend_latency_estimation(num_join, num_groupby,
                                                                             input_card, proc_suspension_time)
        if latency_proc_suspend_est < latency_proc_suspend:
            latency_proc_suspend = latency_proc_suspend_est

        latency_proc_resume_est = proc_estimator.resume_latency_estimation(num_join, num_groupby,
                                                                           input_card, proc_suspension_time)
        if latency_proc_resume_est < latency_proc_resume:
            latency_proc_resume = latency_proc_resume_est

    ######################
    # Cost Model Decision
    ######################
    cost_redo = TERM_PROB * ppl_suspension_time
    if ppl_suspension_time + latency_ppl_suspend > ts:
        prob_ppl_term = TERM_PROB
    else:
        prob_ppl_term = 0
    cost_ppl = latency_ppl_suspend + latency_ppl_resume + (prob_ppl_term * ppl_suspension_time)

    if proc_suspension_time + latency_proc_suspend > ts:
        prob_proc_term = TERM_PROB
    else:
        prob_proc_term = 0
    cost_proc = latency_proc_suspend + latency_proc_resume + (prob_proc_term * proc_suspension_time)
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
    print("[Python] Using Pipeline-level Suspension")

    # Wait until the process is finished
    ratchet_proc.wait()
    return_code = ratchet_proc.returncode
    print(f'[Python] Ratchet exited with return code: {return_code}')

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
