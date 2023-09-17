# import duckdb
import argparse
import time
import pandas as pd
import subprocess
import psutil
import signal
import os
import sys
import mmap
import sysv_ipc
import ctypes
from multiprocessing.shared_memory import SharedMemory

# from vanilla.queries import *


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)

    hw_threads_num = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory()[0] / 1000000000
    available_memory_gb = psutil.virtual_memory()[1] / 1000000000
    
    print(f"Number of Hardware Threads: {hw_threads_num}")
    print(f"Total Memory Size (GB): {total_memory_gb}")
    print(f"Available Memory Size (GB): {available_memory_gb}")

    ratchet_cmd = 'python3 ./vanilla/ratchet_vanilla.py -q q1 -d vanilla/vanilla.db -df dataset/tpch/parquet-tiny -tmp tmp -s -st 0 -se 0 -sl vanilla/sum.ratchet'

    ratchet_proc = subprocess.Popen([ratchet_cmd], shell=True)

    # Match the keyfile with the C++ program
    shm_cost_model_flag_keyfile = "/tmp/shm_cost_model_flag_key"
    # shared_memory_size = ctypes.sizeof(ctypes.c_uint64)

    # Get shared memory key based on the name of keyfile
    shm_cost_model_flag_key = ctypes.CDLL(None).ftok(shm_cost_model_flag_keyfile.encode(), ord('R'))
    print(f"[Python] Cost Model Flag Key: {shm_cost_model_flag_key}")

    # it seems like time.sleep(1) is necessary
    time.sleep(1)

    shm_cost_model_flag = sysv_ipc.SharedMemory(shm_cost_model_flag_key, ctypes.sizeof(ctypes.c_uint16), 0 | 0o666)
    cost_model_flag = ctypes.c_int.from_buffer(shm_cost_model_flag).value

    print(f"[Python] Cost Model Flag: {cost_model_flag}")

    cost_model_flag = 0

    data_bytes = cost_model_flag.to_bytes(ctypes.sizeof(ctypes.c_uint16), byteorder='little')
    shm_cost_model_flag.write(data_bytes)

    ratchet_proc.wait()
    return_code = ratchet_proc.returncode
    print(f'Ratchet exited with return code: {return_code}')

    shm_cost_model_flag.detach()
    print(f'Detached shared memory variable from Ratchet')

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", type=str, action="store", required=True,
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 
                                 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22',
                                 'q23', 'q24', 'q25', 'q26', 'q27', 'q28', 'q29', 'q30', 'q31', 'q32', 'q33',
                                 'q34', 'q35', 'q36', 'q37', 'q38', 'q39', 'q40', 'q41', 'q42', 'q43', 'q44',
                                 'q45', 'q46', 'q47', 'q48', 'q49', 'q50', 'q51', 'q52', 'q53', 'q54', 'q55',
                                 'q56', 'q57', 'q58', 'q59', 'q60', 'q61', 'q62', 'q63', 'q64', 'q65', 'q66',
                                 'q67', 'q68', 'q69', 'q70', 'q71', 'q72', 'q73', 'q74', 'q75', 'q76', 'q77',
                                 'q78', 'q79', 'q80', 'q81', 'q82', 'q83', 'q84', 'q85', 'q86', 'q87', 'q88',
                                 'q89', 'q90', 'q91', 'q92', 'q93', 'q94', 'q95', 'q96', 'q97', 'q98', 'q99'],
                        help="indicate the query id")

    parser.add_argument("-d", "--database", type=str, action="store", required=True, default="memory",
                        help="indicate the database location, memory or other location")

    parser.add_argument("-df", "--data_folder", type=str, action="store", required=True,
                        help="indicate the TPC-H dataset for Vanilla Queries, such as <exp/dataset/tpch/parquet-sf1>")

    parser.add_argument("-ut", "--update_table", action="store_true",
                        help="force to update table in database")

    parser.add_argument("-tmp", "--tmp_folder", type=str, action="store", required=True,
                        help="indicate the tmp folder for DuckDB, such as <exp/tmp>")

    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")

    parser.add_argument("-s", "--suspend_query", action="store_true", default=False,
                        help="whether it is a suspend query")
    parser.add_argument("-st", "--suspend_start_time", type=float, action="store",
                        help="indicate start time for suspension (second)")
    parser.add_argument("-se", "--suspend_end_time", type=float, action="store",
                        help="indicate end time for suspension (second)")
    parser.add_argument("-sl", "--suspend_location", type=str, action="store",
                        help="indicate the file or folder for suspending query")

    parser.add_argument("-r", "--resume_query", action="store_true", default=False,
                        help="whether it is a resumed query")
    parser.add_argument("-rl", "--resume_location", type=str, action="store",
                        help="indicate the file or folder for resuming query")

    parser.add_argument("-psr", "--partition_suspend_resume", action="store_true", default=False,
                        help="indicate whether we will use partitioned file for suspend and resume")
    args = parser.parse_args()

    qid = args.query
    database = args.database
    data_folder = args.data_folder
    tmp_folder = args.tmp_folder
    thread = args.thread
    suspend_query = args.suspend_query
    resume_query = args.resume_query
    update_table = args.update_table
    partition_suspend_resume = args.partition_suspend_resume

    exec_query = globals()[qid].query

    if suspend_query:
        suspend_start_time = args.suspend_start_time
        suspend_end_time = args.suspend_end_time
        suspend_location = args.suspend_location

    if resume_query:
        resume_location = args.resume_location

    start = time.perf_counter()

    # open and connect a database
    if database == "memory":
        db_conn = duckdb.connect(database=':memory:')
    else:
        db_conn = duckdb.connect(database=database)

    db_conn.execute(f"PRAGMA temp_directory='{tmp_folder}'")
    db_conn.execute(f"PRAGMA threads={thread}")

    tpch_table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                        "customer_address", "customer_demographics", "date_dim", "dbgen_version",
                        "household_demographics", "income_band", "inventory", "item", "promotion",
                        "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
                        "warehouse", "web_page", "web_returns", "web_sales", "web_site"]

    # Create or Update TPC-H Datasets
    for t in tpch_table_names:
        if update_table:
            db_conn.execute(f"DROP TABLE {t};")
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{data_folder}/{t}.parquet');")

    # start the query execution
    results = None
    if suspend_query:
        execution = db_conn.execute_suspend(exec_query,
                                            suspend_location,
                                            suspend_start_time,
                                            suspend_end_time,
                                            partition_suspend_resume)
        results = execution.fetchdf()
    elif resume_query:
        execution = db_conn.execute_resume(exec_query, resume_location, partition_suspend_resume)
        results = execution.fetchdf()
    else:
        if isinstance(exec_query, list):
            for idx, query in enumerate(exec_query):
                if idx == len(exec_query) - 1:
                    results = db_conn.execute(query).fetchdf()
                else:
                    db_conn.execute(query)
        else:
            results = db_conn.execute(exec_query).fetchdf()

    print(results)
    end = time.perf_counter()
    print("Total Runtime: {}".format(end - start))
    db_conn.close()
    '''

if __name__ == "__main__":
    main()
