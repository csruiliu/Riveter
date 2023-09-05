import duckdb
import argparse
import time
import pandas as pd

from queries import *


def main():
    pd.set_option('display.float_format', '{:.1f}'.format)
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", type=str, action="store", required=True,
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', 
                                 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22'], 
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

    tpch_table_names = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

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


if __name__ == "__main__":
    main()
