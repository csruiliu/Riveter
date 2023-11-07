import argparse
import duckdb

from tpch.queries import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", type=str, action="store", required=True,
                        choices=['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11',
                                 'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21', 'q22'],
                        help="indicate the query id")
    parser.add_argument("-d", "--database", type=str, action="store", required=True, default="memory",
                        help="indicate the database location, memory or other location")
    parser.add_argument("-df", "--data_folder", type=str, action="store", required=True,
                        help="indicate the TPC-H dataset for query execution, such as <dataset/tpch/parquet-sf1>")
    parser.add_argument("-tmp", "--tmp_folder", type=str, action="store", required=True,
                        help="indicate the tmp folder for DuckDB, such as </tmp>")
    parser.add_argument("-td", "--thread", type=int, action="store", default=1,
                        help="indicate the number of threads in DuckDB")
    parser.add_argument("-xpl", "--explain_mode", type=str, action="store", choices=["default", "analyze"],
                        help="indicate the explain mode in DuckDB")
    parser.add_argument("-ut", "--update_table", action="store_true",
                        help="force to update table in database")

    args = parser.parse_args()

    qid = args.query
    database = args.database
    data_folder = args.data_folder
    tmp_folder = args.tmp_folder
    thread = args.thread
    explain_mode = args.explain_mode

    update_table = args.update_table

    exec_query = globals()[qid].query

    if database == "memory":
        db_conn = duckdb.connect(database=':memory:')
    else:
        db_conn = duckdb.connect(database=database)

    db_conn.execute(f"PRAGMA temp_directory='{tmp_folder}'")
    db_conn.execute(f"PRAGMA threads={thread}")
    db_conn.execute(f"PRAGMA enable_profiling='json'")
    db_conn.execute(f"PRAGMA profile_output='/home/ruiliu/Develop/riveter/cbo.json'")

    tpch_table_names = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

    # Create or Update TPC-H Datasets
    for t in tpch_table_names:
        if update_table:
            db_conn.execute(f"DROP TABLE IF EXISTS {t};")
        db_conn.execute(f"CREATE TABLE IF NOT EXISTS {t} AS SELECT * FROM read_parquet('{data_folder}/{t}.parquet');")

    if explain_mode == "default":
        result = db_conn.sql(exec_query).explain()
    else:
        result = db_conn.sql(exec_query).explain("ANALYZE")

    print(result)
    db_conn.close()


if __name__ == "__main__":
    main()
