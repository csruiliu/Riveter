import argparse
import duckdb
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table_name", type=str, action="store",
                        choices=["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"],
                        help="indicate the name of table that needs to be converted to parquet")
    parser.add_argument("-d", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for conversion such as dataset/tpch/tbl-sf1")
    parser.add_argument("-f", "--output_format", type=str, action="store", required=True,
                        help="indicate the output data format", choices=["csv", "parquet"])
    parser.add_argument("-rgs", "--row_group_size", type=int, action="store",
                        help="indicate scale factor of the dataset, such as 100000")
    args = parser.parse_args()

    tbl_name = args.table_name
    data_folder = args.data_folder
    output_format = args.output_format
    row_group_size = args.row_group_size

    if output_format == "parquet" and row_group_size is None:
        raise ValueError("Please indicate row group size for parquet")

    if tbl_name is not None:
        table_list = [tbl_name]
    else:
        table_list = ["part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region"]

    part_schema = """(P_PARTKEY INTEGER, P_NAME VARCHAR, P_MFGR VARCHAR, P_BRAND VARCHAR, P_TYPE VARCHAR, 
    P_SIZE INTEGER, P_CONTAINER VARCHAR, P_RETAILPRICE DOUBLE, P_COMMENT VARCHAR)"""

    supplier_schema = """(S_SUPPKEY INTEGER, S_NAME VARCHAR, S_ADDRESS VARCHAR, S_NATIONKEY INTEGER, 
    S_PHONE VARCHAR, S_ACCTBAL DOUBLE, S_COMMENT VARCHAR)"""

    partsupp_schema = """(PS_PARTKEY INTEGER, PS_SUPPKEY INTEGER, 
    PS_AVAILQTY INTEGER, PS_SUPPLYCOST DOUBLE, PS_COMMENT VARCHAR)"""

    customer_schema = """(C_CUSTKEY INTEGER, C_NAME VARCHAR, C_ADDRESS VARCHAR, C_NATIONKEY INTEGER, 
    C_PHONE VARCHAR, C_ACCTBAL DOUBLE, C_MKTSEGMENT VARCHAR, C_COMMENT VARCHAR)"""

    orders_schema = """(O_ORDERKEY INTEGER, O_CUSTKEY INTEGER, O_ORDERSTATUS VARCHAR, O_TOTALPRICE DOUBLE, 
    O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR, O_CLERK VARCHAR, O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR)"""

    lineitem_schema = """(L_ORDERKEY INTEGER, L_PARTKEY INTEGER, L_SUPPKEY INTEGER, L_LINENUMBER INTEGER, 
    L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG VARCHAR, 
    L_LINESTATUS VARCHAR, L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR, 
    L_SHIPMODE VARCHAR, L_COMMENT VARCHAR)"""

    nation_schema = """(N_NATIONKEY INTEGER, N_NAME VARCHAR, N_REGIONKEY INTEGER, N_COMMENT VARCHAR)"""

    region_schema = "(R_REGIONKEY INTEGER, R_NAME VARCHAR, R_COMMENT VARCHAR)"

    db_conn = duckdb.connect(database=':memory:')

    for table in table_list:
        print(f"Convert {table}.tbl to parquet...")
        table_schema = table + "_schema"
        db_conn.execute(f"CREATE TABLE {table} {locals()[table_schema]};")
        db_conn.execute(f"COPY {table} FROM '{data_folder}/{table}.tbl' ( DELIMITER '|' );")
        if output_format == "parquet":
            db_conn.execute(f"COPY {table} TO '{table}.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE {row_group_size});")
            parquet_file = pq.ParquetFile(f"{table}.parquet")
            print("Number of row groups in {}.parquet: {}".format(table, parquet_file.num_row_groups))
        elif output_format == "csv":
            db_conn.execute(f"COPY {table} TO '{table}.csv' WITH (HEADER 1, DELIMITER ',');;")


if __name__ == "__main__":
    main()
