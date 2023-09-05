import argparse
import duckdb
import pyarrow.parquet as pq
import table_schema as schema


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-t", "--table_name", type=str, action="store",
                        choices=["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", 
                                 "customer_address", "customer_demographics", "date_dim", "dbgen_version", 
                                 "household_demographics", "income_band", "inventory", "item", "promotion", 
                                 "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", 
                                 "warehouse", "web_page", "web_returns", "web_sales", "web_site"],
                        help="indicate the name of table that needs to be converted to parquet")
    
    parser.add_argument("-d", "--data_folder", type=str, action="store", required=True,
                        help="indicate the data source folder for conversion such as dataset/tpcds/dat-sf1>")
    
    parser.add_argument("-f", "--output_format", type=str, action="store", required=True,
                        help="indicate the output data format", choices=["csv", "parquet"])
    
    parser.add_argument("-rgs", "--row_group_size", type=int, action="store",
                        help="indicate scale factor of the dataset, such as 100000")
    
    args = parser.parse_args()

    table_name = args.table_name
    data_folder = args.data_folder
    output_format = args.output_format
    row_group_size = args.row_group_size

    if output_format == "parquet" and row_group_size is None:
        raise ValueError("Please indicate row group size for parquet")

    if table_name is not None:
        table_list = [table_name]
    else:
        table_list = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", 
                      "customer_address", "customer_demographics", "date_dim", "dbgen_version", 
                      "household_demographics", "income_band", "inventory", "item", "promotion", 
                      "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", 
                      "warehouse", "web_page", "web_returns", "web_sales", "web_site"]

    db_conn = duckdb.connect(database=':memory:')

    for table in table_list:
        print(f"Convert {table}.dat to parquet...")

        table_schema = getattr(schema, table)

        db_conn.execute(f"CREATE TABLE {table} {table_schema};")

        db_conn.execute(f"COPY {table} FROM '{data_folder}/{table}.dat' ( DELIMITER '|' );")

        if output_format == "parquet":
            db_conn.execute(f"COPY {table} TO '{table}.parquet' (FORMAT 'parquet', ROW_GROUP_SIZE {row_group_size});")
            parquet_file = pq.ParquetFile(f"{table}.parquet")
            print("Number of row groups in {}.parquet: {}".format(table, parquet_file.num_row_groups))
        elif output_format == "csv":
            db_conn.execute(f"COPY {table} TO '{table}.csv' WITH (HEADER 1, DELIMITER ',');;")


if __name__ == "__main__":
    main()
