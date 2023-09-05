# Riveter

A Resource-adaptive Query Suspension and Resumption System. It supports pipeline-level and process-level query suspension and resumption. 

## CRIU

We also exploit `CRIU` to benchmark the performance of suspending and resuming queries at the process level. More details can be found [here](criu/README.md).


## Experiments

### Vanilla and TPC-H

First, generating the original tables (`tbl` format) using TPC-H tools, and simply running `duckdb_tpch_data.py` can convert the table files to `parquet` or `csv` format using, for example, the following command,
```bash
python3 duckdb_tpch_data.py -d dataset/tpch/tbl-sf1 -f parquet -rgs 10000
```
You can move the converted data to any folder you want.

We have several datasets:

+ TCP-H SF50: `dataset/tpch/tbl-sf50`, `dataset/tpch/parquet-sf50`
+ TCP-H SF10: `dataset/tpch/tbl-sf10`, `dataset/tpch/parquet-sf10`
+ TCP-H SF1: `dataset/tpch/tbl-sf1`, `dataset/tpch/parquet-sf1`
+ TCP-H Small (SF-0.1): `dataset/tpch/tbl-small`, `dataset/tpch/parquet-small`
+ TCP-H Tiny (SF-0.01): `dataset/tpch/tbl-tiny`, `dataset/tpch/parquet-tiny`

We have two sets of queries, vanilla and tpc-h, based on tcp-h datasets. 

**Vanilla**

We provide some demo queries in `demo/queries` for suspend and resume, which can be triggered by the following commands

```bash
# run q1 based on demo.db and the dataset from parquet-tiny using 2 threads
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2

# run q1 with suspension and serialize into single file
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl xxx.ratchet 
# run q1 with suspension and serialize into multiple files (will generate part-*.ratchet in demo folder)
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl ./ -psr

# run q1 with resumption using a single file
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl xxx.ratchet 
# run q1 with resumption using multiple files (will use all part-*.ratchet in the demo folder)
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl ./ -psr
```

**TPC-H**

`ratchet_tpch.py` will trigger the original TPC-H queries from q1 to q22 (stored in queries folder). For example,
```bash
python3 ratchet_tpch.py -q q1 -d ../dataset/tpch/parquet-tiny -td 1
```
The above command will run `q1` in TPC-H based on the data from `../dataset/tpch/parquet-tiny` using `1` thread.

The TPC-H benchmark is mostly used for functionality test.

### TPC-DS

Generating the original tables (`dat` format) using TPC-DS tools, and simply running `duckdb_tpcds_data.py` can convert the table files to `parquet` or `csv` format using, for example, the following command,
```bash
python3 duckdb_tpcds_data.py -d dataset/dat-sf1 -f parquet -rgs 10000
```

We have several datasets:

TCP-DS SF1: `dataset/tpcds/dat-sf1`, `dataset/tpcds/parquet-sf1`

`ratchet_tpcds.py` will trigger the original TPC-DS queries from q1 to q99 (stored in queries folder). For example,
```bash
python3 ratchet_tpcds.py -q q1 -d ../dataset/tpcds/parquet-sf1 -td 1
```
The above command will run `q1` in TPC-H based on the data from `../dataset/tpcds/parquet-sf1` using `1` thread.

