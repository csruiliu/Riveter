# Riveter

An adaptive query suspension and resumption framework. It supports redo, pipeline-level, process-level query suspension and resumption strategy. 

## Datasets

We use TPC-H benchmark to generate datasets. 

First, generating the original tables (`tbl` format) using TPC-H tools, and simply running `duckdb_tpch_data.py` can convert the table files to `parquet` or `csv` format using the following command,
```bash
# make sure in the tpch folder
cd tpch
python3 duckdb_tpch_data.py -d ../dataset/tpch/tbl-sf1 -f parquet -rgs 10000
```
You can move the converted data to any folder you want.

We have several datasets:

+ TPC-H SF-0.01 (Tiny): `dataset/tpch/tbl-tiny`, `dataset/tpch/parquet-tiny`
+ TPC-H SF-0.1 (Small): `dataset/tpch/tbl-small`, `dataset/tpch/parquet-small`
+ TPC-H SF1: `dataset/tpch/tbl-sf1`, `dataset/tpch/parquet-sf1`
+ TPC-H SF10: `dataset/tpch/tbl-sf10`, `dataset/tpch/parquet-sf10`
+ TPC-H SF50: `dataset/tpch/tbl-sf50`, `dataset/tpch/parquet-sf50`
+ TPC-H SF100: `dataset/tpch/tbl-sf100`, `dataset/tpch/parquet-sf100`

We have two sets of queries using TPC-H datasets: vanilla and tpc-h. 

## Experiments

### Benchmark for Pipeline-level Suspension and Resumption

```bash
# choose the benchmark: tpch, or tpcds
bm=tpch
# make sure in <bm> folder
cd <bm>
# the queries should be in <bm/queries>, the name the argument for "-q" option
# for example, q1-q22 in tpch
# run q1 based on xxx.db and the dataset from parquet-tiny using 2 threads
python3 ratchet_<bm>.py -q q1 -d xxx.db -df ../dataset/<bm>/parquet-sf10 -td 2

# run q1 with a suspension point determined by time window [st, se] with uniform distribution, and serialize into single file
python3 ratchet_<bm>.py -q q1 -d xxx.db -df ../dataset/<bm>/parquet-sf10 -td 2 -s -st 0 -se 0 -sl yyy.ratchet 
# run q1 with resumption using a single file
python3 ratchet_<bm>.py -q q1 -d xxx.db -df ../dataset/<bm>/parquet-sf10 -td 2 -r -rl yyy.ratchet 

# run q1 with a suspension point determined by time window [st, se] with uniform distribution
# and serialize into multiple files (will generate part-*.ratchet in demo folder)
python3 ratchet_<bm>.py -q q1 -d xxx.db -df ../dataset/<bm>/parquet-sf10 -td 2 -s -st 0 -se 0 -sl ./ -psr
# run q1 with resumption using multiple files (will use all part-*.ratchet in the demo folder)
python3 ratchet_<bm>.py -q q1 -d xxx.db -df ../dataset/<bm>/parquet-sf10 -td 2 -r -rl ./ -psr
```

### Benchmark for Process-level Suspension and Resumption 

We also benchmark the performance of suspending and resuming queries at the process level. More details can be found [here](criu/README.md).

### Benchmark for Suspension and Resumption with Cost Model

The main cost model implementation can be found in `riveter.py` and `cost_model.py`

We exploit the python packages such as `subprocess`, `sysv_ipc`, `ctypes` to interact with different processes for pipeline-level strategy, process-level strategy, and redo strategy. We use shared memory to interact with processes. 
