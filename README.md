# Riveter

An adaptive query suspension and resumption framework. It supports redo, pipeline-level, process-level query suspension and resumption strategy. 

## Datasets

We use TPC-H and TPC-DS benchmark to generate datasets. 

### TPC-H Dataset ###

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

### TPC-DS Dataset ###

First, generating the original tables (`dat` format) using TPC-DS tools, and simply running `duckdb_tpcds_data.py` can convert the table files to `parquet` or `csv` format using the following command,
```bash
cd tpcds
python3 duckdb_tpcds_data.py -d ../dataset/dat-sf1 -f parquet -rgs 10000
```

We have several datasets:

+ TPC-DS SF-0.01 (Tiny): `dataset/tpcds/dat-tiny`, `dataset/tpcds/parquet-tiny`
+ TPC-DS SF-0.1 (Small): `dataset/tpcds/dat-small`, `dataset/tpcds/parquet-small`
+ TPC-DS SF1: `dataset/tpcds/dat-sf1`, `dataset/tpcds/parquet-sf1`
+ TPC-DS SF10: `dataset/tpcds/dat-sf10`, `dataset/tpcds/parquet-sf10`
+ TPC-DS SF50: `dataset/tpcds/dat-sf50`, `dataset/tpcds/parquet-sf50`
+ TPC-DS SF100: `dataset/tpcds/dat-sf100`, `dataset/tpcds/parquet-sf100`


## Experiments

### Benchmark for Pipeline-level Suspension and Resumption

```bash
# choose the benchmark: tpch, or tpcds
bm=[tpch, tpcds]
# make sure in <bm> folder
cd <bm>
# the queries should be in <bm/queries>, the name the argument for "-q" option
# for example, q1-q22 in tpch, and q1-q99 in tpcds
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




## MISC

### Ubuntu Disk Commands

```bash
# Test the speed of cached read and buffered write
sudo hdparm -Tt <disk name, such as /dev/nvme0n1p3>
```

```bash
# Test the speed of random write 
sudo fio -ioengine=libaio -bs=4k -direct=1 -thread -rw=randwrite -filename=/dev/nvme0n1p3 -name="BS 4k randwrite test" -iodepth=16 -runtime=30
# Test the speed of rand read 
sudo fio -ioengine=libaio -bs=4k -direct=1 -thread -rw=randread -filename=/dev/nvme0n1p3 -name="BS 4k randread test" -iodepth=16 -runtime=30
```

With `df / -h`, you will know the disk where your root filesystem and OS (e.g., Ubuntu) is located.

```bash
$ df / -h 
Filesystem      Size  Used Avail Use% Mounted on
/dev/nvme0n1p3  449G  145G  282G  34% /
```

`lshw` will give more details about the disks or SSDs

```bash
$ sudo lshw -short -C disk
H/W path         Device          Class          Description
===========================================================
/0/100/1d/0/0    hwmon1          disk           NVMe disk
/0/100/1d/0/2    /dev/ng0n1      disk           NVMe disk
/0/100/1d/0/1    /dev/nvme0n1    disk           500GB NVMe disk
```

`lsblk` can further show something such as rotation, type, and mountpoints.

```bash
$ lsblk
NAME          MAJ:MIN RM   SIZE RO TYPE  MOUNTPOINTS
zram0         252:0    0    16G  0 disk  [SWAP]
nvme0n1       259:0    0 465.8G  0 disk  
├─nvme0n1p1   259:1    0  1022M  0 part  /boot/efi
├─nvme0n1p2   259:2    0     4G  0 part  /recovery
├─nvme0n1p3   259:3    0 456.8G  0 part  /
└─nvme0n1p4   259:4    0     4G  0 part  
  └─cryptswap 253:0    0     4G  0 crypt [SWAP]
```

