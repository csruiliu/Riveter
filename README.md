# Riveter

A Resource-adaptive Query Suspension and Resumption System. It supports pipeline-level and process-level query suspension and resumption. 

## CRIU

We also exploit `CRIU` to benchmark the performance of suspending and resuming queries at the process level. More details can be found [here](criu/README.md).


## Experiments

### Vanilla and TPC-H

First, generating the original tables (`tbl` format) using TPC-H tools, and simply running `duckdb_tpch_data.py` can convert the table files to `parquet` or `csv` format using the following command,
```bash
# make sure in the tpch folder
cd tpch
python3 duckdb_tpch_data.py -d ../dataset/tpch/tbl-sf1 -f parquet -rgs 10000
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

We provide some simple queries in `vanilla/queries` for suspend and resume, which can be triggered by the following commands

```bash
# make sure in the vanilla folder
cd vanilla
# run q1 based on xxx.db and the dataset from parquet-tiny using 2 threads
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2

# run q1 with suspension and serialize into single file
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl yyy.ratchet 
# run q1 with resumption using a single file
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl yyy.ratchet 

# run q1 with suspension and serialize into multiple files (will generate part-*.ratchet in demo folder)
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl ./ -psr
# run q1 with resumption using multiple files (will use all part-*.ratchet in the demo folder)
python3 ratchet_vanilla.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl ./ -psr
```

**TPC-H**

We have tpch queries in `tpch/queries` for suspend and resume. `ratchet_tpch.py` will trigger the original TPC-H queries from q1 to q22. 

```bash
# make sure in the tpch folder
cd tpch
# run q1 based on xxx.db and the dataset from parquet-tiny using 2 threads
python3 ratchet_tpch.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2

# run q1 with suspension and serialize into single file
python3 ratchet_tpch.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl yyy.ratchet 
# run q1 with resumption using a single file
python3 ratchet_tpch.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl yyy.ratchet 

# run q1 with suspension and serialize into multiple files (will generate part-*.ratchet in demo folder)
python3 ratchet_tpch.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -s -st 0 -se 0 -sl ./ -psr
# run q1 with resumption using multiple files (will use all part-*.ratchet in the demo folder)
python3 ratchet_tpch.py -q q1 -d xxx.db -df ../dataset/tpch/parquet-tiny -td 2 -r -rl ./ -psr
```

The above command will run `q1` in TPC-H based on the data from `../dataset/tpch/parquet-tiny` using `1` thread.

The TPC-H benchmark is mostly used for functionality test.

### TPC-DS

We have tpch queries in `tpcds/queries` for suspend and resume. `ratchet_tpcds.py` will trigger the original TPC-DS queries from q1 to q99. 

First, generating the original tables (`dat` format) using TPC-DS tools, and simply running `duckdb_tpcds_data.py` can convert the table files to `parquet` or `csv` format using the following command,
```bash
cd tpcds
python3 duckdb_tpcds_data.py -d ../dataset/dat-sf1 -f parquet -rgs 10000
```

We have several datasets:

TCP-DS SF1: `dataset/tpcds/dat-sf1`, `dataset/tpcds/parquet-sf1`

`ratchet_tpcds.py` will trigger the original TPC-DS queries from q1 to q99 (stored in queries folder). For example,
```bash
python3 ratchet_tpcds.py -q q1 -d ../dataset/tpcds/parquet-sf1 -td 1
```
The above command will run `q1` in TPC-H based on the data from `../dataset/tpcds/parquet-sf1` using `1` thread.

The suspension and resumption commands can follow the above ones in the Vanilla and TPC-H.    

## MISC

Some commands to get a better understand of your disk or SSD

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
