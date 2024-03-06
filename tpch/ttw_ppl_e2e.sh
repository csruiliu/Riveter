#!/bin/bash

# This script measure the end-to-end time under 100% termaination probaility and various termination time windows (TTW) for each query in TPC-H benchmark, using pipeline-level strategy, 

SF=100
THREAD=1
TMP="tmp"
DATABASE="tpch-sf$SF.db"
DATA_FILE="../dataset/tpch/parquet-sf$SF"
CRIU_CMD=/opt/criu/sbin/criu
CKPT_PATH=./criu-ckpt
PID=0

# sf-100
exec_times=(108 9.9 80.6 81.1 71.9 62.4 138 80.7 158.6 84.9 6.8 50.5 64 60 88.3 10.6 116.1 209.7 99.9 72.4 242.4 22.2)

# termination point (percent of the overall execution time 0-100)

# sf-100
# first_ttp=(12.9 0.8 7.5 10.4 7.6 9.1 16.6 9.5 23.9 10.8 0.8 7.3 8.8 7.8 10.9 1 20.3 31.7 13.4 8.2 22.1 3.1)
# second_ttp=(35.4 3.8 29 32.2 26.6 24.3 52.3 30 59.4 32.8 2.5 18 23.4 20.5 38.4 3.9 38.9 80.2 43.1 27.6 84.7 8.5)
# third_ttp=(65.8 6.2 48.9 45.5 45.4 40.7 92.1 50.9 97 53.5 4.5 30.5 38.9 36.8 56.9 6.4 68.8 123.8 62.4 44.5 150.3 13.4)
# fourth_ttp=(94.4 8.8 69.1 70.3 65.8 54.6 120.9 67 140 75.4 6 43 58.1 51.2 77.4 9.2 98.4 178.7 89.4 62 212.7 19.4)

# queries=("q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22")

# for test
first_ttp=(12.9)
second_ttp=(35.4)
third_ttp=(65.8)
fourth_ttp=(94.4)

queries=("q1")

# run a ligh query for ingest the data
echo "Ingesting datasets..."
nohup python3 ratchet_normal.py -q "q2" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "tmp_q2.out" 2>&1

ttp_mark=1

for ((i=0 ; i < ${#queries[@]} ; ++i)); do
  qid=${queries[i]}
  et=${exec_times[i]}
  
  if [ $ttp_mark -eq 1 ]; then
    ttp=${first_ttp[i]}
  elif [ $ttp_mark -eq 2 ]; then
    ttp=${second_ttp[i]}
  elif [ $ttp_mark -eq 3 ]; then
    ttp=${third_ttp[i]}
  elif [ $ttp_mark -eq 4 ]; then
    ttp=${fourth_ttp[i]}
  fi

  echo -e "\n########################"
  echo "# Query $qid"
  echo "########################"

  echo "Termination Time: $ttp"
  
  if [ -e "./$qid.ratchet" ]; then
    echo "Removing $qid.ratchet file."
    rm -rf "./$qid.ratchet"
  fi

  # checkpoint process into disk
  start_time=$(date +%s.%3N)

  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP -s -st "$ttp" -se "$ttp" -sl "$qid.ratchet"

  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP -r -rl "$qid.ratchet"

  end_time=$(date +%s.%3N)

  elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
  eval "echo Elapsed Time: $elapsed seconds"
done

