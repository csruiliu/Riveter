#!/bin/bash

SF=100
THREAD=1
TMP="tmp"
DATABASE="tpch-sf$SF.db"
DATA_FILE="../dataset/tpch/parquet-sf$SF"
CRIU_CMD=/opt/criu/sbin/criu
CKPT_PATH=./criu-ckpt
PID=0

# sf-10
#exec_times=(7.6 0.9 4 3.9 4.2 3.4 8.1 4.2 12.7 5.9 0.4 2.2 4.8 3.3 4.7 0.9 8.3 14.3 5.9 4.3 10.3 1.7)

# sf-50
#exec_times=(44.8 4.4 26.2 23.9 26 22 54.5 27.7 68.3 34.6 2.2 16.7 27.2 22.2 32.3 4.7 49.6 81.9 39.2 28.5 78.5 10.1)

# sf-100
#exec_times=(108.4 9.4 69.3 67.1 64.3 65.6 135 74.2 160.7 82.9 4.7 48.8 56.1 58.6 90.4 9.9 115.8 193.9 98.7 72 223.6 21)

# test
exec_times=(7.4)

# suspension point (percent of the overall execution time 0-100)

# sf-10
#sps=(50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50)

# sf-50
#sps=(50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50)

# sf-100
#sps=(50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50 50)

# test
sps=(50)

queries=("q1")
# queries=("q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22")

# remove database if exists
#if [ -f "$DATABASE" ]; then
#  echo "Remove $DATABASE"
#  rm "$DATABASE"
#fi

# run a ligh query for ingest the data
#echo "Ingesting datasets..."
#nohup python3 ratchet_tpch.py -q "q2" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "init_q2.out" 2>&1

for ((i=0 ; i < ${#queries[@]} ; ++i)); do
  qid=${queries[i]}
  et=${exec_times[i]}
  sp=${sps[i]}

  echo -e "\n########################"
  echo "# Query $qid"
  echo "########################"

  st=$(echo "scale=3; $et * $sp / 100" | bc)
  echo "Suspension Time: $st"

  # checkpoint process into disk
  start_time=$(date +%s.%3N)

  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP -s -st "$st" -se "$st" -sl "$qid.ratchet"

  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP -r -rl "$qid.ratchet"

  end_time=$(date +%s.%3N)

  elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
  eval "echo Elapsed Time: $elapsed seconds"
done


