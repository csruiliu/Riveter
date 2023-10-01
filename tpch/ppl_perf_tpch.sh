#!/bin/bash

SF=10
THREAD=1
TMP="tmp"
DATABASE="tpch-sf$SF.db"
DATA_FILE="../dataset/tpch/parquet-sf$SF"
CRIU_CMD=/opt/criu/sbin/criu
CKPT_PATH=./criu-ckpt
PID=0

# sf-10
#exec_times=(7.4 0.85 4.4 4 4.5 3.3 9.3 4.4 12.8 5.9 0.44 2.3 5.1 3.5 5.2 0.9 8.5 14.4 6.2 4.7 10.5 1.7)

# sf-50
#exec_times=(44.3 4.6 27 23.9 26.5 21.6 54 29.1 69.8 35.7 5.4 17.5 28.4 21.7 32.1 7.7 50.4 87.8 38.65 28.23 73.9 9.8)

# sf-100
#exec_times=(107.1 9.3 67.8 68.6 64.6 63.6 141 265.1 361.4 250.5 30.7 248.2 86.5 275.3 245.8 24.4 412.2 311.7 388.4 269.9 353.3 61.6)

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
if [ -f "$DATABASE" ]; then
  echo "Remove $DATABASE"
  rm "$DATABASE"
fi

# run a ligh query for ingest the data
echo "Ingesting datasets..."
nohup python3 ratchet_tpch.py -q "q2" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "init_q2.out" 2>&1

for ((i=0 ; i < ${#queries[@]} ; ++i)); do
  qid=${queries[i]}
  et=${exec_times[i]}
  sp=${sps[i]}

  echo -e "\n########################"
  echo "# Query $qid"
  echo "########################"

  st=$(echo "scale=3; $et * $sp / 100" | bc)
  echo "Suspension Time: $st"

  python3 ratchet_tpch.py -q "q2" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP -s -st "$st" -se "$st" -sl "$qid.ratchet"
done


