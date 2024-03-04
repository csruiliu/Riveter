#!/bin/bash

# This script measure the end-to-end time for normal execution, 

SF=100
THREAD=1
TMP="tmp"
DATABASE="tpch-sf$SF.db"
DATA_FILE="../dataset/tpch/parquet-sf$SF"
ITER=10

# test
queries=("q1")
# all queries
queries=("q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22")

# remove database if exists
#if [ -f "$DATABASE" ]; then
#  echo "Remove $DATABASE"
#  rm "$DATABASE"
#fi

# run a light query for ingest the data
echo "Ingesting datasets..."
nohup python3 ratchet_normal.py -q "q2" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "tmp_q2.out" 2>&1
rm "tmp_q2.out"

for n in {1..10}; do
    echo "{$n}th iteration"

    for ((i=0 ; i < ${#queries[@]} ; ++i)); do
      qid=${queries[i]}

      echo -e "\n########################"
      echo "# Query $qid"
      echo "########################"

      # checkpoint process into disk
      start_time=$(date +%s.%3N)

      python3 ratchet_normal.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP

      end_time=$(date +%s.%3N)

      elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
      eval "echo Elapsed Time: $elapsed seconds"

      # echo 3 > /proc/sys/vm/drop_caches
    done
done



