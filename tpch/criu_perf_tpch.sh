#!/bin/bash

SF=1
THREAD=1
# suspension point (percent of the overall execution time, 0-100)
SP=50
TMP="tmp"
DATA_FILE="../dataset/tpch/parquet-sf$SF"
CRIU_CMD=/opt/criu/sbin/criu
CKPT_PATH=./criu-ckpt
PID=0

# queries=("q1" "q2")
queries=("q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q10" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22")

for qid in "${queries[@]}"; do
  echo -e "\n########################"
  echo "# Query $qid"
  echo "########################"

  DATABASE="tpch-$qid-sf$SF.db"

  if [ -f "$DATABASE" ]; then
    echo "Remove $DATABASE"
    rm "$DATABASE"
  fi

  # ingest the data
  nohup python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "init_$qid.out" 2>&1

  # run an initial query
  nohup python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP > "init_$qid.out" 2>&1

  # get the total execution time and set the suspension points
  exec_time=$(echo "scale=3; $(tail -n 1 "init_$qid.out")" | bc)
  echo "$exec_time"
  st=$(echo "scale=3; $exec_time * $SP / 100" | bc)
  echo "Suspension Time: $st"

  echo "== Cleaning cache =="
  sudo sysctl -w vm.drop_caches=1

  start_time=$(date +%s.%3N)
  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP &
  PID=$!

  sleep "$st"
  echo "== Suspend Job at SP$SP =="

  # checkpoint process into disk
  checkpoint_start_time=$(date +%s.%3N)

  if [ -d "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}" ]; then
    echo "Removing and Creating $CKPT_PATH/ckpt_sf${SF}_${qid}_${SP} folder."
    rm -rf "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"
    mkdir "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"
  else
    echo "Creating $CKPT_PATH/ckpt_sf${SF}_${qid}_${SP} folder."
    mkdir "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"
  fi

  sudo "$CRIU_CMD" dump -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}" -t "${PID}" --file-locks --shell-job
  echo "Dumping to $CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"

  # force data sync between buffer and disk
  sync

  checkpoint_end_time=$(date +%s.%3N)
  checkpoint_time=$(echo "scale=3; $checkpoint_end_time - $checkpoint_start_time" | bc)
  echo "Checkpoint Time: $checkpoint_time"

  # clean page cache
  sudo sysctl -w vm.drop_caches=1

  echo "== Resume Job at ${SP} =="
  echo "Restoring from $CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"

  # restore the process from disk and print out final results
  output=$(sudo "$CRIU_CMD" restore -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}" --shell-job)
  echo "$output"
  # sudo "$CRIU_CMD" restore -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}" --file-locks --shell-job

  end_time=$(date +%s.%3N)

  # elapsed time with millisecond resolution
  # keep three digits after floating point.
  elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
  eval "echo Elapsed Time: $elapsed seconds"

  if [ -f "$DATABASE" ]; then
    echo "Remove $DATABASE"
    rm "$DATABASE"
  fi

  ckpt_size=$(du -sh "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}")
  eval "echo Size of CKPT by CRIU: $ckpt_size"

  if [ -d "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}" ]; then
    echo "Removing $CKPT_PATH/ckpt_sf${SF}_${qid}_${SP} folder."
    rm -rf "$CKPT_PATH/ckpt_sf${SF}_${qid}_${SP}"
  fi
done
