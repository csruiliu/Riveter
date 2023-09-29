#!/bin/bash

SF=1
THREAD=1
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
  echo $exec_time
  sp30=$(echo "scale=3; $exec_time * 0.3" | bc)
  sp60=$(echo "scale=3; $exec_time * 0.6" | bc)
  sp90=$(echo "scale=3; $exec_time * 0.9" | bc)

  echo "30% Suspension Point: $sp30"
  echo "60% Suspension Point: $sp60"
  echo "90% Suspension Point: $sp90"

  echo "== Cleaning cache =="
  sudo sysctl -w vm.drop_caches=1

  start_time=$(date +%s.%3N)
  python3 ratchet_tpch.py -q "$qid" -d "$DATABASE" -df "$DATA_FILE" -td "$THREAD" -tmp $TMP &
  PID=$!

  sleep "$sp30"
  echo "== Suspend Job at SP30 =="

  # checkpoint process into disk
  checkpoint_start_time=$(date +%s.%3N)

  if [ -d "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30" ]; then
    echo "Removing and Creating $CKPT_PATH/ckpt_sf${SF}_${qid}_sp30 folder."
    rm -rf "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"
    mkdir "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"
  else
    echo "Creating $CKPT_PATH/ckpt_sf${SF}_${qid}_sp30 folder."
    mkdir "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"
  fi

  sudo "$CRIU_CMD" dump -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30" -t "${PID}" --file-locks --shell-job
  echo "Dumping to $CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"

  # force data sync between buffer and disk
  sync

  checkpoint_end_time=$(date +%s.%3N)
  checkpoint_time=$(echo "scale=3; $checkpoint_end_time - $checkpoint_start_time" | bc)
  echo "Checkpoint Time: $checkpoint_time"

  # clean page cache
  sudo sysctl -w vm.drop_caches=1

  echo "== Resume Job at SP30 =="
  echo "Restoring from $CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"

  # restore the process from disk and print out final results
  output=$(sudo "$CRIU_CMD" restore -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30" --shell-job)
  echo "$output"
  # sudo "$CRIU_CMD" restore -D "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30" --file-locks --shell-job

  end_time=$(date +%s.%3N)

  # elapsed time with millisecond resolution
  # keep three digits after floating point.
  elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
  eval "echo Elapsed Time: $elapsed seconds"

  if [ -f "$DATABASE" ]; then
    echo "Remove $DATABASE"
    rm "$DATABASE"
  fi

  ckpt_size=$(du -sh "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30")
  eval "echo Size of CKPT by CRIU: $ckpt_size"

  if [ -d "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30" ]; then
    echo "Removing $CKPT_PATH/ckpt_sf${SF}_${qid}_sp30 folder."
    rm -rf "$CKPT_PATH/ckpt_sf${SF}_${qid}_sp30"
  fi
done
