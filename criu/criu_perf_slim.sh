#!/bin/bash

criu_cmd=/usr/lib/criu/criu-3.17.1/criu/criu
ckpt_path=/home/ruiliu/ratchet-duckdb/ratchet/criu/criu-ckpt
PID=0

echo "== Cleaning cache =="
sudo sh -c "/usr/bin/echo 1 > /proc/sys/vm/drop_caches"

start_time=$(date +%s.%3N)
python3 demo.py -q q1 -d demo.db -df ../dataset/tpch/parquet-sf10 -td 2 &
PID=$(ps -ef | grep "python3 demo.py" | grep -v grep | awk '{print $2}')

sleep 0.5
echo "== Suspend Job =="
if [ -d "$ckpt_path/ckpt_${PID}" ]; then
  echo "Removing and Creating $ckpt_path/ckpt_${PID} folder."
  sudo rm -rf "$ckpt_path/ckpt_${PID}"
  mkdir "$ckpt_path/ckpt_${PID}"
else
  echo "Creating $ckpt_path/ckpt_${PID} folder."
  mkdir "$ckpt_path/ckpt_${PID}"
fi

sudo "$criu_cmd" dump -D "$ckpt_path/ckpt_${PID}" -t "$PID" --file-locks --shell-job
echo "Dumping to $ckpt_path/ckpt_${PID}"

if [ -d "$ckpt_path/ckpt_${PID}" ]; then
  echo "Resuming from $ckpt_path/ckpt_${PID} folder."
  output=$(sudo "$criu_cmd" restore -D "$ckpt_path/ckpt_${PID}" --shell-job)
  echo "$output"
else
  echo "We cannot find $ckpt_path/ckpt_${PID} folder."
fi
end_time=$(date +%s.%3N)
elapsed=$(echo "scale=3; $end_time - $start_time" | bc)
eval "echo Elapsed Time: $elapsed seconds"