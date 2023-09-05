criu_cmd=/usr/lib/criu/criu-3.17.1/criu/criu
ckpt_path=/home/ruiliu/Develop/ratchet-duckdb/ratchet/criu/criu-ckpt

PID=$(ps -ef | grep "python3 demo.py" | grep -v grep | awk '{print $2}')

if [ -d "$ckpt_path/ckpt_${PID}" ]; then
  echo "Removing and Creating $ckpt_path/ckpt_${PID} folder."
  sudo rm -rf "$ckpt_path/ckpt_${PID}"
  mkdir "$ckpt_path/ckpt_${PID}_${i}"
else
  echo "Creating $ckpt_path/ckpt_${PID} folder."
  mkdir "$ckpt_path/ckpt_${PID}"
fi

sudo "$criu_cmd" dump -D "$ckpt_path/ckpt_${PID}" -t "$PID" --file-locks --shell-job
echo "Dumping to $ckpt_path/ckpt_${PID}"
