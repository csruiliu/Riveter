
help_func() {
    echo "Usage:"
    echo "criu_restore.sh [-p CKPT_PATH]"
    echo "Description:"
    echo "CKPT_PATH, the checkpoint path for resume"
    exit 0
}

while getopts 'p:h' OPT
do
    case $OPT in
        p) CKPT_PATH="$OPTARG";;
        h) help_func;;
        ?) echo "Unrecognized Parameters"; exit 1;;
    esac
done

criu_cmd=/usr/lib/criu/criu-3.17.1/criu/criu
ckpt_path=/home/ruiliu/Develop/ratchet-duckdb/ratchet/criu/criu-ckpt/

PID=$(ps -ef | grep "python3 demo.py" | grep -v grep | awk '{print $2}')

if [ -d "$CKPT_PATH" ]; then
  echo "Resuming from "$CKPT_PATH" folder."
  output=$(sudo "$criu_cmd" restore -D "$CKPT_PATH" --shell-job)
  echo "$output"
else
  echo "We cannot find "$CKPT_PATH" folder."
fi



