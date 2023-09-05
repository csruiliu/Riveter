
# CRIU for Ratchet

We have tested CRIU for suspending and resuming ratchet-duckdb.

## CRIU Installation

It is better to follow the instructions in the [CRIU offical website](https://criu.org/Installation). The following commands essentially show how to list the required packages.

```bash
#!/bin/bash

# install gcc and make packages
sudo apt-get install -y build-essential

# install protocol buffers
sudo apt-get install -y libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf

# install other required libs
sudo apt-get install -y libcap-dev libnl-3-dev libnet-dev

# install pkg-config to check on build library dependencies
sudo apt-get install -y pkg-config

# <folder_for_criu> is the place you want to put criu, such as, /usr/lib/criu
cd <folder_for_criu>

# get criu-3.17.1 or other visions of criu
wget https://github.com/checkpoint-restore/criu/archive/refs/tags/v3.17.1.tar.gz

tar -xvf v3.17.1.tar.gz

cd <fold_for_criu>/criu-3.17.1

# compile the criu and generate the `<folder_for_criu>/criu-3.17.1/criu/criu` which can be used for execution
make
```

### Install CRIU using Docker

It is feasible to install and run CRIU inside docker, but this feature is only available in the `experimental` mode for Docker. More details can be found [here](https://criu.org/Docker). Due to this limited availability, we still install and run CRIU in a regular system (e.g., Ubuntu 22.04) instead of a docker. 


## CRIU Running

We have three script `criu_perf.sh`, `criu_suspend.sh`, and `criu_resume.sh`.

When using CRIU in for ratchet-duckdb in the above scripts, it is important to add `--file-locks --shell-job` for suspension and add `--shell-job` for resumption.
