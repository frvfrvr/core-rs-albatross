#!/bin/bash

set -e

PIPE="/home/nimiq/nimiq.log.pipe"

mkfifo $PIPE || true
cat $PIPE &
mkdir -p /home/nimiq/.nimiq

if [[ -z "$NIMIQ_HOST" ]]; then
    export NIMIQ_HOST=$(hostname -i)
fi

./docker_config.sh > /home/nimiq/.nimiq/client.toml

#cat /home/nimiq/.nimiq/client.toml > $PIPE

/usr/local/bin/nimiq-client $@
