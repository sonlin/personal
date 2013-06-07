#!/bin/bash
bin_dir=/data/services/mbtq/backend/mbtq_servers/bin

bin_list=$(ls $bin_dir)
echo $bin_list
for bin in $bin_list
do
	pkill $bin
done

source /data/services/mbtq/var/monitor/monitor_process_auto_start.sh

