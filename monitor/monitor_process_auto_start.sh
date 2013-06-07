#!/bin/bash
#set -x
source /data/services/mbtq/var/monitor/service.conf
source /data/services/mbtq/var/monitor/monitor_library.sh

dead_process_list=$( processmonitor "$service" )
echo $dead_process_list
echo "---------------"

start_process "$service_handler" "$dead_process_list"

