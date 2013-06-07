#!/bin/bash 
#set -x
source /data/services/mbtq/var/monitor/service.conf
source /data/services/mbtq/var/monitor/warnbysms.sh
source /data/services/mbtq/var/monitor/monitor_library.sh

dead_process=$( processmonitor )

local_ip="$(ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1):"
if [ "$dead_process"x != ""x ];then
	content=$local_ip" service $dead_process down"
	sendsms "$content"
fi


