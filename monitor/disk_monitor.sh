#!/bin/bash
#Data 2012-06-30

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games
MASK="/dev|/boot|/lib/init/rw|/sync_node|/mnt|/srv|/swap|/proc/bus/usb|/run"
LOGFILE=/data/services/mbtq/var/log/warnbysms.log
sendwarn=1
warnning=0

mobiles="
13570595934
15018799197
13926022585
18613079396
18664652719
"

function sendsms() {
  content=$1
  echo `date +%y-%m-%d" "%H:%M:%S` :  $content >>$LOGFILE
  for mobile in $mobiles
  do
    echo SEND  "$content" TO $mobile >>$LOGFILE
    sudo wget -T 10 -t 2 -SO /tmp/wget.html  "http://monitor.duowan.com:8081/monitor_interface.jsp?target=$mobile&action=sms&content=$content"
    sleep 10
  done
}

MOUNT_ARRAY=`df -hl|grep -v "tmpfs"|awk '{print $6}'|grep -v "Mounted"|grep -v "/lib/modules/*"|grep -v -E -w $MASK|sed "/^$/d"`
for MOUNT in `echo $MOUNT_ARRAY`
do
	USAGE=`df -hl|grep -w "$MOUNT\$"|awk '{print $5}'|cut -d % -f 1|grep -v "^$"`
	INODE=`df -il|grep -w "$MOUNT\$"|awk '{print $5}'|cut -d % -f 1|grep -v "^$"`
	
    if [ $USAGE -ge 95 ];then
		ip=`ifconfig | grep -A 1 'eth0' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1`
		content="disk alarm! $ip -- $MOUNT usage is larger than 95"
		sendsms "$content"
    fi
done
