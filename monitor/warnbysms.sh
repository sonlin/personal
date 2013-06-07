LOGFILE=/data/services/mbtq/var/log/warnbysms.log
mobiles="
18613079396
#13570595934
#15018799197
#13926022585
#18664652719
"
source /data/services/mbtq/var/monitor/service.conf

function sendsms() {
  content=$1
  echo `date +%y-%m-%d" "%H:%M:%S` :  $content >>$LOGFILE

  for mobile in $mobiles
  do
    echo SEND  "$content" TO $mobile >>$LOGFILE
    sudo wget -T 10 -t 2 -SO /tmp/wget.html  "http://monitor.duowan.com:8081/monitor_interface.jsp?target=$mobile&action=sms&content=$content"
    sleep 5
  done
}

