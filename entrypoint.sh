#!/bin/bash

echo "Starting Docker Container"

declare -p | grep -Ev 'BASHOPTS|BASH_VERSINFO|EUID|PPID|SHELLOPTS|UID' > /container.env

touch /var/log/cron.log

chmod 0744 /etc/cron.d/covid
crontab /etc/cron.d/covid
cron -f