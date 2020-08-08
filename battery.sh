capacity=`cat /sys/class/power_supply/BAT0/capacity`
charge_now="$((`cat /sys/class/power_supply/BAT0/charge_now`/1000))"
current_now="$((`cat /sys/class/power_supply/BAT0/current_now`/1000))"
voltage_now="$((`cat /sys/class/power_supply/BAT0/voltage_now`/1000))"
status=`cat /sys/class/power_supply/BAT0/status`
datetime=`date`
echo "$capacity, $charge_now, $current_now, $voltage_now, $datetime, $status" >> /home/anirudh/mybattery
