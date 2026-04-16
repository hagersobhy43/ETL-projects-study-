#!/bin/bash

city=Cairo

# extract current observed temperature
obs_tmp=$(curl -s "wttr.in/$city?T" | head -n 13 | tail -n 1 | cut -d "C" -f2 | grep -Eo '[[:digit:]].*')
echo "Current weather in $city: ${obs_tmp}C"

# extract forecast temperature
fc_temp=$(curl -s "wttr.in/$city?T" | head -n 23 | tail -n 1 | cut -d "C" -f2 | grep -Eo '[[:digit:]].*')
echo "Forecast for $city: ${fc_temp}C"

# get current date in Cairo timezone
day=$(TZ='Africa/Cairo' date +%d)
month=$(TZ='Africa/Cairo' date +%m)
year=$(TZ='Africa/Cairo' date +%Y)

# build and log the record
record=$(echo -e "$year\t$month\t$day\t${obs_tmp}C\t${fc_temp}C")
echo "$record" >> /app/rx_poc.log
