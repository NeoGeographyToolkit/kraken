#!/bin/bash
n=$1

screen -dm -S reapers # create a screen session in the background named "reapers"

for i in {1..n}
do
   screen -S reapers -X screen ./reaper.py # attach processes to the existing session
done
