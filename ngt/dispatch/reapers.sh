#!/bin/bash
n=$1

screen -dm -S reapers # create a screen session in the background named "reapers"

#for i in `seq 1 $n`
for (( i=1; i<=$n; i++))
do
   echo "launching reaper $i"
   screen -S reapers -X screen ./reaper.py # attach processes to the existing session
done
