#!/bin/bash 

rm worker.log
echo "Running worker..."
nohup ./cloud-worker > worker.log 2>&1 &
echo "Started! "

