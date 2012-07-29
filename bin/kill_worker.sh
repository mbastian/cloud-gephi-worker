#!/bin/bash

for p in $( ps aux | grep -e 'cloud-worker' | grep -v grep | awk '{print $2}' )
do
    kill -9 $p
done

for p in $( ps aux | grep -e 'cloud.worker' | grep -v grep | awk '{print $2}' )
do
    kill -9 $p
done
