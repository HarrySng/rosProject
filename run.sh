#!/bin/bash

cd ncfiles
mkdir backupnc

for file in *.nc; do
    echo "Calculating RoS for $file"
    fname=$(echo "$file" | rev | cut -d'.' -f 2- | rev)
    ncpdq -a lat,lon,time ${file} ${fname}_d.nc
    d=$(echo "$file" | cut -d'_' -f 3-4 | cut -d'.' -f 1)
    mkdir -p $d
    #go run ../ros.go ${fname}_d.nc
    touch 1.txt 2.txt
    mv *.txt ./$d/
    mv $file ./backupnc/
done

