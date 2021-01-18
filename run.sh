#!/bin/bash

cd ncfiles
mkdir backupnc

for file in *.nc; do
    echo "Calculating RoS for $file" # File name without extension
    fname=$(echo "$file" | rev | cut -d'.' -f 2- | rev) # File name without extension
    ncpdq -a lat,lon,time ${file} ${fname}_d.nc; mv ${fname}_d.nc ${file} # Flip dimensions
    ncks --fix_rec_dmn lat ${file} -o ${fname}_d.nc; mv ${fname}_d.nc ${file} # Fix lat (not UNLIMITED)
    ncks --mk_rec_dmn time ${file} -o ${fname}_d.nc; mv ${fname}_d.nc ${file} # Make time UNLIMITED again
    d=$(echo "$file" | cut -d'_' -f 3-4 | cut -d'.' -f 1) # Extract string for naming directory
    mkdir -p $d # Create directory
    go run ../ros.go $file # Extract data and split it into grid-wise txt files
    mv *.txt ./$d/ # Copy all txt files to result directory
    mv $file ./backupnc/ # Move original file to backup
done