#!/bin/bash

mkdir -p data
cd $1
go run ../flux.go
cd ..
Rscript sc.R
cp data/grid.txt $1.txt
rm -rf data
mv rls.rds $1.rds
