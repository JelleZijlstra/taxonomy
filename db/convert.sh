#!/bin/bash
indir=$1
outdir=$2

for file in $indir/*.ods; do
	outfile=${file/$indir/$outdir}
	outfile=${outfile/\.ods/.csv}
	unoconv -f csv -o $outfile $file
done
