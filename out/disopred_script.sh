#/usr/bin/env sh
FASTA_DIR=$1
N_PROC=$2
DISOPRED_CMD=../run_disopred.pl

for f in $FASTA_DIR/*.fasta; do
    echo "Running DISOPRED for $f"
    perl $DISOPRED_CMD $f $N_PROC
done
