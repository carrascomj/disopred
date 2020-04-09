#!/usr/bin/python3
"""Run disopred across different processors.

Running from out/:
```
python3 disopred_multi.py ../../protein_production/data/interim/aa_sequences/ -n 32 -v
    path_to_data_dir: directory that contains the fasta files as input
    -n: number of processors to use
    -v: if verbose
```
"""
import argparse
import subprocess

from multiprocessing import Pool
from pathlib import Path


def _run_disopred(input_fasta: str) -> str:
    try:
        subprocess.run(
            ["perl", "../run_disopred.pl", input_fasta, "1"], check=True
        )
        return input_fasta
    except subprocess.CalledProcessError:
        return False


def run_in_processors(
    fasta_dir: str, num_processors: int, verbose: int
) -> list:
    """Disribute disopred across different processors."""
    pathlist = Path(fasta_dir).glob("**/*.fasta")
    all_fasta = [str(path) for path in pathlist]
    num_inputs = len(all_fasta)
    if verbose:
        print(f"Running for {num_inputs} FASTA files in {fasta_dir}.")
    chunk_size = num_inputs // num_processors
    pool = Pool(num_processors)
    results = pool.imap_unordered(
        _run_disopred, all_fasta, chunksize=chunk_size
    )
    pool.close()
    pool.join()
    failed = [result for result in results if result]
    if verbose:
        print(f"{len(failed)} has failed out of {num_inputs} inputs.")
    return failed


def parse_arguments() -> argparse.ArgumentParser:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Run disopred across different processors."
    )
    parser.add_argument(
        "dir_fasta",
        metavar="FASTA_DIR",
        type=str,
        help="directory that contains the fasta files as input.",
    )
    parser.add_argument(
        "--num-processors",
        "-n",
        metavar="NC",
        type=int,
        default=1,
        help="number of processors to use",
    )
    parser.add_argument("--verbose", "-v", action="count", default=0)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_arguments()
    failed = run_in_processors(
        args.dir_fasta, args.num_processors, args.verbose
    )
    if args.verbose:
        print("FILES failed: ", failed)
