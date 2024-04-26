"""
This script generates access pattern for a set of files with a Zipfian distribution.

Usage:
    python scripts/generate_traces.py --num_files 10 --skew_param 1.2 --num_accesses 100 -o <YOUR_OUTPUT_FILE>
"""

import numpy as np
import csv
import argparse
import random

mp = {1: [1, 2, 3, 4, 5, 6, 7, 8, 9], 100: [
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19]}


def generate_access_counts(num_files, s, num_accesses, file_size):
    access_counts = []
    # Generate Zipfian distribution probabilities
    probabilities = 1 / np.arange(1, num_files + 1) ** s
    # Normalize
    probabilities /= np.sum(probabilities)

    # Simulate file accesses
    for _ in range(num_accesses):
        file_index = np.random.choice(mp[file_size], p=probabilities)
        access_counts.append(file_index)

    return access_counts


def write_to_csv(access_counts, output_file):
    with open(output_file, "w") as f:
        writer = csv.writer(f)
        timestamp = 0  # timestamp is in milliseconds
        for file_index in access_counts:
            writer.writerow([timestamp, file_index])
            timestamp += random.randint(1, 500)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate access counts with a Zipfian distribution")
    parser.add_argument(
        "--num_files",
        type=int,
        default=10,
        help="Number of files (default: 10)")
    parser.add_argument(
        "-s",
        "--skew_param",
        type=float,
        default=1.5,
        help="Skew parameter (default: 1.5)")
    parser.add_argument(
        "--num_accesses",
        type=int,
        default=20,
        help="Number of accesses (default: 20)")
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        default="data/traces/trace_1m.csv",
        help="Output CSV file (default: data/trace.csv)")
    parser.add_argument(
        "--size",
        type=int,
        default=1,
        help="Size of the parquet file in MB"
    )
    args = parser.parse_args()

    if args.size not in mp:
        raise ValueError("Size should be either 1 or 100")

    access_counts = generate_access_counts(
        args.num_files, args.skew_param, args.num_accesses, args.size)
    write_to_csv(access_counts, args.output_file)

    print("Access counts generated and written to", args.output_file)
