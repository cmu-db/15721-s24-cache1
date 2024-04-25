"""
This script generates access pattern for a set of files with a Zipfian distribution.

Usage:
    python scripts/generate_traces.py --num_files 10 --skew_param 1.2 --num_accesses 100 -o <YOUR_OUTPUT_FILE>
"""

import numpy as np
import csv
import argparse
import random


def generate_access_counts(num_files, s, num_accesses):
    access_counts = [0] * num_files
    # Generate Zipfian distribution probabilities
    probabilities = 1 / np.arange(1, num_files + 1) ** s
    # Normalize
    probabilities /= np.sum(probabilities)

    # Simulate file accesses
    for _ in range(num_accesses):
        file_index = np.random.choice(np.arange(num_files), p=probabilities)
        access_counts[file_index] += 1

    return access_counts


def write_to_csv(access_counts, output_file):
    with open(output_file, "w") as f:
        writer = csv.writer(f)
        timestamp = 0  # timestamp is in milliseconds
        for i, count in enumerate(access_counts):
            # timestamp, filename, access_count
            writer.writerow(
                [timestamp, f"random_data_100m_{i}.parquet", count])
            timestamp += random.randint(1, 500)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate access counts with a Zipfian distribution")
    parser.add_argument(
        "-nf",
        "--num_files",
        type=int,
        default=10,
        help="Number of files (default: 10)")
    parser.add_argument(
        "-s",
        "--skew_param",
        type=float,
        default=1.2,
        help="Skew parameter (default: 1.2)")
    parser.add_argument(
        "-na",
        "--num_accesses",
        type=int,
        default=100,
        help="Number of accesses (default: 100)")
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        default="data/trace_100m.csv",
        help="Output CSV file (default: data/trace.csv)")
    args = parser.parse_args()

    access_counts = generate_access_counts(
        args.num_files, args.skew_param, args.num_accesses)
    write_to_csv(access_counts, args.output_file)

    print("Access counts generated and written to", args.output_file)
