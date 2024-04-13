"""
This script generates random parquet files given the number of rows, columns,
and files.

Usage:
    python scripts/generate_parquet.py -d data -r 5500 -c 20 -n 5
"""
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import argparse
from tqdm import tqdm


def generate_random_parquet_files(output_dir, num_rows, num_cols, num_files):
    for i in tqdm(range(num_files)):
        data = pd.DataFrame(np.random.rand(num_rows, num_cols))
        data.columns = [f'col {i + 1}' for i in range(num_cols)]
        table = pa.Table.from_pandas(data)
        file_name = os.path.join(output_dir, f"random_data_{i}.parquet")
        pq.write_table(table, file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate random parquet files')
    parser.add_argument(
        '-d',
        '--dir',
        type=str,
        help='output directory for parquet files')
    parser.add_argument(
        '-r',
        '--row',
        type=int,
        help='number of rows in each file',
        default=5500,  # Roughly 1MB
        required=False)
    parser.add_argument(
        '-c',
        '--col',
        type=int,
        help='number of columns in each file',
        default=20,
        required=False)
    parser.add_argument(
        '-n',
        type=int,
        help='number of files to generate',
        default=5,
        required=False)
    args = parser.parse_args()

    generate_random_parquet_files(args.dir, args.row, args.col, args.n)
