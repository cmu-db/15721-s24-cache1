# This file is used to decode a parquet file and display
# the first n rows of the file, mainly for visualization
# purposes.
#
# Usage:
# python decode_parquet.py <path-to-parquet-file> --n
# <number-of-rows-to-display>
#

from IPython.display import display
import pandas as pd
import argparse


def decode_parquet(file_path, n=5):
    df = pd.read_parquet(file_path)
    print('---------- Statistics ----------')
    print(df.describe(), '\n')
    print(f'---------- First {n} rows of the parquet file ----------')
    display(df.head(n), '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Decode parquet file')
    parser.add_argument('file_path', type=str, help='path to parquet file')
    parser.add_argument(
        '--n',
        type=int,
        default=5,
        help='number of rows to display',
        required=False)
    args = parser.parse_args()
    decode_parquet(args.file_path, args.n)
