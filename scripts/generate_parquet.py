# Generate a random CSV file and convert it to Parquet format

import csv
import random
import sys
import pandas as pd
import fastparquet as fp
from IPython.display import display
import os

def generate_random_csv(filename, num_rows, num_cols):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow([f'Column_{i}' for i in range(1, num_cols+1)])
        # Write data
        for _ in range(num_rows):
            row = [random.randint(1, 100) for _ in range(num_cols)]
            writer.writerow(row)

def validate_parquet(filename, n=5):
    df = pd.read_parquet(filename, engine='fastparquet')
    print(f"-------- The first {n} rows of the generated parquet --------")
    display(df.head(n))

if __name__ == "__main__":
    if len(sys.argv) == 1:
        num_rows = 10
        num_cols = 5
    elif len(sys.argv) == 3:
        try:
            num_rows = int(sys.argv[1])
            num_cols = int(sys.argv[2])
        except ValueError:
            print("Error: Invalid arguments. Please provide integer values for number of rows and columns.")
            sys.exit(1)
    else:
        print("Usage: python generate_parquet.py [num_rows] [num_cols]")
        sys.exit(1)
    
    if not os.path.exists("static"):
        os.makedirs("static")

    filename = os.path.join("static", "random_data.csv")
    generate_random_csv(filename, num_rows, num_cols)
    print(f"Random CSV file '{filename}' generated with {num_rows} rows and {num_cols} columns.")

    # TODO We can actually specify the number of row groups here
    parquet_filename = os.path.join("static", "random_data.parquet")
    df = pd.read_csv(filename)
    fp.write(parquet_filename, df)
    print(f"CSV file converted to Parquet: '{parquet_filename}'")

    validate_parquet(parquet_filename)
