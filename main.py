import os
from datetime import datetime
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
from converters.pyarrow import PyArrowConverter, PyArrowConverterPanda
from utils import generate_csv_panda, ff, read, read2, generate_csv_schema

now = datetime.now()
file_name = str(now).replace(" ", "_").replace(":", "-")[:19]
# file_name = 'bigfile.csv'
output_path = Path(__file__).resolve().parent.joinpath('data', f'{file_name}')
file_csv = Path(__file__).resolve().parent.joinpath('data', f'{str(file_name)}.csv')
# file_csv = 'data/old/bigfile.csv'

# file_csv = Path(__file__).resolve().parent.joinpath(f'{output_path}', 'CSV_file.csv')
# file_pq = Path(__file__).resolve().parent.joinpath('data', f'{str(file_name)}.parquet')


generate_csv_schema(10_000, file_csv)

converter = PyArrowConverterPanda(file_csv, output_path)
# stream = converter.read(20000)
converter.run(3_000)

# converter.info()

converter.convert_back()


# generate_csv(10**3, file_csv)
# ff()
# read('data/2024-03-14_10-07-20.csv')
# read('data/2024-03-14_10-07-20dddd.csv')
# read2('data/2024-03-14_10-07-20.csv')
# read2('data/2024-03-14_10-07-20dddd.csv')


# dataset = pq.ParquetDataset(f'{output_path}')
# print(dataset.read().to_pandas())
# dataset.read().to_pandas().to_csv(f'data/{file_name}_AFTER_CONV.csv', index=False)