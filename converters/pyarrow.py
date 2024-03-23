import pathlib
import time

from tqdm import tqdm

from converters.basic import BasicConverter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from pathlib import Path
import polars as pl


class PyArrowConverterPolars(BasicConverter):
    """Converting CSV file to Parquet via PyArrow"""
    def run(self, chunksize):
        start_time = time.time()
        reader = pl.read_csv(self.input, batch_size=chunksize)
        ddd = pl.scan_csv(self.input)
        # reader = pl.read_csv_batched(self.input)
        # batches = reader.next_batches(10)
        self.reading_time = time.time() - start_time

        # Convert to Parquet
        start_time = time.time()
        for i, df in enumerate(reader):
            chunk_file = Path(__file__).resolve().parent.joinpath(f'{self.output}', f'parquet_{i + 1}.csv')
            ddd.sink_parquet(chunk_file)
        self.converting_time = time.time() - start_time

    def convert_back(self):
        dataset = pq.ParquetDataset(f'{self.output}')
        input_path = Path(self.input)
        converted_back_file = pathlib.Path.joinpath(input_path.parent, f'{input_path.stem}_AFTER_CONV.csv')
        dataset.read().to_pandas().to_csv(converted_back_file, index=False)

        print(f"Info on BACK convertion:\n"
              f"total {len(pd.read_csv(converted_back_file))} rows\n\n")


class ConverterSerial(BasicConverter):
    """Converting CSV file to Parquet (SERAIL)"""

    def read(self, chunksize):  # из этого сделать валидацию с проверкой строк и общей инфой?
        start_time = time.time()
        csv_stream = pd.read_csv(self.input, sep='\t', chunksize=chunksize, low_memory=False)
        read_time = time.time() - start_time

        self.reading_time = read_time
        print(f"reading: {self.reading_time}\n")

        chunks_count = 0
        chunks_length = []
        for i, ch in enumerate(csv_stream):
            # print(f"{i + 1}_chunk is: {ch}")
            # print(ch.info())
            # print(ch.index)
            chunks_length.append(ch.shape[0])
            chunks_count += 1
        print(f'total {chunks_count} chunks\n'
              f'total {sum([int(i) for i in chunks_length])} rows\n')
        return csv_stream
        # return csv_stream, chunks_count, chunks_length

    def run(self, chunksize):
        # reading
        print('start reading')
        start_time = time.time()
        csv_stream = pd.read_csv(self.input, sep=',', chunksize=chunksize, low_memory=False)
        pq_schema = pa.Schema.from_pandas(pd.read_csv(self.input, sep=',', low_memory=False))
        read_time = time.time() - start_time
        self.reading_time = read_time
        print('stop reading')
        # print(f"READING TIME: {self.reading_time}")

        # converting
        start_time = time.time()
        chunks_count = 0
        chunks_length = []
        # parquet_schema = None
        for i, chunk in tqdm(enumerate(csv_stream)):
            # if i == 0:
            #     parquet_schema = pa.Table.from_pandas(df=chunk).schema

            chunk_file = Path(__file__).resolve().parent.joinpath(f'{self.output}', f'parquet_{i + 1}.parquet')
            chunks_length.append(chunk.shape[0])
            chunks_count += 1

            # Open a Parquet file for writing the headers
            parquet_writer = pq.ParquetWriter(chunk_file, pq_schema, compression='snappy')

            # Write CSV chunk to the parquet file
            table = pa.Table.from_pandas(chunk, schema=pq_schema)
            parquet_writer.write_table(table)
            parquet_writer.close()

        convert_time = time.time() - start_time
        self.converting_time = convert_time
        # print(f"CONVERTION TIME: {self.converting_time}")
        print(f'\nInfo after convertion:\n'
              f'total {chunks_count} chunks\n'
              f'total {sum([int(i) for i in chunks_length])} rows\n')
