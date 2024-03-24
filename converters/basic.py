import csv
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa

from utils import chunker


class BasicConverter(ABC):
    """Abstract base class for different converting tools"""

    def __init__(self, input_file, output_path):
        self.input = input_file
        self.output = self.create_dir(output_path)
        self.__reading_time = None
        self.__converting_time = None
        self.__execution_time = None

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def info(self):
        if self.execution_time is not None:
            print(f"{self.__class__.__name__} (execution time): {self.execution_time:.2f} seconds")
            if self.reading_time is not None and self.converting_time is not None:
                print(f"including:\n{self.__class__.__name__} (reading time): {self.reading_time:.2f} seconds")
                print(f"{self.__class__.__name__} (converting time): {self.converting_time:.2f} seconds")
        else:
            raise Exception("Seems that the converter has not been launched")

        print(f"\nInput file had: {len(list(csv.reader(open(self.input), delimiter=','))) - 1} rows"
              f"\nOutput dir has: {pq.read_table(f'{self.output}').shape[0]} rows in {len(os.listdir(self.output))} files")

    def convert_back(self):
        # start_time = time.time()
        table = pq.read_table(f'{self.output}')
        print(f"Info on BACK convertion:\n"
              f"total {table.shape[0]} rows\n\n")
        # convert_time = time.time() - start_time
        # print(f'{convert_time}')

        # start_time = time.time()
        dataset = pq.ParquetDataset(f'{self.output}')
        # # print(dataset.read().to_pandas())
        input_path = Path(self.input)
        converted_back_file = Path.joinpath(input_path.parent, f'{input_path.stem}_AFTER_CONV.csv')
        dataset.read().to_pandas().to_csv(converted_back_file, index=False)

        print(f"Info on BACK convertion:\n"
              f"total {len(list(csv.reader(open(converted_back_file), delimiter=','))) - 1} rows\n\n")
        # convert_time = time.time() - start_time
        # print(f'{convert_time}')

    def make_chunks(self, chunksize):
        # reading
        print('start reading')
        start_time = time.time()

        # scan/read csv and split into chunks:

        # ## option1 - too long
        # csv_stream = pd.read_csv(self.input, sep=',', chunksize=chunksize, low_memory=False)
        ## option2:
        with open(self.input) as f:
            # reader = csv.reader(f, delimiter=',')
            # list_of_column_names = next(reader)
            reader = csv.DictReader(f, delimiter=',')
            lst = list(reader)
        chunks = [group for group in chunker(lst, chunksize)]

        # take dataset schema from input csv
        dataset = ds.dataset(self.input, format="csv")
        pq_schema = dataset.schema

        read_time = time.time() - start_time
        self.reading_time = read_time
        print('stop reading')
        return chunks, pq_schema

    def create_dir(self, output):
        os.makedirs(output, exist_ok=True)
        return output

    def convert_chunk(self, num, chunk, parquet_schema):
        # print(type(chunk))

        # generate output file name
        chunk_file = Path(__file__).resolve().parent.joinpath(f'{self.output}', f'parquet_{num}.parquet')

        # generate table
        table = pa.Table.from_pylist(chunk)
        # print(table)
        # table = pa.Table.from_pylist(chunk, schema=parquet_schema)
        # table = pa.Table.from_pandas(chunk, schema=parquet_schema) --> when chunks formed from pd.read

        # write table into parquet
        pq.write_table(table, chunk_file)
        return num

    def convert_chunk0(self, future):
        # print(type(chunk))
        num, chunk = future.result()[0], future.result()[1]
        # generate output file name
        chunk_file = Path(__file__).resolve().parent.joinpath(f'{self.output}', f'parquet_{num}.parquet')

        # generate table
        table = pa.Table.from_pylist(chunk)
        # print(table)
        # table = pa.Table.from_pylist(chunk, schema=parquet_schema)
        # table = pa.Table.from_pandas(chunk, schema=parquet_schema) --> when chunks formed from pd.read

        # write table into parquet
        pq.write_table(table, chunk_file)
        print(f'{num} ---> finished writing parquet')
        return num

    @abstractmethod
    def run(self, *kwargs):
        pass

    @property
    def reading_time(self):
        return self.__reading_time

    @reading_time.setter
    def reading_time(self, value):
        self.__reading_time = value

    @property
    def converting_time(self):
        return self.__converting_time

    @converting_time.setter
    def converting_time(self, value):
        self.__converting_time = value

    @property
    def execution_time(self):
        if self.__execution_time is None:
            if self.__converting_time is not None and self.__reading_time is not None:
                return self.__converting_time + self.__reading_time
        return self.__execution_time

    @execution_time.setter
    def execution_time(self, value):
        self.__execution_time = value
