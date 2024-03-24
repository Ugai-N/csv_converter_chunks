import time
from concurrent.futures import as_completed, ProcessPoolExecutor
from itertools import repeat

import pandas as pd
import pyarrow as pa
from tqdm import tqdm

from converters.basic import BasicConverter
from bin.thread import convert_chunk


class ConverterProcessing(BasicConverter):
    """Converting CSV file to Parquet (PROCESSING)"""

    def run(self, chunksize):
        # scan csv and get chunks
        data = self.make_chunks(chunksize)
        chunks = data[0]
        pq_schema = data[1]

        # converting
        start_time = time.time()

        with ProcessPoolExecutor() as executor:
            # workers = 2 * multiprocessing.cpu_count()
            # futures = [executor.submit(convert_chunk, (i + 1), chunk, self.output, pq_schema)
            # for i, chunk in enumerate(csv_stream)]
            futures = []
            chunks_length = []
            for i, chunk in enumerate(chunks):
                chunks_length.append(len(chunk))
                with tqdm(total=len(chunk), desc=f'chunk {i + 1}') as pbar:
                    future = executor.submit(self.convert_chunk, (i + 1), chunk, pq_schema)
                    futures.append(future)
                    pbar.update(len(chunk))

            for future in as_completed(futures):
                print(f'{future.result()} ---> completed\n')
                # pbar.update()

        convert_time = time.time() - start_time
        self.converting_time = convert_time
        # print(f"CONVERTION TIME: {self.converting_time}")
        print(f'\nInfo after convertion:\n'
              f'total {len(chunks)} chunks\n'
              f'total {sum([int(i) for i in chunks_length])} rows\n')

    def run_map(self, chunksize):
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

        with tqdm(total=10) as pbar:
            with ProcessPoolExecutor() as executor:
                chunks = [chunk for chunk in csv_stream]
                results = executor.map(convert_chunk, range(len(chunks)), chunks, repeat(self.output),
                                       repeat(pq_schema))
                for i in results:
                    print(f'{i + 1} ---> initializing next one ---> {i + 2}\n')

            print('all done')
        convert_time = time.time() - start_time
        self.converting_time = convert_time
        # print(f"CONVERTION TIME: {self.converting_time}")
        print(f'\nInfo after convertion:\n'
              f'total {chunks_count} chunks\n'
              f'total {sum([int(i) for i in chunks_length])} rows\n')
