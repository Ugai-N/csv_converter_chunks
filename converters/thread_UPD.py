import csv
import time
from concurrent.futures import ThreadPoolExecutor

from converters.basic import BasicConverter
from utils import chunker0


class ConverterThreadingParallel(BasicConverter):
    """Converting CSV file to Parquet (THREADING --> parallel reading)"""

    def run(self, chunksize):
        # reading
        print('start reading')
        start_time = time.time()
        # scan/read csv and split into chunks:
        with open(self.input) as f:
            reader = csv.DictReader(f, delimiter=',')
            lst = list(reader)
        with ThreadPoolExecutor(4) as executor:
            chunks_qty = len(lst) // chunksize if len(lst) % chunksize == 0 else (len(lst) // chunksize) + 1
            start_pos_list = [i * chunksize for i in range(chunks_qty)]
            futures = []
            for index, start_pos in enumerate(start_pos_list):
                future = executor.submit(chunker0, index, start_pos, lst, chunksize)
                print(f"{future.result()[0]} --> start convertion")
                future.add_done_callback(self.convert_chunk0)
                futures.append(future)

        execution_time = time.time() - start_time
        self.execution_time = execution_time
