import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from converters.basic import BasicConverter


class ConverterThreading(BasicConverter):
    """Converting CSV file to Parquet (THREADING)"""

    def run(self, chunksize):
        # scan csv and get chunks
        data = self.make_chunks(chunksize)
        chunks = data[0]
        pq_schema = data[1]

        # converting
        start_time = time.time()

        with ThreadPoolExecutor(4) as executor:
            # futures = [executor.submit(convert_chunk, (i + 1), chunk, self.output, pq_schema)
            # for i, chunk in enumerate(csv_stream)]
            futures = []
            chunks_length = []
            for i, chunk in enumerate(chunks):
                chunks_length.append(len(chunk))
                with tqdm(total=len(chunk), desc=f'chunk {i + 1}') as pbar:
                    futures.append(executor.submit(self.convert_chunk, (i + 1), chunk, self.output, pq_schema))
                    pbar.update()

            for future in as_completed(futures):
                # print(f'{future.result()} ---> completed\n')
                pbar.update()

        convert_time = time.time() - start_time
        self.converting_time = convert_time
        # print(f"CONVERTION TIME: {self.converting_time}")
        print(f'\nInfo after convertion:\n'
              f'total {len(chunks)} chunks\n'
              f'total {sum([int(i) for i in chunks_length])} rows\n')
