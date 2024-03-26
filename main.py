from datetime import datetime
from multiprocessing import freeze_support
from pathlib import Path

from bin.thread_read_rows import ConverterThreading_test
from converters.thread import ConverterThreading
from utils import generate_csv_schema


def main():
    now = datetime.now()
    file_name = str(now).replace(" ", "_").replace(":", "-")[:19]
    # file_name = "tst2"
    # file_name = '2024-03-14_10-07-20dddd'

    output_path = Path(__file__).resolve().parent.joinpath('data', f'{file_name}')
    file_csv = Path(__file__).resolve().parent.joinpath('data', f'{str(file_name)}.csv')
    # file_csv = "data_dev/tst2.csv"
    # file_csv = "data_dev/old/2024-03-14_10-07-20dddd.csv"

    # generates csv with the stated n of rows
    generate_csv_schema(100000, file_csv)

    # choose the converter type
    # converter = ConverterProcessing(file_csv, output_path)
    converter = ConverterThreading(file_csv, output_path)
    # converter = ConverterSerial(file_csv, output_path)
    # converter = ConverterThreadingParallel(file_csv, output_path)
    # converter = ConverterThreading_test(file_csv, output_path)

    # initialize converter, input chunksize
    converter.run(100_000)

    # prints the info on convertion process
    converter.info()

    # converter.convert_back()


if __name__ == '__main__':
    freeze_support()
    main()
