from datetime import datetime
from multiprocessing import freeze_support
from pathlib import Path

from converters.process import ConverterProcessing
from converters.thread import ConverterThreading
from converters.thread_UPD import ConverterThreadingParallel
from utils import generate_csv_schema


def main():
    now = datetime.now()
    file_name = str(now).replace(" ", "_").replace(":", "-")[:19]

    output_path = Path(__file__).resolve().parent.joinpath('data', f'{file_name}')
    file_csv = Path(__file__).resolve().parent.joinpath('data', f'{str(file_name)}.csv')

    # generates csv with the stated n of rows
    generate_csv_schema(100000, file_csv)

    # choose the converter type
    converter = ConverterProcessing(file_csv, output_path)
    # converter = ConverterThreading(file_csv, output_path)
    # converter = ConverterSerial(file_csv, output_path)
    # converter = ConverterThreadingParallel(file_csv, output_path)

    # initialize converter, input chunksize
    converter.run(8_000)

    # prints the info on convertion process
    converter.info()

    # converter.convert_back()


if __name__ == '__main__':
    freeze_support()
    main()
