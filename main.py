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
    file_name = 'one_mil_2'
    file_name = 'test100'
    output_path = Path(__file__).resolve().parent.joinpath('data', f'{file_name}')
    file_csv = Path(__file__).resolve().parent.joinpath('data', f'{str(file_name)}.csv')
    file_csv = 'data_dev/one_mil_2.csv'
    file_csv = 'data_dev/test100.csv'

    # generate_csv_schema(100000, file_csv)

    converter = ConverterProcessing(file_csv, output_path)
    # converter = ConverterThreading(file_csv, output_path)
    # converter = ConverterSerial(file_csv, output_path)
    # converter = ConverterThreadingParallel(file_csv, output_path)

    converter.run(8_000)

    converter.info()

    # converter.convert_back()


if __name__ == '__main__':
    freeze_support()
    main()
