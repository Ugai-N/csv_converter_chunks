import csv
import datetime
import random
import string
import time
import pyarrow.csv as pacsv

import pandas as pd
import pyarrow as pa
from faker import Faker
from random import randint
from tqdm import tqdm

fake = Faker()


def generate_csv_schema(lines_qty, file_csv):  # (1.45 sec for 1000) & (155,26 for 100_000) & (7235 for 1 mln)
    """Generating CSV file with random data"""
    start_time = time.time()

    # create the schema:
    defined_headers = [
        pa.field('1:id_uni(int)', pa.int64()),
        pa.field('2:name', pa.string()),
        pa.field('3:email', pa.string()),
        pa.field('4:qty(int)', pa.int64()),
        pa.field('5:this_year_date', pa.date64()),
        pa.field('6:datetime_this_year', pa.timestamp('s')),
        pa.field('7:country', pa.string()),
        pa.field('8:address', pa.string()),
        pa.field('9:rate', pa.float64()),
        pa.field('10:notes(txt)', pa.string())
    ]
    for y in range(50):
        defined_headers.append(pa.field(f'{y+11}:lexify', pa.string()))
    my_schema = pa.schema(defined_headers)

    # create the list of lines:
    pylist = []
    for i in tqdm(range(0, lines_qty)):
        line = {
            my_schema.field(0).name: int(fake.unique.random_number(digits=len(str(lines_qty)), fix_len=False)),
            my_schema.field(1).name: fake.name(),
            my_schema.field(2).name: fake.email(),
            my_schema.field(3).name: int(randint(1, lines_qty)),
            my_schema.field(4).name: fake.date_this_year(),
            my_schema.field(5).name: fake.date_time_this_year(),
            my_schema.field(6).name: fake.country(),
            my_schema.field(7).name: fake.address(),
            my_schema.field(8).name: fake.pyfloat(positive=True, max_value=10, right_digits=2),
            my_schema.field(9).name: fake.text(max_nb_chars=50)
        }
        for y in range(50):
            line[f'{y + 11}:lexify'] = fake.lexify(text='Code: ??????????')
        pylist.append(line)

        if (i+1) % 10000 == 0:
            print(f'created: {i+1} rows')

    # create the Table:
    tab = pa.Table.from_pylist(pylist, schema=my_schema)
    # print(tab)
    # print(tab.schema)

    # write CSV:
    options = pacsv.WriteOptions(include_header=True)
    pacsv.write_csv(tab, file_csv, options)

    generating_time = time.time() - start_time
    print(f'generating_time: {generating_time}')

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
#
# def generate_csv_from_pddf(x, file_csv):  # 16 sec for 1000
#     """Generating CSV file with random data (qty of lines -> 100 ** 3)"""
#     # pandas dataframe
#     start_time = time.time()
#
#     data = pd.DataFrame()
#     for i in range(0, x):
#         data.loc[i, '1:id_uni(int)'] = int(fake.unique.random_number(digits=len(str(x)), fix_len=False))
#         data.loc[i, '2:name'] = fake.name()
#         data.loc[i, '3:email'] = fake.email()
#         data.loc[i, '4:qty(int)'] = int(randint(1, x))
#         data.loc[i, '5:this_year_date'] = fake.date_this_year()
#         data.loc[i, '6:datetime_this_year'] = fake.date_time_this_year()
#         data.loc[i, '7:country'] = fake.country()
#         data.loc[i, '8:address'] = fake.address()
#         data.loc[i, '9:latitude'] = fake.latitude()
#         data.loc[i, '10:notes(txt)'] = fake.text(max_nb_chars=50)
#         for y in range(50):
#             data.loc[i, f'{y+11}:lexify'] = fake.lexify(text='Code: ??????????')
#
#         if (i+1) % 100 == 0:
#             print(f'created: {i+1} rows')
#     res = pa.Table.from_pandas(data)
#     options = pacsv.WriteOptions(include_header=True)
#     pacsv.write_csv(res, file_csv, options)
#
#     generating_time = time.time() - start_time
#     print(f'generating_time: {generating_time}')


def generate_csv_panda(x, file_csv):  # 15 sec for 1000
    """Generating CSV file with random data (qty of lines -> 100 ** 3)"""
    # pandas dataframe
    start_time = time.time()
    data = pd.DataFrame()
    for i in range(0, x):
        data.loc[i, '1:id_uni(int)'] = int(fake.unique.random_number(digits=len(str(x)), fix_len=False))
        data.loc[i, '2:name'] = fake.name()
        data.loc[i, '3:email'] = fake.email()
        data.loc[i, '4:qty(int)'] = int(randint(1, x))
        data.loc[i, '5:this_year_date'] = fake.date_this_year()
        data.loc[i, '6:datetime_this_year'] = fake.date_time_this_year()
        data.loc[i, '7:country'] = fake.country()
        data.loc[i, '8:address'] = fake.address()
        data.loc[i, '9:latitude'] = fake.latitude()
        data.loc[i, '10:notes(txt)'] = fake.text(max_nb_chars=50)
        for y in range(50):
            data.loc[i, f'{y+11}:lexify'] = fake.lexify(text='Code: ??????????')

        if (i+1) % 100 == 0:
            print(f'created: {i+1} rows')
    data.to_csv(file_csv, index=False)
    generating_time = time.time() - start_time
    print(f'generating_time: {generating_time}')


def ff():
    print((10 ** 6) // 10)
    print(fake.pystr(min_chars=10, max_chars=255))
    print(fake.text(max_nb_chars=50))


def read(file):
    # parse_options = pacsv.ParseOptions(delimiter="\t", quote_char="^")
    df = pd.read_csv(file)
    # df = pd.read_csv(file, sep=',')
    # for col in df:
    #     print(f"----> {col}")
    print(df)



def read2(file):
    with open(file) as csv_file:
        hdr = csv.Sniffer().has_header(csv_file.read())
        csv_file.seek(0)
        r = csv.reader(csv_file)
        # mailing_list = []
        if hdr:
            next(r)
        for row in r:
            print(row)
            if '\n' in row:
                print('found')
            # mailing_list.append(row)

    # mailing_list