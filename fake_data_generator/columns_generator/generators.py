import pandas as pd
import sqlalchemy
from datetime import datetime, date, timedelta
from decimal import Decimal
from random import uniform
from rstr import xeger
from pandas import Series
# from pytz import timezone
from numpy.random import choice
from faker import Faker


def get_generator_for_nulls():
    output_size = yield
    while True:
        output_size = yield Series([None] * output_size)


def get_generator_for_categorical_column(values, probabilities):
    output_size = yield
    while True:
        fake_sample = choice(a=values, p=probabilities, size=output_size, replace=True)
        fake_series = Series(fake_sample, dtype=object)
        output_size = yield fake_series.where(fake_series.notna(), None)


def float_to_int():
    return int


def float_to_decimal(precision=0):
    if precision != 0:
        return lambda x: Decimal(str(round(x, precision)))
    else:
        return lambda x: Decimal(int(x))


def float_to_date():
    return lambda x: date.fromordinal(int(x))


def float_to_datetime(date_flag: bool = False):
    if date_flag:
        return lambda x: datetime.fromtimestamp(x).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return lambda x: datetime.fromtimestamp(x)


CONVERTERS_FROM_FLOAT = {
    'int': float_to_int,
    'decimal': float_to_decimal,
    'date': float_to_date,
    'datetime': float_to_datetime,
}


def get_generator_for_continuous_column(intervals,
                                        probabilities,
                                        output_data_type: str,
                                        params: dict = None):
    output_size = yield
    if params is None:
        params = {}
    multiplier_for_probabilities = 1 / sum(probabilities)
    norm_probabilities = list(map(lambda p: p * multiplier_for_probabilities, probabilities))
    applied_func = CONVERTERS_FROM_FLOAT.get(output_data_type)(**params)
    while True:
        fake_sample = map(lambda interval_index: applied_func(uniform(intervals[interval_index][0], intervals[interval_index][1])),
                          choice(a=len(intervals), size=output_size, p=norm_probabilities, replace=True))
        output_size = yield Series(fake_sample)


def get_generator_for_string_column(common_regex):
    output_size = yield
    while True:
        list_of_fake_strings = [xeger(common_regex) for _ in range(output_size)]
        output_size = yield Series(list_of_fake_strings)


def get_generator_for_current_dttm_column():
    output_size = yield
    while True:
        fake_timestamps = [datetime.now().replace(microsecond=0) + timedelta(hours=3) for _ in range(output_size)]
        output_size = yield Series(fake_timestamps)


def get_generator_for_fio_in_upper_case_column():
    output_size = yield
    faker = Faker('ru_RU')
    while True:
        fake_emails = [faker.name().upper() for _ in range(output_size)]
        output_size = yield Series(fake_emails)


def get_generator_for_fio_only_starting_with_upper_case_column():
    output_size = yield
    faker = Faker('ru_RU')
    while True:
        fake_emails = [faker.name() for _ in range(output_size)]
        output_size = yield Series(fake_emails)


def get_generator_for_email_column():
    output_size = yield
    faker = Faker()
    while True:
        fake_emails = [faker.email() for _ in range(output_size)]
        output_size = yield Series(fake_emails)


def get_generator_for_incremental_id_column(conn, table_name, incremental_id_column_name):
    output_size = yield
    if isinstance(conn, sqlalchemy.engine.base.Engine) and conn.name == 'postgresql':
        current_max_id_df = pd.read_sql_query(f'SELECT MAX({incremental_id_column_name}) AS id FROM {table_name}', conn)
    else:
        current_max_id_df = pd.DataFrame({'id': [None]})
    if current_max_id_df['id'][0] is not None:
        start_id = current_max_id_df['id'][0] + 1
    else:
        start_id = 1
    while True:
        output_size = yield Series([i for i in range(start_id, start_id + output_size)])
        start_id += output_size


def get_generator_for_foreign_key_column(conn, foreign_key_table_name, foreign_key_column_name):
    output_size = yield
    while True:
        if isinstance(conn, sqlalchemy.engine.base.Engine) and conn.name == 'postgresql':
            fk_df = pd.read_sql_query(
                f'SELECT {foreign_key_column_name} AS fk '
                f'FROM {foreign_key_table_name} '
                f'ORDER BY RANDOM() LIMIT {output_size}', conn
            )
        else:
            fk_df = pd.DataFrame({'fk': [None] * output_size})
        output_size = yield fk_df['fk'].sample(n=output_size, replace=True).reset_index(drop=True)
