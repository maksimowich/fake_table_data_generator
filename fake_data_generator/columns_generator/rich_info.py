import re
from datetime import datetime
from loguru import logger
from fake_data_generator.columns_generator.column import \
    Column, CategoricalColumn, ContinuousColumn, StringFromRegexColumn, CurrentTimestampColumn, \
    FioInUpperCaseColumn, FioOnlyStartingWithUpperCaseColumn, EmailColumn
from fake_data_generator.columns_generator.info_for_columns import \
    get_info_for_categorical_column,\
    get_info_for_continuous_column,\
    get_info_for_string_column
from fake_data_generator.columns_generator.generators import \
    get_generator_for_nulls,\
    get_generator_for_categorical_column,\
    get_generator_for_continuous_column, \
    get_generator_for_string_column, \
    get_generator_for_current_dttm_column, \
    get_generator_for_fio_in_upper_case_column, \
    get_generator_for_fio_only_starting_with_upper_case_column, \
    get_generator_for_email_column, \
    get_generator_for_incremental_id_column, \
    get_generator_for_foreign_key_column


def get_input_data_type(data_type):
    if 'decimal' in data_type or 'numeric' in data_type:
        return 'float'
    elif 'int' in data_type:
        return 'int'
    elif 'date' == data_type:
        return 'date'
    elif 'timestamp' in data_type:
        return 'datetime'


def get_output_data_type(data_type):
    if 'decimal' in data_type or 'numeric' in data_type:
        return 'decimal'
    elif 'int' in data_type:
        return 'int'
    elif 'date' == data_type:
        return 'date'
    elif 'timestamp' in data_type:
        return 'datetime'

REGEX_FOR_FIO_IN_UPPER_CASE = r'[А-Я]{2,} [А-Я]{2,} [А-Я]{2,}\Z'
REGEX_FOR_FIO_ONLY_STARTING_WITH_UPPER_CASE = r'[А-Я][а-я]+ [А-Я][а-я]+ [А-Я][а-я]+\Z'
REGEX_FOR_EMAIL = r'[.A-Za-z0-9_-]+@[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\Z'


def get_string_column_class(strings):
    matchings_cnt_fio_in_upper_case = 0
    matchings_cnt_fio_only_starting_with_upper_case = 0
    matchings_cnt_email = 0
    for string in strings:
        if re.match(REGEX_FOR_FIO_IN_UPPER_CASE, string):
            matchings_cnt_fio_in_upper_case += 1
        elif re.match(REGEX_FOR_FIO_ONLY_STARTING_WITH_UPPER_CASE, string):
            matchings_cnt_fio_only_starting_with_upper_case += 1
        elif re.match(REGEX_FOR_EMAIL, string):
            matchings_cnt_email += 1
    if matchings_cnt_fio_in_upper_case / len(strings) > 0.8:
        return FioInUpperCaseColumn
    elif matchings_cnt_fio_only_starting_with_upper_case / len(strings) > 0.8:
        return FioOnlyStartingWithUpperCaseColumn
    elif matchings_cnt_email / len(strings) > 0.8:
        return EmailColumn


def get_rich_column_info(column_values,
                         column_info,
                         number_of_intervals,
                         categorical_threshold):
    column_data_type = column_info.get_data_type()
    column_name = column_info.get_column_name()
    categorical_column_flag = (isinstance(column_info, CategoricalColumn) or (column_values.nunique() / column_values.count() < categorical_threshold) or column_values.nunique() in [0, 1]) and \
        'decimal' not in column_data_type and 'numeric' not in column_data_type and type(column_info) in [Column, CategoricalColumn]

    if categorical_column_flag:
        logger.info(f'Column "{column_name}" — CATEGORICAL COLUMN')
        if not isinstance(column_info, CategoricalColumn):
            column_info = CategoricalColumn(column_name=column_name, data_type=column_data_type)
        if column_info.get_values() is None or column_info.get_probabilities() is None:
            values, probabilities = get_info_for_categorical_column(column_values)
            column_info.set_values(values)
            column_info.set_probabilities(probabilities)
        generator = get_generator_for_categorical_column(values=column_info.get_values(),
                                                         probabilities=column_info.get_probabilities())

    elif column_data_type == 'string' or 'varchar' in column_data_type:
        detected_string_class = None
        if not isinstance(column_info, StringFromRegexColumn):
            detected_string_class = get_string_column_class(column_values.dropna())
            if detected_string_class is not None:
                column_info = detected_string_class(column_name=column_name, data_type=column_data_type)
                if FioInUpperCaseColumn == detected_string_class:
                    logger.info(f'Column "{column_name}" — FIO_IN_UPPER_CASE')
                    generator = get_generator_for_fio_in_upper_case_column()
                elif FioOnlyStartingWithUpperCaseColumn == detected_string_class:
                    logger.info(f'Column "{column_name}" — FIO_ONLY_STARTING_WITH_UPPER_CASE')
                    generator = get_generator_for_fio_only_starting_with_upper_case_column()
                elif EmailColumn == detected_string_class:
                    logger.info(f'Column "{column_name}" — EMAIL')
                    generator = get_generator_for_email_column()

        if detected_string_class is None:
            logger.info(f'Column "{column_name}" — STRING FROM REGEX')
            if not isinstance(column_info, StringFromRegexColumn):
                column_info = StringFromRegexColumn(column_name=column_name, data_type=column_data_type)
            if column_info.get_common_regex() is None:
                common_regex = get_info_for_string_column(column_values.dropna())
                column_info.set_common_regex(common_regex)
            generator = get_generator_for_string_column(common_regex=column_info.get_common_regex())

    elif isinstance(column_info, CurrentTimestampColumn):
        logger.info(f'Column "{column_name}" — CURRENT_TIMESTAMP COLUMN')
        generator = get_generator_for_current_dttm_column()

    else:
        logger.info(f'Column "{column_values.name}" — CONTINUOUS {column_data_type.upper()} COLUMN')
        if not isinstance(column_info, ContinuousColumn):
            column_info = ContinuousColumn(data_type=column_data_type)

        params = None
        if 'decimal' in column_data_type:
            precision = int(re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()[1])
            params = {'precision': precision}
        if column_info.get_date_flag():
            params = {'date_flag': True}

        if column_info.get_intervals() is None or column_info.get_probabilities():
            if column_values.nunique() in [0, 1] and 'decimal' in column_data_type:
                column_info.set_generator(get_generator_for_nulls())
                return column_info
            intervals, probabilities = get_info_for_continuous_column(column_values=column_values,
                                                                      input_data_type=get_input_data_type(column_data_type),
                                                                      number_of_intervals=number_of_intervals)
            column_info.set_intervals(intervals)
            column_info.set_probabilities(probabilities)
        generator = get_generator_for_continuous_column(intervals=column_info.get_intervals(),
                                                        probabilities=column_info.get_probabilities(),
                                                        output_data_type=get_output_data_type(column_data_type),
                                                        params=params)

    column_info.set_generator(generator)
    return column_info


def get_columns_info_with_set_generators(rich_columns_info_dict, conn=None, table_name=None):
    columns_info_with_set_generators = []
    for column_name, column_info_dict in rich_columns_info_dict.items():
        column_type = column_info_dict.get('type')
        column_data_type = column_info_dict.get('data_type')
        if column_type == 'CUSTOM_COLUMN':
            continue
        generator = None
        if column_type == 'CATEGORICAL':
            if column_data_type == 'date':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if isinstance(x, str) else x,
                                  column_info_dict['values']))
            elif 'timestamp' in column_data_type:
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if isinstance(x, str) else x,
                                  column_info_dict['values']))
            else:
                values = column_info_dict.get('values')
            probabilities = column_info_dict.get('probabilities')
            generator = get_generator_for_categorical_column(values=values,
                                                             probabilities=probabilities)

        elif column_type == 'CONTINUES':
            intervals = column_info_dict.get('intervals')
            probabilities = column_info_dict.get('probabilities')
            if intervals is None or probabilities is None:
                generator = get_generator_for_nulls()
            else:
                params = None
                if 'decimal' in column_data_type:
                    precision = int(re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()[1])
                    params = {'precision': precision}
                if column_info_dict.get('date_flag'):
                    params = {'date_flag': True}

                generator = get_generator_for_continuous_column(intervals=intervals,
                                                                probabilities=probabilities,
                                                                output_data_type=get_output_data_type(column_data_type),
                                                                params=params)
        elif column_type == 'STRING_FROM_REGEX':
            common_regex = column_info_dict.get('common_regex')
            generator = get_generator_for_string_column(common_regex=common_regex)

        elif column_type == 'CURRENT_TIMESTAMP':
            generator = get_generator_for_current_dttm_column()

        elif column_type == 'FIO_IN_UPPER_CASE':
            generator = get_generator_for_fio_in_upper_case_column()

        elif column_type == 'FIO_ONLY_STARTING_WITH_UPPER_CASE':
            generator = get_generator_for_fio_only_starting_with_upper_case_column()

        elif column_type == 'EMAIL':
            generator = get_generator_for_email_column()

        elif column_type == 'INCREMENTAL_ID':
            generator = get_generator_for_incremental_id_column(conn=conn,
                                                                table_name=table_name,
                                                                incremental_id_column_name=column_name)
        elif column_type == 'FOREIGN_KEY':
            foreign_key_table_name = column_info_dict.get('foreign_key_table_name')
            foreign_key_column_name = column_info_dict.get('foreign_key_column_name')
            generator = get_generator_for_foreign_key_column(conn, foreign_key_table_name, foreign_key_column_name)

        column_info = Column(column_name=column_name, data_type=column_data_type)
        column_info.set_generator(generator)
        columns_info_with_set_generators.append(column_info)

    return columns_info_with_set_generators
