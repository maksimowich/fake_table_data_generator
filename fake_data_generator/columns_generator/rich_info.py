import re
from datetime import datetime
from loguru import logger
from fake_data_generator.columns_generator.column import \
    Column, CategoricalColumn, ContinuousColumn, StringColumn, CurrentTimestampColumn
from fake_data_generator.columns_generator.info_for_columns import \
    get_info_for_categorical_column,\
    get_info_for_continuous_column,\
    get_info_for_string_column
from fake_data_generator.columns_generator.generators import \
    get_generator_for_nulls,\
    get_generator_for_categorical_column,\
    get_generator_for_continuous_column, \
    get_generator_for_string_column, \
    get_generator_for_current_dttm_column


def get_input_data_type(data_type):
    if 'decimal' in data_type:
        return 'float'
    elif 'int' in data_type:
        return 'int'
    elif 'date' == data_type:
        return 'date'
    elif 'timestamp' == data_type:
        return 'datetime'


def get_output_data_type(data_type):
    if 'decimal' in data_type:
        return 'decimal'
    elif 'int' in data_type:
        return 'int'
    elif 'date' == data_type:
        return 'date'
    elif 'timestamp' == data_type:
        return 'datetime'


def get_rich_column_info(column_values,
                         column_info):
    column_data_type = column_info.get_data_type()
    column_name = column_info.get_column_name()
    categorical_column_flag = (isinstance(column_info, CategoricalColumn) or (column_values.nunique() / column_values.count() < 0.2) or column_values.nunique() in [0, 1]) and \
        'decimal' not in column_data_type and type(column_info) in [Column, CategoricalColumn]

    if categorical_column_flag:
        logger.info(f'Column "{column_name}" — CATEGORICAL COLUMN')
        if not isinstance(column_info, CategoricalColumn):
            column_info = CategoricalColumn(column_name=column_name, data_type=column_data_type)
        if column_info.get_values() is None or column_info.get_probabilities() is None:
            values, probabilities = get_info_for_categorical_column(column_values)
            column_info.set_values(values)
            column_info.set_probabilities(probabilities)
        generator = get_generator_for_categorical_column(column_name=column_name,
                                                         values=column_info.get_values(),
                                                         probabilities=column_info.get_probabilities())

    elif column_data_type == 'string':
        logger.info(f'Column "{column_name}" — STRING NON CATEGORICAL COLUMN')
        if isinstance(column_info, StringColumn) and column_info.get_string_copy_of() is not None:
            return column_info
        if not isinstance(column_info, StringColumn):
            column_info = StringColumn(column_name=column_name, data_type=column_data_type)
        if column_info.get_common_regex() is None:
            common_regex = get_info_for_string_column(column_values.dropna())
            column_info.set_common_regex(common_regex)
        generator = get_generator_for_string_column(column_name=column_name,
                                                    common_regex=column_info.get_common_regex())

    elif isinstance(column_info, CurrentTimestampColumn):
        logger.info(f'Column "{column_name}" — CURRENT_TIMESTAMP COLUMN')
        generator = get_generator_for_current_dttm_column(column_name=column_name)

    else:
        logger.info(f'Column "{column_values.name}" — CONTINUOUS {column_data_type.upper()} COLUMN')
        if not isinstance(column_info, ContinuousColumn):
            column_info = ContinuousColumn(column_name=column_name, data_type=column_data_type)

        params = None
        if 'decimal' in column_data_type:
            precision = int(re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()[1])
            params = {'precision': precision}
        if column_info.get_date_flag():
            params = {'date_flag': True}

        if column_info.get_intervals() is None or column_info.get_probabilities():
            if column_values.nunique() in [0, 1] and 'decimal' in column_data_type:
                column_info.set_generator(get_generator_for_nulls(column_name))
                return column_info
            intervals, probabilities = get_info_for_continuous_column(column_values=column_values,
                                                                      input_data_type=get_input_data_type(column_data_type))
            column_info.set_intervals(intervals)
            column_info.set_probabilities(probabilities)
        generator = get_generator_for_continuous_column(column_name=column_name,
                                                        intervals=column_info.get_intervals(),
                                                        probabilities=column_info.get_probabilities(),
                                                        output_data_type=get_output_data_type(column_data_type),
                                                        params=params)

    column_info.set_generator(generator)
    return column_info


def get_columns_info_with_set_generators(rich_columns_info_dict):
    columns_info_with_set_generators = []
    for column_name, column_info_dict in rich_columns_info_dict.items():
        column_type = column_info_dict.get('type')
        column_data_type = column_info_dict.get('data_type')
        generator = None
        if column_type == 'CATEGORICAL':
            if column_data_type == 'date':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if isinstance(x, str) else x,
                                  column_info_dict['values']))
            elif column_data_type == 'timestamp':
                values = list(map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S") if isinstance(x, str) else x,
                                  column_info_dict['values']))
            else:
                values = column_info_dict.get('values')
            probabilities = column_info_dict.get('probabilities')
            generator = get_generator_for_categorical_column(column_name=column_name,
                                                             values=values,
                                                             probabilities=probabilities)

        elif column_type == 'CONTINUES':
            intervals = column_info_dict.get('intervals')
            probabilities = column_info_dict.get('probabilities')
            if intervals is None or probabilities is None:
                generator = get_generator_for_nulls(column_name)
            else:
                params = None
                if 'decimal' in column_data_type:
                    precision = int(re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()[1])
                    params = {'precision': precision}
                if column_info_dict.get('date_flag'):
                    params = {'date_flag': True}

                generator = get_generator_for_continuous_column(column_name=column_name,
                                                                intervals=intervals,
                                                                probabilities=probabilities,
                                                                output_data_type=get_output_data_type(column_data_type),
                                                                params=params)
        elif column_type == 'STRING':
            if column_info_dict.get('string_copy_of') is not None:
                column_info = StringColumn(column_name=column_name,
                                           data_type=column_data_type,
                                           string_copy_of=column_info_dict.get('string_copy_of'))
                columns_info_with_set_generators.append(column_info)
                continue
            common_regex = column_info_dict.get('common_regex')
            generator = get_generator_for_string_column(column_name=column_name,
                                                        common_regex=common_regex)

        elif column_type == 'CURRENT_TIMESTAMP':
            generator = get_generator_for_current_dttm_column(column_name=column_name)

        column_info = Column(column_name=column_name, data_type=column_data_type)
        column_info.set_generator(generator)
        columns_info_with_set_generators.append(column_info)

    return columns_info_with_set_generators
