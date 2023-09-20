from loguru import logger
from pandas import concat


def get_fake_data_for_insertion(output_size,
                                columns_info_with_set_generator):
    list_of_fake_column_data = []
    for column_info in columns_info_with_set_generator:
        column_name = column_info.get_column_name()
        fake_column_data_in_series = column_info.get_generator().send(output_size)
        fake_column_data_in_series.name = column_name
        logger.info(f'Data for {column_name} was generated.')
        list_of_fake_column_data.append(fake_column_data_in_series)
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    return df_to_insert
