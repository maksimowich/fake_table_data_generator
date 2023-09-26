from loguru import logger
from pandas import concat
from fake_data_generator.columns_generator.column import MultipleColumns


def get_fake_data_for_insertion(output_size,
                                columns_info_with_set_generator):
    list_of_fake_column_data = []
    for column_info in columns_info_with_set_generator:
        if type(column_info) == MultipleColumns:
            col_names = [col_info.get_column_name() for col_info in column_info.get_columns()]
            fake_data_in_df = column_info.get_generator().send(output_size)
            fake_data_in_df.rename(columns={index: name for index, name in enumerate(col_names)}, inplace=True)
            list_of_fake_column_data.append(fake_data_in_df)
        else:
            column_name = column_info.get_column_name()
            fake_column_data_in_series = column_info.get_generator().send(output_size)
            fake_column_data_in_series.name = column_name
            logger.info(f'Data for {column_name} was generated.')
            list_of_fake_column_data.append(fake_column_data_in_series)
    df_to_insert = concat(list_of_fake_column_data, axis=1)
    return df_to_insert
