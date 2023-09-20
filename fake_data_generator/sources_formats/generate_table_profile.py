import json
from loguru import logger
from fake_data_generator.sources_formats.helper_functions import get_rich_columns_info


def generate_table_profile(output_table_profile_path: str,
                           conn=None,
                           source_table_name_with_schema: str = None,
                           number_of_rows_from_which_to_create_pattern: int = None,
                           columns_info: list = None,
                           columns_to_include: list = None,
                           number_of_intervals=5,
                           categorical_threshold=0.2):
    rich_columns_info = get_rich_columns_info(conn=conn,
                                              source_table_name_with_schema=source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern=number_of_rows_from_which_to_create_pattern,
                                              columns_info=columns_info,
                                              columns_to_include=columns_to_include,
                                              number_of_intervals=number_of_intervals,
                                              categorical_threshold=categorical_threshold)

    dict_to_dump = {}
    for column_info in rich_columns_info:
        dict_to_dump.update(column_info.get_as_dict())

    with open(output_table_profile_path, 'w') as file:
        json.dump(dict_to_dump, file)
    logger.info(f'Profile was loaded into {output_table_profile_path}.')
