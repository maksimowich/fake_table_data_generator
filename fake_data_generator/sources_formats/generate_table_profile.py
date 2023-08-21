import json
from loguru import logger
from fake_data_generator.sources_formats.helper_functions import get_rich_columns_info


def generate_table_profile(conn,
                           source_table_name_with_schema: str,
                           output_table_profile_path: str,
                           number_of_rows_from_which_to_create_pattern: int,
                           columns_info: list = None,
                           columns_to_include: list = None):
    rich_columns_info = get_rich_columns_info(conn,
                                              source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern,
                                              columns_info,
                                              columns_to_include)

    dict_to_dump = {}
    for column_info in rich_columns_info:
        dict_to_dump.update(column_info.get_as_dict())

    with open(output_table_profile_path, 'w') as file:
        json.dump(dict_to_dump, file)
    logger.info(f'Profile was loaded into {output_table_profile_path}.')
