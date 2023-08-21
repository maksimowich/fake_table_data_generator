import json
from fake_data_generator.columns_generator import get_columns_info_with_set_generators
from fake_data_generator.sources_formats.helper_functions import \
    get_create_query, create_table_if_not_exists, execute_insertion


def generate_table_from_profile(conn,
                                source_table_profile_path: str,
                                dest_table_name_with_schema: str,
                                number_of_rows_to_insert: int,
                                columns_info=None,
                                batch_size=100):
    with open(source_table_profile_path, 'r') as file:
        rich_columns_info_dict = json.load(file)

    for column_info in columns_info or []:
        rich_columns_info_dict.update(column_info.get_as_dict())

    create_table_if_not_exists(conn, dest_table_name_with_schema=dest_table_name_with_schema, create_query=get_create_query(dest_table_name_with_schema, rich_columns_info_dict))
    columns_info_with_set_generators = get_columns_info_with_set_generators(rich_columns_info_dict)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, columns_info_with_set_generators, batch_size)
