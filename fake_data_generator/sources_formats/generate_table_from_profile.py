import json
from fake_data_generator.columns_generator import get_columns_info_with_set_generators
from fake_data_generator.columns_generator.column import Column, MultipleColumns
from fake_data_generator.sources_formats.helper_functions import \
    get_create_query, create_table_if_not_exists, execute_insertion


def generate_table_from_profile(conn,
                                dest_table_name_with_schema: str,
                                number_of_rows_to_insert: int,
                                source_table_profile_path: str = None,
                                columns_info=None,
                                batch_size=100):
    rich_columns_info_dict = {}
    if source_table_profile_path is not None:
        with open(source_table_profile_path, 'r') as file:
            rich_columns_info_dict = json.load(file)

    columns_with_generators_as_parameter = []
    for column_info in columns_info or []:
        if type(column_info) == MultipleColumns:
            for col_info in column_info.get_columns():
                rich_columns_info_dict.update(col_info.get_as_dict())
            columns_with_generators_as_parameter.append(column_info)
        else:
            if type(column_info) == Column and column_info.get_generator() is not None:
                columns_with_generators_as_parameter.append(column_info)
            rich_columns_info_dict.update(column_info.get_as_dict())

    create_table_if_not_exists(conn=conn,
                               dest_table_name_with_schema=dest_table_name_with_schema,
                               create_query=get_create_query(dest_table_name_with_schema, rich_columns_info_dict))

    columns_with_set_generators = get_columns_info_with_set_generators(rich_columns_info_dict, conn, dest_table_name_with_schema)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert,
                      columns_with_set_generators + columns_with_generators_as_parameter, batch_size)
