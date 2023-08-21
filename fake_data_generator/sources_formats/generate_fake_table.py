from fake_data_generator.sources_formats.helper_functions import \
    get_rich_columns_info, create_table_if_not_exists, execute_insertion


def generate_fake_table(conn,
                        source_table_name_with_schema: str,
                        dest_table_name_with_schema: str,
                        number_of_rows_to_insert: int,
                        number_of_rows_from_which_to_create_pattern: int,
                        columns_info: list = None,
                        columns_to_include: list = None,
                        batch_size=100):
    rich_columns_info = get_rich_columns_info(conn, source_table_name_with_schema,
                                              number_of_rows_from_which_to_create_pattern, columns_info, columns_to_include)
    create_table_if_not_exists(conn, source_table_name_with_schema, dest_table_name_with_schema, columns_to_include)
    execute_insertion(conn, dest_table_name_with_schema, number_of_rows_to_insert, rich_columns_info, batch_size)
