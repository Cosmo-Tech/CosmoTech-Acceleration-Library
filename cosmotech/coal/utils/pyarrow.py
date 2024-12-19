import pyarrow as pa
import pyarrow.compute as pc


def replace_null_characters(table: pa.Table, replacement_value: str = "") -> pa.Table:
    string_columns = []
    for column_name in table.column_names:
        if table.schema.field(column_name).type == "string":
            string_columns.append(column_name)

    for column_name in string_columns:
        col_index = table.column_names.index(column_name)
        field = table.field(col_index)
        original_column = table.column(col_index)
        new_data = pc.replace_substring(original_column, "\x00", replacement_value)
        table = table.set_column(col_index, field, new_data)

    return table
