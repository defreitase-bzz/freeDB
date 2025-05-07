import re
import os


def run(args):
    if len(args) < 4:
        params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
        return f"Usage: <{args[0]}> {params}"

    params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
    print(f"Usage: <{args[0]}> {params}")

    table = args[1]
    columns = args[2]
    values = args[3:]


    if not correct_format(table, columns, values):
        return f"Error: <{params}> Command does not match required format"


    columns = tolist(columns)
    values = tolist(values)

    print(values)


    data_path = f"db/{table}.data"
    schema_path = f"db/{table}.schema"

    if not os.path.exists(schema_path):
        return f"Error: table {table} does not exist"

    with open(schema_path, "r") as f:
        schema = f.readline().strip().split(",")
        schema_table = {}

    for i in range(len(schema)):
        if ":" in schema[i]:
            separated = schema[i].split(":")
            schema_table[separated[0]] = separated[1]
            if separated[0] not in columns:
                for value in values:
                    value.insert(i, "")
        else:
            schema_table[schema[i]] = ""


    print(schema_table)

    if not matches_schema(schema_table, columns, values):
        with open(data_path, 'a') as f:
            for line in values:
                f.write(":".join([str(i) for i in line]) + '\n')
            return "Data inserted."

def correct_format(table, columns, values):
    is_valid_columns = re.fullmatch(r"[a-zA-Z0-9,]+", columns) is not None
    is_valid_table_name = table.isalnum()
    print(is_valid_columns)

    for i in values:
        is_valid_row = len(i.split(":")) == len(columns.split(","))

        if not all([is_valid_row, is_valid_columns, is_valid_table_name]):
            return False

    return True


def tolist(data):
    print(data)
    if not isinstance(data, list):
        return data.split(",")
    else:
        for i in range(len(data)):
            parsed_list = data[i].split(":")
            for y in range(len(parsed_list)):
                if parsed_list[y].isnumeric():
                    parsed_list[y] = int(parsed_list[y])
            data[i] = parsed_list
        return data

def matches_schema(schema, columns, values):
    for i in range(len(values)):
        for y in range(len(values[i])):
            if columns[y] not in schema:
                keys = list(schema.keys())
                return f"Error: <{columns}> {columns[y]} doesnt match any table columns of table {keys}"

            if schema[columns[y]] == "int" and not isinstance(values[i], int):
                return False
            elif schema[columns[y]] == "str" and not isinstance(values[i], str):
                return False
    return True


