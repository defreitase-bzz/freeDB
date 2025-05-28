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

    print(values)

    if not correct_format(table, columns, values):
        return f"Error: <{params}> Command does not match required format"

    columns = tolist(columns)
    values = tolist(values)

    data_path = f"db/{table}/{table}.data"
    schema_path = f"db/{table}/{table}.schema"

    if not os.path.exists(schema_path):
        return f"Error: table {table} does not exist"

    with open(schema_path, "r") as f:
        schema = f.readline().strip().split(",")
        schema_table = {}

    for i, field_def in enumerate(schema):
        if ":" in field_def:
            name, rest = field_def.split(":", 1)
        else:
            name, rest = field_def, ""
        dtype, sep, flag_part = rest.partition("[")
        dtype = dtype or ""
        if sep:
            flags = flag_part.rstrip("]").split()
        else:
            flags = []

        schema_table[name] = (dtype, flags)

        if name not in columns:
            if "not_null" in flags:
                if "auto" in flags:
                    idx_path = f"db/{table}/{table}_{name}.idx"
                    if os.path.exists(idx_path):
                        with open(idx_path, "r") as idxf:
                            existing_ids = [int(l.strip().split(",")[0]) for l in idxf if l.strip()]
                    else:
                        with open(idx_path, "w") as f:
                            pass
                        existing_ids = []

                    base = existing_ids[-1] + 1 if existing_ids else 1
                    new_ids = list(range(base, base + len(values)))

                    for row in range(0, len(values)):
                        values[row].insert(i, new_ids[row])
                    columns.insert(i, f"{name}")
                else:
                    return f"Error: <{columns}> <{values}> table {table} column {name} is defined as not_null and cannot be left empty"
            else:
                for row in values:
                    row.insert(i, "")
        elif name in columns and "auto" in flags or "PK" in flags:
            new_ids = [int(values[row][i]) for row in range(len(values))]
            idx_path = f"db/{table}/{table}_{name}.idx"
            if os.path.exists(idx_path):
                with open(idx_path, "r") as idxf:
                    existing_ids = [int(l.strip().split(",")[0]) for l in idxf if l.strip()]
            else:
                with open(idx_path, "w") as f:
                    pass
                existing_ids = []

            if existing_ids:
                for new_id in new_ids:
                    pos = binary_search(existing_ids, new_id)
                    if pos < len(existing_ids) and existing_ids[pos] == new_id or new_ids.count(new_id) > 1 and "PK" in flags:
                        return f"Error: ID {new_id} already exists"

    schema_valid, response = matches_schema(schema_table, columns, values)

    if schema_valid:
        pk_name = [col for col, (dt, fl) in schema_table.items() if "PK" in fl][0]
        pk_index = columns.index(pk_name)

        idx_path = f"db/{table}/{table}_{pk_name}.idx"
        with open(data_path, "a+b") as df, open(idx_path, "a") as idxf:
            for row in values:
                df.seek(0, os.SEEK_END)
                offset = df.tell()

                record = ",".join(str(v) for v in row) + "\n"
                df.write(record.encode())

                new_id = row[pk_index]
                idxf.write(f"{new_id},{offset}\n")

        return f"Inserted {len(values)} row(s) into table '{table}'."
    else:
        return response


def correct_format(table, columns, values):
    is_valid_columns = re.fullmatch(r"[a-zA-Z0-9,_]+", columns) is not None
    is_valid_table_name = table.isalnum()

    for i in values:
        in_quotes = False
        count = 1

        for char in i:
            if char == "'":
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                count += 1

        if count != len(columns.split(",")) or not is_valid_columns or not is_valid_table_name:
            return False

    return True


def tolist(data):
    if not isinstance(data, list):
        return data.split(",")
    else:
        for i in range(len(data)):
            copy = data[i]
            if "'" in data[i]:
                in_quotes = False
                copy = ""

                for x in range(len(data[i])):
                    if data[i][x] == "," and in_quotes:
                        copy += "$"
                    elif data[i][x] == "'" and not in_quotes:
                        in_quotes = True
                        copy += '"'
                    elif data[i][x] == "'" and in_quotes:
                        in_quotes = False
                        copy += '"'
                    else:
                        copy += data[i][x]

            parsed_list = [value.replace("$", ",") for value in copy.split(",")]

            for y in range(len(parsed_list)):
                if parsed_list[y].isdigit():
                    parsed_list[y] = int(parsed_list[y])
            data[i] = parsed_list
        return data


def matches_schema(schema, columns, values):
    for i in range(len(values)):
        for y in range(len(values[i])):
            if columns[y] not in schema:
                keys = list(schema.keys())
                return False, f"Error: <{columns}> {columns[y]} doesnt match any table columns of table {keys}"

            if schema[columns[y]][0] == "int" and not isinstance(values[i][y], int):
                return False, f"Error: <{values[i]}> {values[i][y]} is not an integer"
            elif schema[columns[y]][0] == "str" and not isinstance(values[i][y], str):
                return False, f"Error: <{values[i]}> {values[i][y]} is not a string"

    return True, "Matches!"


def binary_search(array, target):
    i, j = 0, len(array)
    m = 0

    while j > i:
        m = (i + j) // 2

        if array[m] > target:
            j = m
        elif array[m] < target:
            i = m + 1
        else:
            return m
    return m
