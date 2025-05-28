import os
import re

def run(args):
    if len(args) < 3:
        params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
        return f"Usage: <{args[0]}> {params}"

    params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
    print(f"Usage: <{args[0]}> {params}")

    table = args[1]

    schema_path = f"db/{table}/{table}.schema"

    if not os.path.exists("db"):
        os.mkdir("db")

    if not os.path.exists(f"db/{table}"):
        os.mkdir(f"db/{table}")

    if os.path.exists(schema_path):
        return f"Error: table {table} already exists"

    values = args[2].strip()

    if correct_format(table, values):
        pass
    else:
        return f"Error: <{params}> Command does not match required format"


    line = values

    for i in line.split(","):
        if ":" in i:
            separated = i.split(":")
            data_type = re.sub(r"\[.*?\]", "", separated[1]).strip()
            # muss später noch schauen wegen weiter typen, ist provisorisch ig
            if data_type not in ["str", "int"]:
                return f"Error: <{line}> only allowed types: int, str"


    is_valid_keywords, result = check_keywords(line, table)

    if not result:
        return is_valid_keywords

    line = is_valid_keywords

    with open(schema_path, 'w') as f:
        f.write(line + '\n')

    return f"Created table '{table}' with schema: {line}"

def correct_format(command, params):
    regex = re.fullmatch(r"[\[\]a-zA-Z0-9:,_ ]+", params) is not None
    result = all([params, regex, command.isalnum()])
    return result

def check_keywords(line, table):
    has_primary_key = False
    existing_pk_names = set()

    for tname in os.listdir("db"):
        if tname == table:
            continue
        schema_file = os.path.join("db", tname, f"{tname}.schema")
        if os.path.exists(schema_file):
            with open(schema_file, "r") as f:
                schema_line = f.readline().strip()
                for fielddef in schema_line.split(","):
                    if "[PK" in fielddef:
                        fieldname = fielddef.split(":")[0].strip()
                        existing_pk_names.add(fieldname)

    for i in line.split(","):
        if ":" in i:
            separated = i.split(":")
            data_type = re.sub(r"\[.*?\]", "", separated[1]).strip()
            field = separated[0]
        else:
            data_type = ""
            field = re.sub(r"\[.*?\]", "", i).strip()

        inside_brackets = False
        brackets_closed = False
        keywords = ""

        for char in i:
            if char == "[":
                if inside_brackets:
                    return f"Error: <{i}> nested brackets not allowed.", False
                inside_brackets = True
                continue
            elif char == "]":
                if not inside_brackets:
                    return f"Error: <{i}> closing bracket without opening.", False
                inside_brackets = False
                brackets_closed = True
                continue

            if inside_brackets:
                keywords += char
            elif brackets_closed and char not in " \t":
                return f"Error: <{i}> nothing must follow after keyword brackets []: '{char}'", False

        if "[" in i and not brackets_closed:
            return f"Error: <{line}> keyword brackets not closed.", False
        elif "[" in i and not all([i in ["not_null", "auto", "PK", "FK"] for i in keywords.split(" ")]):
            return f"Error: <{line}> unknown keyword.", False

        if "PK" in keywords:
            if has_primary_key:
                return f"Error: <{line}> <{i}> Table cannot have more than one primary key", False
            elif "FK" in keywords:
                return f"Error: <{line}> <{i}> field cannot be both primary key and foreign key at the same time.", False
            elif data_type and data_type != "int":
                return f"Error: <{line}> <{i}> field that is primary key must be int and not {data_type}", False
            elif not data_type:
                line = line.replace(field, field + ":int")
            elif "not_null" not in keywords:
                keywords += "not_null"

            if field in existing_pk_names:
                return f"Error: <{field}> already exists as a primary key in another table", False

            has_primary_key = True

        elif "FK" in keywords:
            if "not_null" not in keywords:
                keywords += "not_null"
            elif "auto" in keywords:
                return f"Error: <{line}> <{i}> field cannot be foreign key and be auto increment", False
            elif data_type and data_type != "int":
                return f"Error: <{line}> <{i}> field that is foreign key must be int and not {data_type}", False
            elif not data_type:
                line = line.replace(field, field + ":int")

    if not has_primary_key:
        generated = f"{table}ID"
        if generated in existing_pk_names:
            return f"Error: auto-generated PK '{generated}' already exists in another table", False
        line = f"{generated}:int[PK not_null auto]," + line

    return line, True

