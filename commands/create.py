import os
import re

def run(args):
    if 4 < len(args) < 3:
        params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
        print(f"Usage: <{args[0]}> {params}")
        return

    params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
    print(f"Usage: <{args[0]}> {params}")

    table = args[1]

    schema_path = f"db/{table}.schema"

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
            # muss später noch schauen wegen weiter typen, ist provisorisch ig
            if separated[1] not in ["str", "int"]:
                return f"Error: <{line}> only allowed types: int, str"


    with open(schema_path, 'w') as f:
        f.write(line + '\n')

    print("Table created.")

def correct_format(command, params):
    regex = re.fullmatch(r"[a-zA-Z0-9:,]+", params) is not None
    result = all([params, regex, command.isalnum()])
    return result