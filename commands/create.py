import os
import re

def run(args):
    if 4 < len(args) < 3:
        params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
        return f"Usage: <{args[0]}> {params}"

    params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
    print(f"Usage: <{args[0]}> {params}")

    table = args[1]

    schema_path = f"db/{table}.schema"

    if not os.path.exists("./db"):
        os.mkdir("db")

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
            separated[1] = re.sub(r"\[.*?\]", "", separated[1]).strip()
            # muss später noch schauen wegen weiter typen, ist provisorisch ig
            if separated[1] not in ["str", "int"]:
                return f"Error: <{line}> only allowed types: int, str"

    is_valid_keywords, result = check_keywords(line)

    if not is_valid_keywords[1]:
        return is_valid_keywords[0]


    with open(schema_path, 'w') as f:
        f.write(line + '\n')

    return "Table created."

def correct_format(command, params):
    regex = re.fullmatch(r"[\[\]a-zA-Z0-9:,_ ]+", params) is not None
    result = all([params, regex, command.isalnum()])
    return result

def check_keywords(line):
    closed = True
    for i in line.split(","):
        keywords = ""
        for char in i:
            if char == "[":
                closed = False
            elif char == "]":
                closed = True
            elif not closed:
                keywords += char

        if not closed:
            return f"Error: <{line}> keyword brackets not closed.", False
        elif not all([i in ["not_null", "auto", "PK"] for i in keywords.split(" ")]):
            return f"Error: <{line}> unknown keyword.", False
    return "Valid Keywords", True






