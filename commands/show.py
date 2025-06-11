import os

def run(args):
    if len(args) < 2:
        params = "<table>"
        return f"Usage: <{args[0]}> {params}"

    try:
        table = args[1]
        return print_table(table)
    except Exception as e:
        return f"Error: {e}"

def print_table(table):
    result = ""
    table_path = table
    if table_path == "tables":
        table_path = "db"
        tables = os.listdir(table_path)
        for i in tables:
            with open(f"db/{i}/{i}.schema", "r") as f:
                fields = f.read().strip().split(",")
                longest = 0
                for y in fields:
                    if len(y) > longest:
                        longest = len(y)
                longest = max(longest, len(i))

                fields = [i] + fields

                result += "_"*(longest+4) + "\n"


                for x in range(len(fields)):
                    if not x:
                        result += "| " + fields[x] + (longest-len(fields[x]))*" " + " |" + "\n"
                        result += "_" * (longest + 4) + "\n"
                    else:
                        result += "| " + fields[x] + (longest - len(fields[x])) * " " + " |" + "\n"

                result += "_" * (longest + 4) + "\n\n"
    else:
        if not os.path.exists(f"db/{table}"):
            return f"Error: table {table} does not exist"

        with open(f"db/{table}/{table}.schema", "r") as f:
            fields = f.read().strip().split(",")
            longest = 0
            for y in fields:
                if len(y) > longest:
                    longest = len(y)
            longest = max(longest, len(table))

            fields = [table] + fields

            result += "_" * (longest + 4) + "\n"

            for x in range(len(fields)):
                if not x:
                    result += "| " + fields[x] + (longest - len(fields[x])) * " " + " |" + "\n"
                    result += "_" * (longest + 4) + "\n"
                else:
                    result += "| " + fields[x] + (longest - len(fields[x])) * " " + " |" + "\n"

            result += "_" * (longest + 4) + "\n\n"
    return result

