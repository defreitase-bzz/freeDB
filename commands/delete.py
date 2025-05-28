import os
import sys
import re

def run(args):
    if len(args) < 4:
        print(f"Usage: <{args[0]}> <table> <columns> <valueSet1> [<valueSet2> ...] [-cc]")
        sys.exit(1)

    table = args[1]
    columns = args[2].split(",")
    # Collect value sets (strings with commas, excluding flags like "-cc")
    value_sets = [arg for arg in args[3:] if not re.match(r"^-\w+", arg)]

    # Check safe delete
    flags = [arg for arg in args if arg.startswith("-")]
    safe = check_safe_delete(flags)

    pos, unmatched = get_position(columns, table)
    if unmatched:
        print(f"\n[ERROR]: Column(s) {unmatched} not found in table {table}.")
        sys.exit(1)

    all_deleted_pks = []
    for valset in value_sets:
        values = valset.replace(" ", "").split(",")
        deleted_pks = delete_entry(pos, values, table, safe)
        all_deleted_pks.extend(deleted_pks)

    if not all_deleted_pks:
        return "[ERROR]: No entry found with the given parameters."
    else:
        return "DELETED PRIMARY KEY(s): " + str(all_deleted_pks)


def check_safe_delete(params):
    if "-cc" in params:
        print("SAFE DELETE: ENABLED")
        return True
    else:
        safe_del = str(input("WARNING: Do you want to delete this entry with all its relations? (y/n) "))
        if safe_del == "y":
            print("\nSAFE DELETE: ENABLED")
            return True
        else:
            print("\nSAFE DELETE: DISABLED")
            return False


def get_primary_key(table):
    try:
        with open(f"./db/{table}/{table}.schema")  as file:
            schema = file.read()
            divided_schema = schema.split(",")
            index_pk = [divided_schema.index(c) for c in divided_schema if "PK" in c]
            return index_pk[0]
    except FileNotFoundError:
        print("[Error]: Data couldn't be found. Make sure data exists before performing a delete command.")
        sys.exit(1)

def get_position(cols, table):
    try:
        with open(f"./db/{table}/{table}.schema")  as file:
            schema = file.read()
            divided_schema = schema.split(",")

            positions = []
            matched_columns = []

            for i in cols:
                for idx, value in enumerate(divided_schema):
                    if value.startswith(i + ":"):
                        positions.append(idx)
                        matched_columns.append(i)
                        break

            unmatched = [c for c in cols if c not in matched_columns]
            return positions, unmatched

    except FileNotFoundError:
        print("[Error]: Data couldn't be found. Make sure data exists before performing a delete command.")
        sys.exit(1)

def delete_entry(positions, values, table, safe):
    try:
        with open(f"./db/{table}/{table}.data") as file:
            data = file.readlines()

        pk_index = get_primary_key(table)
        deleted_pks = []
        updated_lines = []

        print("LOOKING FOR: " + str(values) + "\n")

        for line in data:
            values_in_line = line.strip().split(",")
            match = True
            for pos, val in zip(positions, values):
                if pos >= len(values_in_line) or values_in_line[pos] != val:
                    match = False
                    break

            if match:
                deleted_pks.append(values_in_line[pk_index])
            else:
                updated_lines.append(line)

        with open(f"./db/{table}/{table}.data", "w") as file:
            file.writelines(updated_lines)

        if safe:
            pk_column = get_pk_column_name(table)
            for pk in deleted_pks:
                delete_relations(pk_column, pk)

        delete_pks(deleted_pks, table)


        return deleted_pks

    except FileNotFoundError:
        print("[Error]: Data couldn't be found. Make sure data exists before performing a delete command.")
        sys.exit(1)

def get_pk_column_name(table):
    with open(f"./db/{table}/{table}.schema") as f:
        schema = f.read().strip().split(",")
        for col in schema:
            if "[PK" in col:
                return col.split(":")[0]
        return None


def delete_pks(deleted_pks, table):
    new_idx_entries = []
    byte_offset = 0

    with open(f"./db/{table}/{table}.data", "r") as data_file:
        for line in data_file:
            values = line.strip().split(",")
            pk = values[get_primary_key(table)]
            if pk not in deleted_pks:
                new_idx_entries.append(f"{pk},{byte_offset}\n")
                byte_offset += len(line)

    with open(f"./db/{table}/{table}_{table}ID.idx", "w") as idx_file:
        idx_file.writelines(new_idx_entries)

    print(f"Index successfully rebuilt for table '{table}' after deleting PKs: {deleted_pks}")

def delete_relations(pk_column, pk_value):
    for subfolder in os.listdir("./db"):
        schema_path = f"./db/{subfolder}/{subfolder}.schema"
        data_path = f"./db/{subfolder}/{subfolder}.data"

        if not os.path.exists(schema_path) or not os.path.exists(data_path):
            continue

        with open(schema_path) as f:
            schema = f.read().strip().split(",")

        fk_positions = [
            i for i, col in enumerate(schema)
            if col.startswith(f"{pk_column}:") and "[FK" in col
        ]

        if not fk_positions:
            continue

        with open(data_path, "r") as f:
            lines = f.readlines()

        updated_lines = []
        for line in lines:
            fields = line.strip().split(",")
            if all(fields[pos] != pk_value for pos in fk_positions):
                updated_lines.append(line)

        with open(data_path, "w") as f:
            f.writelines(updated_lines)

        print(f"Cascade deleted references from {subfolder} where {pk_column} = {pk_value}")

