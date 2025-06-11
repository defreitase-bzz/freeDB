import os
import sys
import shutil

def run(args):
    if len(args) < 2:
        print("Usage: drop.py <table> [-cc]")
        sys.exit(1)

    table = args[1]
    flags = args[2:] if len(args) > 2 else []
    safe = "-cc" in flags

    if "db" in table:
        user_choice = str(input("You are about to delete your entire database. Do you want to continue? [y/n] "))
        if user_choice == "y":
            if os.path.exists("./db"):
                shutil.rmtree("./db")
                print("The database has successfully been deleted.")
                sys.exit(1)
            else:
                print("There is no database that could be deleted.")
                sys.exit(1)
        else:
            print("Disaster averted")
            sys.exit(1)

    if not os.path.exists(f"./db/{table}"):
        print(f"[ERROR]: Table '{table}' does not exist.")
        sys.exit(1)

    pk_column = get_pk_column_name(table)

    # Check if other tables have FKs pointing here
    referencing_tables = find_foreign_keys_pointing_to(pk_column)

    if referencing_tables:
        print(f"[WARNING]: Table '{table}' is referenced by: {referencing_tables}")
        if not safe:
            choice = input("WARNING: Do you want to delete this entry with all its relations? (y/n) ")
            if choice.lower() != "y":
                print("Aborted.")
                sys.exit(0)

        # Cascade delete in referencing tables
        for ref_table, fk_col in referencing_tables:
            cascade_delete_entries(ref_table, fk_col, table)

    # Delete table files
    shutil.rmtree(f"./db/{table}")
    print(f"Table '{table}' and its files have been deleted.")

def get_pk_column_name(table):
    schema_path = f"./db/{table}/{table}.schema"
    with open(schema_path) as f:
        schema = f.read().strip().split(",")
        for col in schema:
            if "[PK" in col:
                return col.split(":")[0]
    return None

def find_foreign_keys_pointing_to(target_pk_column):
    referencing = []
    for folder in os.listdir("./db"):
        schema_path = f"./db/{folder}/{folder}.schema"
        if not os.path.exists(schema_path):
            continue
        with open(schema_path) as f:
            schema = f.read().strip().split(",")
            for col in schema:
                parts = col.split(":")
                if len(parts) >= 2 and parts[0] == target_pk_column and "[FK" in parts[1]:
                    referencing.append((folder, parts[0]))
    return referencing

def cascade_delete_entries(table, fk_column, referenced_table):
    fk_index = get_column_index(fk_column, table)
    referenced_pks = collect_all_pks(referenced_table)

    data_path = f"./db/{table}/{table}.data"
    with open(data_path, "r") as f:
        lines = f.readlines()

    updated_lines = [line for line in lines if line.strip().split(",")[fk_index] not in referenced_pks]

    with open(data_path, "w") as f:
        f.writelines(updated_lines)

    print(f"Cascade-deleted rows from '{table}' referencing '{referenced_table}'")

    # Optional: rebuild index file here, if needed

def get_column_index(colname, table):
    schema_path = f"./db/{table}/{table}.schema"
    with open(schema_path) as f:
        schema = f.read().strip().split(",")
        for idx, col in enumerate(schema):
            if col.startswith(colname + ":"):
                return idx
    return -1

def collect_all_pks(table):
    data_path = f"./db/{table}/{table}.data"
    pk_index = get_pk_column_index(table)
    with open(data_path, "r") as f:
        return [line.strip().split(",")[pk_index] for line in f]

def get_pk_column_index(table):
    schema_path = f"./db/{table}/{table}.schema"
    with open(schema_path) as f:
        schema = f.read().strip().split(",")
        for idx, col in enumerate(schema):
            if "[PK" in col:
                return idx
    return -1

if __name__ == "__main__":
    run(sys.argv)
