import os
import shutil


def run(args):
    if len(args) != 5:
        return f"Usage: <{args[0]}> <table_name> <new_table_name> <column_name> <new_column_name>:<new_datatype>"

    _, table_name, new_table_name, column_name, new_col_def = args

    base_path = f"db/{table_name}"
    data_path = f"{base_path}/{table_name}.data"
    schema_path = f"{base_path}/{table_name}.schema"

    if not os.path.exists(schema_path) or not os.path.exists(data_path):
        return f"Error: Table '{table_name}' does not exist."

    # Parse new_col_def into name and datatype
    if new_col_def and ":" in new_col_def:
        new_column_name, new_datatype = new_col_def.split(":")
    else:
        new_column_name, new_datatype = "", ""

    # Load schema
    with open(schema_path, "r") as f:
        schema_line = f.readline().strip()
    schema_parts = schema_line.split(",")
    schema_fields = [col.split(":")[0] for col in schema_parts]

    updated = False

    # Case 1: modify existing column
    if column_name:
        if column_name in schema_fields:
            idx = schema_fields.index(column_name)
            dtype, flags = "", ""

            if ":" in schema_parts[idx]:
                dtype_flag = schema_parts[idx].split(":")[1]
                if "[" in dtype_flag:
                    dtype, flags = dtype_flag.split("[", 1)
                    flags = "[" + flags
                else:
                    dtype = dtype_flag

            updated_name = new_column_name or column_name
            updated_type = new_datatype or dtype
            schema_parts[idx] = f"{updated_name}:{updated_type}{flags}"
            updated = True
        else:
            return f"Error: Column '{column_name}' not found. To add a new column, leave <column_name> blank."

    # Case 2: Add a new column only (column_name is blank but new_column_name and new_datatype are provided)
    if not column_name and new_column_name and new_datatype:
        if new_column_name in schema_fields:
            return f"Error: Column '{new_column_name}' already exists."

        schema_parts.append(f"{new_column_name}:{new_datatype}")
        updated = True

        # Add empty value to every data row
        with open(data_path, "rb") as f:
            rows = f.readlines()
        for i in range(len(rows)):
            row = rows[i].decode().strip().split(",")
            row.append("")  # Add empty field
            rows[i] = (",".join(row) + "\n").encode()
        with open(data_path, "wb") as f:
            f.writelines(rows)

    # Write updated schema
    if updated:
        with open(schema_path, "w") as f:
            f.write(",".join(schema_parts) + "\n")

    # Case 3: Rename table
    if new_table_name and new_table_name != table_name:
        new_base_path = f"db/{new_table_name}"
        os.makedirs(new_base_path, exist_ok=True)

        for file in os.listdir(base_path):
            old_path = os.path.join(base_path, file)
            new_file = file.replace(table_name, new_table_name)
            new_path = os.path.join(new_base_path, new_file)
            shutil.move(old_path, new_path)

        os.rmdir(base_path)

        return f"Table renamed to '{new_table_name}' and schema/data updated."

    return "Alteration completed."
