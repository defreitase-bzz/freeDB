import os


def run(args):
    if len(args) != 6:
        return f"Usage: <{args[0]}> <table> <match_column> <match_value> <update_columns> <update_values>"

    table = args[1]
    match_col = args[2]
    match_val = args[3]
    update_cols = args[4].split(",")
    update_vals = args[5].split(",")

    if len(update_cols) != len(update_vals):
        return "Error: Mismatch between number of update columns and values."

    schema_path = f"db/{table}/{table}.schema"
    data_path = f"db/{table}/{table}.data"

    if not os.path.exists(schema_path) or not os.path.exists(data_path):
        return f"Error: Table '{table}' or its schema/data file does not exist."

    # Load schema
    with open(schema_path, "r") as f:
        schema_fields = [col.split(":")[0] for col in f.readline().strip().split(",")]

    if match_col not in schema_fields:
        return f"Error: Match column '{match_col}' not found in schema."

    for col in update_cols:
        if col not in schema_fields:
            return f"Error: Update column '{col}' not found in schema."

    match_index = schema_fields.index(match_col)
    update_indices = [schema_fields.index(col) for col in update_cols]

    # Read all rows
    updated_count = 0
    with open(data_path, "rb") as f:
        rows = f.readlines()

    for i in range(len(rows)):
        row = rows[i].decode().strip().split(",")
        if str(row[match_index]) == match_val:
            for idx, val in zip(update_indices, update_vals):
                row[idx] = val
            rows[i] = (",".join(row) + "\n").encode()
            updated_count += 1

    # Write updates
    with open(data_path, "wb") as f:
        f.writelines(rows)

    return f"Updated {updated_count} row(s) in table '{table}' where {match_col} = {match_val}."
