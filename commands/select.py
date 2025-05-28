import re
import os
import subprocess
import shlex

def run(args):
    if len(args) < 3:
        params = "<table> <columns|count> [<conds> …] [join <other> <left>=<right>] [<post-conds> …]"
        return f"Usage: <{args[0]}> {params}"

    try:
        table, columns_raw, pre_filters, join, post_filters = parse_select(args)
    except Exception as e:
        return f"Error: {e}"

    is_count = "-ct" in columns_raw.lower()
    columns = [] if is_count else columns_raw.split(",")

    data_file = f"db/{table}/{table}.data"
    schema_file = f"db/{table}/{table}.schema"
    if not os.path.exists(schema_file):
        return f"Error: table {table} does not exist"
    try:
        with open(schema_file, "r") as sf:
            schema = sf.readline().strip().split(",")
    except:
        return f"Error: could not read schema of table {table}"

    cols = [s.split(":")[0] for s in schema]
    idx_map = {col: i + 1 for i, col in enumerate(cols)}

    awk_cmd = ["awk", "-F,"]

    if join:
        other, left, right = join
        other_file = f"db/{other}/{other}.data"
        other_schema_file = f"db/{other}/{other}.schema"
        if not os.path.exists(other_schema_file):
            return f"Error: table {other} does not exist"
        try:
            with open(other_schema_file, "r") as sf:
                schema = sf.readline().strip().split(",")
        except Exception:
            return f"Error: could not read schema of table {other}"

        if right.split(".")[-1] != left.split(".")[-1]:
            return f"Error: keypairs of Join conditions must match do not match in name {left} <-> {right}"

        if not check_keypair(left.split(".")[-1], schema_file, other_schema_file):
            return f"Error: keypairs of Join conditions must be Primary Key on one side and Foreign Key on the other"

        cols = [s.split(":")[0] for s in schema]
        other_idx_map = {col: i + 1 for i, col in enumerate(cols)}

        try:
            ldx = other_idx_map[left.split(".")[-1] if other in left else right.split(".")[-1]]
            rdx = idx_map[right.split(".")[-1] if not other in right else left.split(".")[-1]]
        except KeyError:
            return f"Error: join keys not found in schemas"

        cond_post = build_awk_cond(post_filters, idx_map, other_idx_map, other)

        body = 'count++' if is_count else f'OFS=","; print {build_awk_proj(columns, idx_map, other_idx_map, other)}'
        script = (
            f'NR==FNR {{ a[${ldx}]=$0; next }} ' +
            f'{{ split(a[${rdx}], b, FS); if (${rdx} in a) ' +
            (f'if (({cond_post})) ' if cond_post else '') +
            f'{{ {body} }} }}' +
            (' END { print count }' if is_count else '')
        )
        awk_cmd.append(script)
        awk_cmd.extend([other_file, data_file])
    else:
        cond_pre = build_awk_cond(pre_filters, idx_map, other_idx_map=None, other=None)
        if is_count:
            awk_cmd.append(f'{cond_pre} {{ count++ }} END {{ print count }}')
        else:
            proj = build_awk_proj(columns, idx_map, other_idx_map=None, other=None)
            awk_cmd.append(f'{cond_pre} {{ print {proj} }}')
        awk_cmd.append(data_file)

    cmd = " ".join(shlex.quote(part) for part in awk_cmd)
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        return f"Error: {result}"

    output = result.stdout.strip()
    if not output:
        return f"No rows found in table '{table}'" + (
            f" joined with '{other}'" if join else "") + " matching the given conditions."

    if is_count:
        return f"Found {output} matching row(s) in table '{table}'" + (f" joined with '{other}'" if join else "") + f".\n{output}"

    lines = output.split("\n")
    return f"Found {len(lines)} row(s) in table '{table}'" + (
        f" joined with '{other}'" if join else "") + ":\n" + "\n".join(
        f"[{i + 1}] {line}" for i, line in enumerate(lines))

def parse_select(args):
    table = args[1]
    columns_raw = args[2]
    rest = args[3:]
    if "join" in rest:
        j = rest.index("join")
        pre = rest[:j]
        other = rest[j + 1]
        link = rest[j + 2]
        post = rest[j + 3:]
        left, right = link.split("=")
        join = (other, left, right)
    else:
        pre = rest
        post = []
        join = None
    pre_filters = [parse_condition(c) for c in pre]
    post_filters = [parse_condition(c) for c in post]
    return table, columns_raw, pre_filters, join, post_filters

def parse_condition(cond):
    m = re.match(r"^([a-zA-Z_][a-zA-Z0-9_.]*)(>=|<=|[=<>~])(.+)$", cond)
    if not m:
        raise ValueError(f"Invalid condition: {cond}")
    col, op, val = m.groups()
    if val.isdigit():
        val = int(val)
    else:
        val = val.strip("'\"")
    return col, op, val

def build_awk_cond(filters, idx_map, other_idx_map, other):
    parts = []
    for col, op, val in filters:
        if other in col.split(".")[0] and other_idx_map:
            fld = "b[" + str(other_idx_map[col.split(".")[-1]]) + "]"
        else:
            fld = idx_map[col] if "." not in col else idx_map[col.split(".")[-1]]
        lit = f'"{val}"' if isinstance(val, str) else val
        if op == "=":
            op = "=="
        elif op == "~":
            awk_expr = f'${fld} ~ /{val}/' if fld[0] != "b" else f'{fld} ~ /{val}/'
            parts.append(awk_expr)
            continue
        awk_expr = f'${fld} {op} {lit}' if fld[0] != "b" else f'{fld} {op} {lit}'
        parts.append(awk_expr)
    return " && ".join(parts) if parts else "1"

def build_awk_proj(columns, idx_map, other_idx_map, other):
    projection_parts = []
    for c in columns:
        if c == "*":
            projection_parts.append("$0")
        elif other_idx_map and other in c.split(".")[0]:
            col = c.split(".")[-1]
            projection_parts.append(f"b[{other_idx_map[col]}]")
        else:
            col = idx_map[c.split(".")[-1]]
            projection_parts.append(f"${col}")
    return ", ".join(projection_parts)


def check_keypair(field, schema, other_schema):
    with open(schema) as f1, open(other_schema) as f2:
        schema_table_list = f1.readline().strip().split(",")
        other_schema_table_list = f2.readline().strip().split(",")

    p = 0

    field_name = field.split(".")[-1]

    schema_table_map = {}
    other_schema_table_map = {}
    schema_len = len(schema_table_list)
    other_schema_len = len(other_schema_table_list)

    while p < schema_len or p < other_schema_len:
        if p < schema_len:
            col, rest = schema_table_list[p].split(":")
            dtype, sep, flag_part = rest.partition("[")
            flags = [i for i in flag_part.rstrip("]").split() if i in ["PK", "FK"]]
            schema_table_map[col] = flags[0] if flags else ""

        if p < other_schema_len:
            col, rest = other_schema_table_list[p].split(":")
            dtype, sep, flag_part = rest.partition("[")
            flags = [i for i in flag_part.rstrip("]").split() if i in ["PK", "FK"]]
            other_schema_table_map[col] = flags[0] if flags else ""

        p += 1

    return (schema_table_map[field_name] == "PK" and other_schema_table_map[field_name] == "FK" or
            schema_table_map[field_name] == "FK" and other_schema_table_map[field_name] == "PK")







