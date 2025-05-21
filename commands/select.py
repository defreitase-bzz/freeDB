import re
import os
import subprocess
import shlex

def run(args):
    if len(args) < 3:
        params = "<table> <columns> [<conds> …] [join <other> <left>=<right>] [<post-conds> …]"
        return f"Usage: <{args[0]}> {params}"

    table, columns, pre_filters, join, post_filters = parse_select(args)

    data_file   = f"db/{table}/{table}.data"
    schema_file = f"db/{table}/{table}.schema"
    if not os.path.exists(schema_file):
        return f"Error: table {table} does not exist"

    with open(schema_file, "r") as sf:
        schema = sf.readline().strip().split(",")
    cols = [s.split(":")[0] for s in schema]
    idx_map = {col: i+1 for i, col in enumerate(cols)}

    awk_cmd = ["awk", 'BEGIN { FPAT = "([^,]+)|(\\"[^\\"]+\\")" }']

    if join:
        other, left, right = join
        other_file = f"db/{other}/{other}.data"

        other_schema_file = f"db/{other}/{other}.schema"
        if not os.path.exists(other_schema_file):
            return f"Error: table {other} does not exist"

        with open(other_schema_file, "r") as sf:
            schema = sf.readline().strip().split(",")

        cols = [s.split(":")[0] for s in schema]
        other_idx_map = {col: i + 1 for i, col in enumerate(cols)}

        ldx = other_idx_map[left]
        rdx = idx_map[right]
        cond_post = build_awk_cond(post_filters, idx_map, other_idx_map)
        script = (
            f'NR==FNR {{ a[${rdx}]=$0; next }} '
            f'{{ if (${ldx} in a) ' +
            (f'if ({cond_post}) ' if cond_post else '') +
            f'print $0 FS a[${ldx}] }}'
        )
        awk_cmd.append(script)
        awk_cmd.extend([other_file, data_file])
    else:
        cond_pre = build_awk_cond(pre_filters, idx_map, other_idx_map=None)
        proj = build_awk_proj(columns, idx_map, other_idx_map=None)
        awk_cmd.append(f'{cond_pre} {{ print {proj} }}')
        awk_cmd.append(data_file)

    cmd = " ".join(shlex.quote(part) for part in awk_cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        return f"Error running AWK: {result.stderr.strip()}"
    return result.stdout

def parse_select(args):
    table   = args[1]
    columns = args[2].split(",")
    rest    = args[3:]
    if "join" in rest:
        j = rest.index("join")
        pre  = rest[:j]
        other = rest[j+1]
        link  = rest[j+2]
        post  = rest[j+3:]
        left, right = link.split("=")
        join = (other, left, right)
    else:
        pre  = rest
        post = []
        join = None
    pre_filters  = [parse_condition(c) for c in pre]
    post_filters = [parse_condition(c) for c in post]
    return table, columns, pre_filters, join, post_filters

def parse_condition(cond):
    m = re.match(r"^([^=<>]+)([=<>]{1})(.+)$", cond)
    if not m:
        raise ValueError(f"Invalid condition: {cond}")
    col, op, val = m.groups()
    if val.isdigit():
        val = int(val)
    else:
        val = val.strip("'\"")
    return col, op, val

def build_awk_cond(filters, idx_map, other_idx_map):
    parts = []
    for col, op, val in filters:
        if "." in col and other_idx_map:
            fld = other_idx_map[col]
        else:
            fld = idx_map[col]
        lit = f'"{val}"' if isinstance(val, str) else val
        op = "==" if op == "=" else op
        parts.append(f'${fld} {op} {lit}')
    return " && ".join(parts) if parts else "1"

def build_awk_proj(columns, idx_map, other_idx_map):
    if columns == ["*"]:
        return "$0"

    projection_parts = []

    for c in columns:
        if other_idx_map and "." in c:
            col = c.split(",")[-1]
            projection_parts.append(f"${other_idx_map[col]}")
        else:
            projection_parts.append(f"${idx_map[c]}")
    return ", ".join(projection_parts)
