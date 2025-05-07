def run(args):

    if len(args) < 2:
        params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
        print(f"Usage: <{args[0]}> {params}")
        return

    params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
    print(f"Usage: <{args[0]}> {params}")