import sys

args = sys.argv
if len(args) < 3:
    params = " ".join([f"<{args[i]}>" for i in range(2, len(args))])
    print(f"Usage: <{args[0]}> {params}")
    sys.exit(1)

params = " ".join([f"<{args[i]}>" for i in range(1, len(args))])
print(f"Usage: <{args[0]}> {params}")

command = args[1]

try:
    module = __import__(f"commands.{command}", fromlist=["run"])
    print(module.run(args[1:]))
except ModuleNotFoundError:
    print(f"Unknown command: {command}")

