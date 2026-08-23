import os

path = "/tmp/tyrion-print-gap.txt"
try:
    os.unlink(path)
except FileNotFoundError:
    pass

print("alpha", "beta", sep="-", end="!")
print("defaults", sep=None, end=None)
output = open(path, "w")
print("file", "value", sep=":", end=";", file=output, flush=True)
output.close()
print("stdout", file=None, flush=True)
with open(path, "rb") as output:
    output.read()
