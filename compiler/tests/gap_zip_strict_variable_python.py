values = [1, 2]
other = [10]
strict_mode = True
try:
    print(list(zip(values, other, strict=strict_mode)))
except ValueError:
    print("caught-mismatch")

strict_mode = False
print(list(zip(values, other, strict=strict_mode)))

try:
    print(list(zip(values, other, strict=1)))
except ValueError:
    print("caught-mismatch-int")
