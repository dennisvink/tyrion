values = (1, 2, 1, 3, 1)
print(values.count(1), values.count(9))
print(values.index(1, 1), values.index(1, -2), values.index(1, 0, 2))
try:
    print(values.index(9))
except ValueError:
    print("missing-error")
