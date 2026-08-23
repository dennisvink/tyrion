def keep(value):
    return value != "b"

print(list(filter(keep, "abc")))
print(list(filter(None, (0, 1, "", "x"))))
