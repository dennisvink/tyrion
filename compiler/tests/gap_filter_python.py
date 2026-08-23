def positive(value):
    return value > 0

print("positive", list(filter(positive, [-2, -1, 0, 1, 2])))
print("none", list(filter(None, [0, 1, 0, 2])))
print("empty", list(filter(positive, [])))
