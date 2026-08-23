def add(left, right):
    return left + right

def add3(first, second, third):
    return first + second + third

def double(value):
    return value * 2

print("one", list(map(double, [1, 2, 3])))
print("many", list(map(add, [1, 2, 3], [10, 20, 30])))
print("shortest", list(map(add, [1, 2], [10, 20, 30])))
print("three", list(map(add3, [1, 2], [10, 20], [100, 200])))
print("empty", list(map(double, [])))
try:
    print("none", list(map(None, [1, 2])))
except TypeError as error:
    print("none-error", type(error).__name__)
try:
    print("missing-iterable", list(map(add)))
except TypeError:
    print("missing-iterable-error")
