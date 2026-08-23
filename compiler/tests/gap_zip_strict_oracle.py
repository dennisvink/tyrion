def zip_strict(*iterables, strict=False):
    values = [list(value) for value in iterables]
    if strict and values:
        expected = len(values[0])
        if any(len(value) != expected for value in values[1:]):
            raise ValueError("zip() argument lengths differ")
    return list(zip(*values))

print(zip_strict([1, 2], [10, 20], strict=False))
print(zip_strict([1, 2], [10, 20], [100, 200], strict=True))
print(zip_strict([1, 2], [10, 20, 30], strict=False))
