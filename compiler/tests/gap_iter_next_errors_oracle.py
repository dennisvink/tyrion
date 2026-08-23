try:
    next(iter([]))
except StopIteration as error:
    print(type(error).__name__)
