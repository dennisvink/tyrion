name = "Arya"
score = 7
values = [10, 20, 30]

print(f"{name} scored {score}")
print(F"House {name}")
print(f"math {2 + 3} index {values[1]}")
print(f"braces {{winter}} and empty:{''}:")
print(f"values {True} {False} {None} {2.5}")
print(f"")

events = []


def mark(value):
    events.append(value)
    return value


print(f"order {mark('A')}{mark('B')}")
print(events)
