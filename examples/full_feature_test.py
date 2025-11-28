print("== primitives ==")
a = 1
b = 2.5
c = "hi"
d = True
print(a, b, c, d)

print("== arithmetic ==")
print(a + b, b - a, a * 3, b / 2, -a)
print(a == 1, a != 2, b > 2.0, b <= 2.5)

print("== conversions ==")
print(int(3.7), float(4), str(False))

print("== collections + indexing/slicing/methods ==")
lst = [1, 2, 3]
tpl = (4, 5, 6)
dct = {"x": 1, "y": 2}
st = {1, 2}
print(lst[0], tpl[1], "slice", [9, 8, 7][1:3], "strslice", "abcde"[1:4])
lst = lst.append(4)
lst = lst.remove(2)
st = st.add(3)
dct["z"] = 3
print("list", lst, "tuple", tpl, "dict z", dct["z"], "set", st)
print("dict keys", dct.keys(), "items", dct.items())

print("== control flow: if/elif/else ==")
if a < 0:
    print("neg")
elif a == 1:
    print("one")
else:
    print("other")

print("== while loop ==")
i = 3
while i > 0:
    print(i)
    i = i - 1

print("== for loops ==")
total = 0
for n in lst:
    total = total + n
print("sum list", total)

pairs = []
for idx, val in enumerate(tpl):
    pairs.append((idx, val))
print("enumerate tuple", pairs)

dict_sum = 0
for k in dct:
    dict_sum = dict_sum + dct[k]
print("dict keys sum", dict_sum)

item_sum = 0
for k, v in dct.items():
    item_sum = item_sum + v
print("dict items sum", item_sum)

range_list = []
for r in range(2, -1, -1):
    range_list.append(r)
print("range reverse", range_list)

set_list = []
for s in st:
    set_list.append(s)
print("set iter", set_list)

print("== file handling ==")
with open("/tmp/pycomp_full.txt", "w") as f:
    f.write("hello file")

with open("/tmp/pycomp_full.txt", "r") as f:
    data = f.read()
print("file read", data)

print("== functions + lambdas ==")

def add(x, y):
    return x + y

def show(v):
    print("show", v)

res = add(5, 7)
print("add result", res)
show(res)

double = lambda n: n * 2
print("lambda", double(3))

print("== error handling ==")

def risky(v):
    if v < 0:
        raise "neg"
    return v * 2

try:
    risky(-5)
    print("should not reach")
except neg as err:
    print("caught", err)
finally:
    print("finally ran")

print("== comprehensions ==")
nums = [1, 2, 3, 4]
squares = [x * x for x in nums]
big = [x for x in nums if x > 2]
pairs = {k: k + 1 for k in nums if k < 4}
print("squares", squares, "big", big, "pairs", pairs)

print("== unpacking + sorted ==")
a, b = (10, 20)
first, *rest = [1, 2, 3, 4]
print("unpack", a, b, first, rest)
sorted_vals = sorted([3, 1, 2])
sorted_by_key = sorted([3, 1, 2], lambda x: 0 - x)
print("sorted", sorted_vals, sorted_by_key)

print("== aug assign + underscores ==")
big = 1_000_000
acc = 0
for i in range(3):
    acc += i
print("big", big, "acc", acc)
