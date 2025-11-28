# Tyrion

Tyrion is a tiny, Python-derived scripting language that compiles to native binaries by first emitting C and then invoking your system C compiler. It keeps Pythonic syntax and semantics for a focused subset: primitives, collections, control flow, functions/lambdas, files, comprehensions, exceptions, and a handful of helpers like `sorted`, `range`, and unpacking. Source files use the `.ty` extension.

## Quick Start

Prereqs: Rust (cargo) and a C toolchain (`cc` on PATH).

Install globally (recommended):
```bash
cargo install --path .
# ensure ~/.cargo/bin is on PATH, then:
tyrion examples/hello.ty ./hello_hello
./hello_hello
```

Or run directly from source without installing:

```bash
cargo run -- examples/hello.ty ./hello_hello
./hello_hello
```

You can also build the compiler once and reuse:

```bash
cargo build --release
./target/release/tyrion program.ty ./program
./program
```

## Language Overview (current feature set)

- **Primitives**: `int`, `float`, `str`, `bool`, unary `-`, arithmetic `+ - * /`, comparisons `== != < <= > >=`, truthiness consistent with Python.
- **IO + conversions**: `print(...)`, `input(prompt)`, `int(x)`, `float(x)`, `str(x)`.
- **Collections**: list `[...]`, tuple `(...)`, dict `{k: v}`, set `{1, 2}`; indexing/slicing (`x[0]`, `x[1:3]`); methods `.append/.remove` (list), `.add` (set), `.keys/.items` (dict); dict assignment `d["k"] = v`.
- **Control flow**: `if/elif/else`, `while`, `for` over `range`, `enumerate`, list/tuple/set/dict/str iteration.
- **Functions + lambdas**: `def name(args): ... return x`, `lambda args: expr`, first-class callables.
- **Classes**: `class C: def __init__(self,...); self.x`; instantiate with `C(...)`; attribute get/set; methods bind `self`.
- **With + files**: `with open(path, mode) as f: ...`, `f.read()`, `f.write(s)`.
- **Exceptions**: `try/except/finally`, `raise <value>`, catch by name (`except MyErr as e:` compares string name).
- **Comprehensions**: list/dict comprehensions with optional `if` guard.
- **Helpers**: `sorted(iterable, key=func)`, unpacking (`a, b = ...`, `first, *rest = ...`), `range`, `enumerate`.

## Examples

### Hello + arithmetic
```python
# examples/hello.ty
msg = "world"
print("hello", msg)
x = 3
print("math", x + 2, x * 4, -x)
```

### Collections + slicing + methods
```python
nums = [1, 2, 3]
tpl = (4, 5, 6)
dct = {"x": 1, "y": 2}
st = {1, 2}

print(nums[0], tpl[1], "slice", nums[1:3], "strslice", "abcde"[1:4])
nums = nums.append(4)
nums = nums.remove(2)
st = st.add(3)
dct["z"] = 3
print("list", nums, "tuple", tpl, "dict z", dct["z"], "set", st)
print("dict keys", dct.keys(), "items", dct.items())
```

### Control flow + loops
```python
total = 0
for n in nums:
    total = total + n
print("sum", total)

count = 3
while count > 0:
    print("tick", count)
    count = count - 1

for i, v in enumerate((10, 20, 30)):
    print("enum", i, v)

rev = []
for r in range(3, 0, -1):
    rev.append(r)
print("range reverse", rev)
```

### Functions, lambdas, sorted, unpacking
```python
def add(x, y):
    return x + y

twice = lambda n: n * 2

print("add", add(5, 7))
print("lambda", twice(3))

vals = [3, 1, 2]
print("sorted", sorted(vals))
print("sorted key", sorted(vals, lambda v: 0 - v))

a, b = (10, 20)
first, *rest = [1, 2, 3, 4]
print("unpack", a, b, first, rest)
```

### Comprehensions
```python
nums = [1, 2, 3, 4]
squares = [x * x for x in nums]
big = [x for x in nums if x > 2]
pairs = {k: k + 1 for k in nums if k < 4}
print("comps", squares, big, pairs)
```

### Classes
```python
class Greeter:
    def __init__(self, name):
        self.name = name
    def greet(self, other):
        print("Hello", other, "I'm", self.name)

g = Greeter("Arya")
g.greet("Sansa")
```

### Exceptions
```python
def risky(v):
    if v < 0:
        raise "neg"
    return v * 2

try:
    risky(-5)
except neg as err:
    print("caught", err)
finally:
    print("finally ran")
```

### Files + with
```python
with open("/tmp/tyrion_demo.txt", "w") as f:
    f.write("hello file")

with open("/tmp/tyrion_demo.txt", "r") as f:
    data = f.read()
print("file read", data)
```

### Full-feature demo
See `full_feature_test.ty` for a combined script covering all features.

## Limitations / Notes

- No classes or modules yet.
- No keyword args (except `key=` in `sorted`), no default params, no kwargs/varargs.
- Exceptions match by name string; there’s no class-based hierarchy.
- Printing of containers is summarized (`<list len=...>` etc.) rather than full repr.
- Generated C is a temporary step; the output binary is native. Cleanup of temp C files is automatic.

## Contributing / Hacking

- Core code: Rust (`src/main.rs`) plus C runtime (`src/prelude_c.txt`).
- Add new syntax in the hand-rolled parser, then thread through codegen and the runtime helpers.
- Tests: use `full_feature_test.ty` or add new examples under `examples/`.
