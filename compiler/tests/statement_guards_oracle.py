enabled = True
disabled = False

if enabled:
    print("if-true")
if disabled:
    print("if-false")
if not disabled:
    print("unless-true")
if not enabled:
    print("unless-false")


def choose(flag):
    if flag:
        return "chosen"
    return "fallback"


def guarded_raise(flag):
    try:
        if flag:
            raise RuntimeError("guarded")
    except RuntimeError:
        return "raised"
    return "not-raised"


value = 1
if enabled:
    value = 2
if not enabled:
    value = 3
print(choose(True), choose(False), value)
print(guarded_raise(True), guarded_raise(False))

seen = []
for number in range(7):
    if number == 1:
        continue
    if number == 5:
        break
    seen.append(number)
print(seen)
