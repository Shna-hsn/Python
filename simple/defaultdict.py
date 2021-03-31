from collections import defaultdict
result = defaultdict(list)
data = [("p", 1), ("p", 2), ("p", 3),("h", 1), ("h", 2), ("h", 3)]
print(data)
for (key, value) in data:
    result[key].append(value)
print(result)