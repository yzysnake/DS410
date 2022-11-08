import string

string.punctuation += '”“'
cache = {}

with open("/Users/ziyuanye/Documents/PSU/2022 Fall/DS 410/hw-fall2022-yzysnake/mrhw/q4/testfile.txt") as infile:
    for line in infile:
        new_line = line.translate(str.maketrans('', '', string.punctuation)).strip('\n')
        words = new_line.split(" ")
        for w in words:
            if w.isalpha():
                w = w.lower()
                if cache.get(w) is None:
                    cache.update({w: 1})
                else:
                    cache[w] += 1


print(cache)
