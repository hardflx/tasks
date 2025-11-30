import os
import hashlib

FOLDER = r"C:\Users\hardf\Desktop\tasks\task #2_FOR_ALL_GROUPS\task_data"
EMAIL  = "konstantynovkostiantyn@gmail.com"

hashes = []

for filename in os.listdir(FOLDER):
    path = os.path.join(FOLDER, filename)
    if os.path.isfile(path):
        h = hashlib.sha3_256()
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        hashes.append(h.hexdigest())

def sort_key(hash_hex):
    product = 1
    for ch in hash_hex:
        product *= int(ch, 16) + 1
    return product

final_input = ("".join(sorted(hashes, key=sort_key))) + EMAIL

final_hash = hashlib.sha3_256(final_input.encode("utf-8")).hexdigest()

print(final_hash)