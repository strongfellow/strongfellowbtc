
import hashlib

def double_sha256(bs):
    return hashlib.sha256(hashlib.sha256(bs).digest()).digest()

