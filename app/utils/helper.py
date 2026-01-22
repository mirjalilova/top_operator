import random
import string
from uuid import UUID

def generate_password(length: int = 8) -> str:
    if length < 8:
        raise ValueError("Password length must be at least 8")

    letters = string.ascii_letters
    digits = string.digits
    symbols = "!@#$%^&*"

    password = [
        random.choice(letters),
        random.choice(digits),
        random.choice(symbols),
    ]

    all_chars = letters + digits + symbols
    password += random.choices(all_chars, k=length - len(password))

    random.shuffle(password)
    return "".join(password)

def is_valid_uuid(val: str) -> bool:
    try:
        UUID(val)
        return True
    except ValueError:
        return False
