
import re

LEET_MAP = str.maketrans({
    "@": "a", "$": "s", "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t",
    "_": " ", "+": " ", "-": " ", "/": " ", "\\": " ", ".": " ", ",": " "
})

def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = s.translate(LEET_MAP)
    s = re.sub(r"\b(\d)k\b", lambda m: str(int(m.group(1)) * 1000), s)
    s = (s
        .replace("microsft", "microsoft")
        .replace("enpoint", "endpoint")
        .replace("defnd", "defend")
        .replace("wizshield", "wiz")
    )
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokens(s: str):
    return [t for t in normalize_text(s).split(" ") if t]
