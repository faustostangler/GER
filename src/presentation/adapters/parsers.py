import re

def parse_term(term: str) -> str:
    """
    SRE FIX: Sanitização contra caracteres de controle SQL perigosos.
    Traduz a entrada do usuário para um formato seguro no banco de dados.
    """
    t = re.sub(r"[;]|--", "", term.strip())
    t = t.replace("'", "''")
    t = t.replace("*", "%")

    if not t.startswith("%"):
        t = f"%{t}"
    if not t.endswith("%"):
        t = f"{t}%"

    return t
