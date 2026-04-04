# src/presentation/adapters/parsers.py
def parse_term(term: str) -> str:
    if not term or not str(term).strip():
        return ""
    
    term = str(term).strip()
    
    # Se o usuário injetou o wildcard explicitamente (*), nós respeitamos a intenção dele
    if "*" in term:
        return term.replace("*", "%")
    
    # Comportamento SRE padrão: Se não há wildcard, busca por Contenção (Contains)
    return f"%{term}%"
