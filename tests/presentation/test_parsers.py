from presentation.adapters.parsers import parse_term

def test_parse_term_replaces_asterisk_with_sql_wildcard():
    # Testa a conversão do wildcard da UI (*) para o SQL (%)
    assert parse_term("cardio*") == "cardio%"
    assert parse_term("*urgia") == "%urgia"
    assert parse_term("clínica") == "clínica"

def test_parse_term_handles_empty_or_whitespace():
    # Testa resiliência a strings vazias
    assert parse_term("") == ""
    assert parse_term("   ") == "   "
