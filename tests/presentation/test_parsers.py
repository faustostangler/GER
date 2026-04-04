# tests/presentation/test_parsers.py
from presentation.adapters.parsers import parse_term

def test_parse_term_replaces_asterisk_with_sql_wildcard():
    # Wildcard no fim (Starts With)
    assert parse_term("cardio*") == "cardio%"
    
    # Wildcard no início (Ends With)
    assert parse_term("*urgia") == "%urgia"
    
    # Wildcard no meio (In Between)
    assert parse_term("clínica*médica") == "clínica%médica"

def test_parse_term_handles_empty_or_whitespace():
    # Resiliência a strings sujas
    assert parse_term("") == ""
    assert parse_term("   ") == ""

def test_parse_term_defaults_to_contains_without_asterisk():
    # Sem curinga, o padrão SRE é envolver em porcentagens
    assert parse_term("pediatria") == "%pediatria%"
