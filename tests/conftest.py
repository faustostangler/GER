import pytest

# SRE FIX: Força o carregamento de C-Extensions no Main Interpreter 
# antes que o Streamlit AppTest ou pytest-cov alterem os import hooks.
import numpy 
import pandas 

@pytest.fixture(autouse=True)
def prevent_streamlit_module_purge(monkeypatch):
    """
    Impede que o Streamlit expurgue pacotes do .venv da memória
    durante a execução dos testes headless.
    """
    import streamlit.source_util as source_util
    # Desativa silenciosamente o watcher de arquivos locais para testes
    monkeypatch.setattr(source_util, "_cached_pages", None, raising=False)
