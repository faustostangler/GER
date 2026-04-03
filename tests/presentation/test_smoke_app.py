import pytest

def test_app_analytics_compiles_and_imports_successfully():
    try:
        import app_analytics  # noqa: F401
    except ImportError as e:
        pytest.fail(f"Falha de importação detectada na camada de apresentação: {e}")
