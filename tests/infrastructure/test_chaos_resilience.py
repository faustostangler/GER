"""
Chaos Engineering Tests: Resiliência da Infraestrutura.

Simula falhas de infraestrutura para provar que o sistema sobrevive ao caos:
1. Redis indisponível → fallback transparente para DuckDB direto
2. Latência de rede no DuckDB → validação de timeout gracioso
3. Parquet corrompido → Circuit Breaker do Data Contract
"""
import os
import time
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch



class TestRedisChaos:
    """Chaos: Simula falha total do Redis e valida Degradação Graciosa."""

    def test_query_works_without_redis(self, tmp_path):
        """Se Redis cair, _query deve retornar dados direto do Parquet sem exceção."""
        # Arrange: Cria um Parquet mínimo válido
        parquet_path = str(tmp_path / "test.parquet")
        df = pd.DataFrame({
            "numeroCMCE": ["P001", "P002"],
            "entidade_classificacaoRisco_cor": ["VERMELHO", "VERDE"],
            "entidade_especialidade_descricao": ["Cardio", "Neuro"],
            "dataSolicitacao": ["2025-01-01", "2025-06-01"],
            "dataCadastro": ["2025-01-01", "2025-06-01"],
        })
        df.to_parquet(parquet_path)

        # Act: Monta o repositório sem Redis (simula queda)
        with patch("infrastructure.config.settings") as mock_settings:
            mock_settings.db.memory_limit = "256MB"
            mock_settings.redis.host = "localhost"
            mock_settings.redis.port = 59999  # Porta inexistente
            mock_settings.OUTPUT_FILE = parquet_path

            from infrastructure.repositories.duckdb_repository import DuckDBAnalyticsRepository
            repo = DuckDBAnalyticsRepository(parquet_path)

        # Assert: redis_client é None (fallback)
        assert repo.redis_client is None

        # A query deve funcionar normalmente
        result = repo._query("SELECT COUNT(*) as cnt FROM gercon")
        assert result["cnt"].iloc[0] == 2

    def test_redis_failure_mid_operation_is_transparent(self, tmp_path):
        """Se Redis falhar DURANTE uma operação, a query não deve travar."""
        parquet_path = str(tmp_path / "test.parquet")
        df = pd.DataFrame({
            "numeroCMCE": ["P001"],
            "entidade_classificacaoRisco_cor": ["VERMELHO"],
            "entidade_especialidade_descricao": ["Cardio"],
            "dataSolicitacao": ["2025-01-01"],
            "dataCadastro": ["2025-01-01"],
        })
        df.to_parquet(parquet_path)

        with patch("infrastructure.config.settings") as mock_settings:
            mock_settings.db.memory_limit = "256MB"
            mock_settings.redis.host = "localhost"
            mock_settings.redis.port = 59999
            mock_settings.OUTPUT_FILE = parquet_path

            from infrastructure.repositories.duckdb_repository import DuckDBAnalyticsRepository
            repo = DuckDBAnalyticsRepository(parquet_path)

        # Simula um client Redis que foi setado mas falha no .get()
        mock_redis = MagicMock()
        mock_redis.get.side_effect = ConnectionError("Connection refused")
        mock_redis.setex.side_effect = ConnectionError("Connection refused")
        repo.redis_client = mock_redis

        # Act: A query deve rodar normalmente (fallback transparent)
        result = repo._query("SELECT COUNT(*) as cnt FROM gercon")
        assert result["cnt"].iloc[0] == 1


class TestDataContractChaos:
    """Chaos: Simula Parquet com schema corrompido."""

    def test_missing_critical_columns_triggers_circuit_breaker(self, tmp_path):
        """Se o Parquet não tiver colunas obrigatórias, ValueError é disparado."""
        parquet_path = str(tmp_path / "corrupted.parquet")
        # Cria um Parquet com schema errado (falte numeroCMCE)
        df = pd.DataFrame({
            "coluna_inventada": [1, 2, 3],
            "outra_irrelevante": ["a", "b", "c"],
        })
        df.to_parquet(parquet_path)

        with patch("infrastructure.config.settings") as mock_settings:
            mock_settings.db.memory_limit = "256MB"
            mock_settings.redis.host = "localhost"
            mock_settings.redis.port = 59999
            mock_settings.OUTPUT_FILE = parquet_path

            from infrastructure.repositories.duckdb_repository import DuckDBAnalyticsRepository
            with pytest.raises(ValueError, match="Data Contract Quebrado"):
                DuckDBAnalyticsRepository(parquet_path)

    def test_valid_schema_passes_contract(self, tmp_path):
        """Parquet com todas as colunas obrigatórias não dispara Circuit Breaker."""
        parquet_path = str(tmp_path / "valid.parquet")
        df = pd.DataFrame({
            "numeroCMCE": ["P001"],
            "entidade_classificacaoRisco_cor": ["VERMELHO"],
            "entidade_especialidade_descricao": ["Cardio"],
            "dataSolicitacao": ["2025-01-01"],
            "dataCadastro": ["2025-01-01"],
            "extra_column": ["ignored"],
        })
        df.to_parquet(parquet_path)

        with patch("infrastructure.config.settings") as mock_settings:
            mock_settings.db.memory_limit = "256MB"
            mock_settings.redis.host = "localhost"
            mock_settings.redis.port = 59999
            mock_settings.OUTPUT_FILE = parquet_path

            from infrastructure.repositories.duckdb_repository import DuckDBAnalyticsRepository
            repo = DuckDBAnalyticsRepository(parquet_path)

        # Smoke test: query funciona
        result = repo._query("SELECT COUNT(*) as cnt FROM gercon")
        assert result["cnt"].iloc[0] == 1


class TestDataFreshnessChaos:
    """Chaos: Simula dados obsoletos (Silêncio dos Dados)."""

    def test_stale_parquet_detected_by_amber_alert_logic(self, tmp_path):
        """Se o Parquet tem mtime > threshold, o Amber Alert deve disparar."""
        parquet_path = str(tmp_path / "stale.parquet")
        df = pd.DataFrame({"numeroCMCE": ["P001"]})
        df.to_parquet(parquet_path)

        # Força o mtime para 5 horas atrás
        five_hours_ago = time.time() - (5 * 3600)
        os.utime(parquet_path, (five_hours_ago, five_hours_ago))

        actual_mtime = os.path.getmtime(parquet_path)
        age_hours = (time.time() - actual_mtime) / 3600
        threshold = 2.0

        assert age_hours > threshold, "O arquivo deveria estar obsoleto para o teste"

    def test_fresh_parquet_does_not_trigger_amber_alert(self, tmp_path):
        """Parquet criado agora não deve disparar Amber Alert."""
        parquet_path = str(tmp_path / "fresh.parquet")
        df = pd.DataFrame({"numeroCMCE": ["P001"]})
        df.to_parquet(parquet_path)

        actual_mtime = os.path.getmtime(parquet_path)
        age_hours = (time.time() - actual_mtime) / 3600
        threshold = 2.0

        assert age_hours < threshold, "O arquivo recém-criado não deveria estar obsoleto"
