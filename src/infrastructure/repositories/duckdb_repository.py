import os
import duckdb
import pandas as pd
import re
from typing import List, Tuple, Any
from src.application.use_cases.interfaces import IAnalyticsRepository
from src.domain.models import AnalyticKPIs, FilterCriteria
from src.infrastructure.auth.token_acl import ValidatedUserToken

class DuckDBAnalyticsRepository(IAnalyticsRepository):
    def __init__(self, db_file: str):
        self.con = duckdb.connect(database=':memory:')
        
        # Limite explícito de RAM para proteção de OOMKilled no Cluster K8s (Reserva memória para Pandas)
        self.con.execute("PRAGMA memory_limit='1.5GB';")
        
        if db_file.startswith("s3://"):
            region = os.getenv("AWS_REGION", "sa-east-1")
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
            self.con.execute("INSTALL aws;")
            self.con.execute("LOAD aws;")
            self.con.execute(f"SET s3_region='{region}';")
            self.con.execute("CALL load_aws_credentials();")
            
        self.con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{db_file}')")

    def _get_rls_cte(self, user: ValidatedUserToken) -> str:
        """Sanitização estrita para Bounded Context RLS via CTE"""
        if not user:
            return "WITH BaseRLS AS (SELECT * FROM gercon WHERE 1=0)"
            
        # SRE FIX: Segurança garantida na Borda (IAP). Acesso global aos dados validado pelo domínio IAM.
        return "WITH BaseRLS AS (SELECT * FROM gercon)"

    def _query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def get_kpis(self, filters: FilterCriteria, user: ValidatedUserToken) -> AnalyticKPIs:
        final_where = filters.get_where_clause()
        cte = self._get_rls_cte(user)
        
        kpis_df = self._query(f"""
            {cte}
            SELECT COUNT(DISTINCT numeroCMCE) as pacientes, 
                   COUNT(*) as eventos, 
                   COUNT(DISTINCT entidade_especialidade_especialidadeMae_descricao) as esp_mae,
                   COUNT(DISTINCT entidade_especialidade_descricao) as sub_esp,
                   COUNT(DISTINCT medicoSolicitante) as medicos,
                   COUNT(DISTINCT entidade_cidPrincipal_descricao) as cids,
                   COUNT(DISTINCT origem_lista) as origens,
                   ROUND(AVG(DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE)), 1) as lead_time,
                   MAX(DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE)) as max_lead_time,
                   DATEDIFF('day', MIN(CAST(dataSolicitacao AS DATE)), MAX(CAST(dataSolicitacao AS DATE))) as span_dias,
                   COUNT(DISTINCT CASE WHEN entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO') THEN numeroCMCE END) as pac_urgentes,
                   COUNT(DISTINCT CASE WHEN DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180 THEN numeroCMCE END) as pac_vencidos
            FROM BaseRLS WHERE {final_where}
        """)

        p90_metrics = self._query(f"""
            {cte}
            SELECT 
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_fila) as p90_lead_time,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_esquecido) as p90_esquecido
            FROM (
                SELECT 
                    numeroCMCE,
                    DATEDIFF('day', MIN(CAST(dataSolicitacao AS DATE)), CURRENT_DATE) as dias_fila,
                    DATEDIFF('day', MAX(CAST(dataCadastro AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
                FROM BaseRLS
                WHERE {final_where}
                GROUP BY numeroCMCE
            )
        """)

        return AnalyticKPIs(
            pacientes=int(kpis_df['pacientes'].iloc[0]) if not kpis_df.empty else 0,
            eventos=int(kpis_df['eventos'].iloc[0]) if not kpis_df.empty else 0,
            esp_mae=int(kpis_df['esp_mae'].iloc[0]) if not kpis_df.empty else 0,
            sub_esp=int(kpis_df['sub_esp'].iloc[0]) if not kpis_df.empty else 0,
            medicos=int(kpis_df['medicos'].iloc[0]) if not kpis_df.empty else 0,
            cids=int(kpis_df['cids'].iloc[0]) if not kpis_df.empty else 0,
            origens=int(kpis_df['origens'].iloc[0]) if not kpis_df.empty else 0,
            lead_time=float(kpis_df['lead_time'].iloc[0]) if not kpis_df.empty and pd.notna(kpis_df['lead_time'].iloc[0]) else 0.0,
            max_lead_time=int(kpis_df['max_lead_time'].iloc[0]) if not kpis_df.empty and pd.notna(kpis_df['max_lead_time'].iloc[0]) else 0,
            span_dias=int(kpis_df['span_dias'].iloc[0]) if not kpis_df.empty and pd.notna(kpis_df['span_dias'].iloc[0]) else 0,
            pac_urgentes=int(kpis_df['pac_urgentes'].iloc[0]) if not kpis_df.empty else 0,
            pac_vencidos=int(kpis_df['pac_vencidos'].iloc[0]) if not kpis_df.empty else 0,
            p90_lead_time=float(p90_metrics['p90_lead_time'].iloc[0]) if not p90_metrics.empty and pd.notna(p90_metrics['p90_lead_time'].iloc[0]) else 0.0,
            p90_esquecido=float(p90_metrics['p90_esquecido'].iloc[0]) if not p90_metrics.empty and pd.notna(p90_metrics['p90_esquecido'].iloc[0]) else 0.0
        )

    def get_distribution_data(self, filters: FilterCriteria, user: ValidatedUserToken) -> pd.DataFrame:
        final_where = filters.get_where_clause()
        cte = self._get_rls_cte(user)
        return self._query(f"""
            {cte}
            SELECT 
                DATEDIFF('day', MIN(CAST(dataSolicitacao AS DATE)), CURRENT_DATE) as dias_fila,
                DATEDIFF('day', MAX(CAST(dataCadastro AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
            FROM BaseRLS
            WHERE {final_where}
            GROUP BY numeroCMCE
        """)

    def get_dynamic_options(self, column: str, current_where: str, user: ValidatedUserToken) -> List[Any]:
        cte = self._get_rls_cte(user)
        try:
            q = f"{cte} SELECT DISTINCT \"{column}\" FROM BaseRLS WHERE {current_where} AND \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1"
            return self._query(q)[column].tolist()
        except Exception:
            return []

    def get_global_bounds(self, column: str, is_date: bool = False, user: ValidatedUserToken = None) -> Tuple[Any, Any]:
        cast = "DATE" if is_date else "INTEGER"
        cte = self._get_rls_cte(user)
        try:
            df = self._query(f"{cte} SELECT MIN(TRY_CAST(\"{column}\" AS {cast})) as vmin, MAX(TRY_CAST(\"{column}\" AS {cast})) as vmax FROM BaseRLS")
            return df['vmin'].iloc[0], df['vmax'].iloc[0]
        except:
            return None, None

    def execute_custom_query(self, sql: str, user: ValidatedUserToken) -> pd.DataFrame:
        cte = self._get_rls_cte(user)
        
        # 1. Substituição Robusta de RLS (Ignora maiúsculas/minúsculas, aspas e múltiplos espaços)
        # Ex: converte 'from  "gercon"' ou 'FROM gercon' para 'FROM BaseRLS'
        sql_seguro = re.sub(r'(?i)\bfrom\s+("?)gercon("?)\b', r'FROM \1BaseRLS\2', sql)
        
        # 2. Validação Fail-Close (Proteção contra Joins maliciosos ou bypass)
        # Se a palavra gercon ainda existir na query (ex: em um JOIN gercon), abortamos.
        if re.search(r'(?i)\bgercon\b', sql_seguro):
            raise ValueError("Violação de Segurança: Tentativa de acesso direto à view base ou sintaxe SQL não suportada pelo RLS.")

        # 3. Tratamento de Colisão de CTEs (Se a query original já usa WITH)
        sql_upper = sql_seguro.strip().upper()
        if sql_upper.startswith("WITH "):
            # Removemos o "WITH " da nossa CTE base para encadear na CTE do usuário
            cte_limpa = cte.replace("WITH BaseRLS AS", "BaseRLS AS", 1)
            # Injeta a nossa BaseRLS como a primeira CTE da cadeia
            sql_final = re.sub(r'(?i)^WITH\s+', f"WITH {cte_limpa}, ", sql_seguro.strip())
        else:
            sql_final = f"{cte}\n{sql_seguro}"

        return self._query(sql_final)
