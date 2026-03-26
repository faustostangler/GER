import duckdb
import pandas as pd
from typing import List, Tuple, Any
from src.application.use_cases.interfaces import IAnalyticsRepository
from src.domain.models import AnalyticKPIs, FilterCriteria

class DuckDBAnalyticsRepository(IAnalyticsRepository):
    def __init__(self, db_file: str):
        self.con = duckdb.connect(database=':memory:')
        self.con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{db_file}')")

    def _query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def get_kpis(self, filters: FilterCriteria) -> AnalyticKPIs:
        final_where = filters.get_where_clause()
        
        kpis_df = self._query(f"""
            SELECT COUNT(DISTINCT Protocolo) as pacientes, 
                   COUNT(*) as eventos, 
                   COUNT(DISTINCT "Especialidade Mãe") as esp_mae,
                   COUNT(DISTINCT Especialidade) as sub_esp,
                   COUNT(DISTINCT "Médico Solicitante") as medicos,
                   COUNT(DISTINCT "CID Descrição") as cids,
                   COUNT(DISTINCT "Origem da Lista") as origens,
                   ROUND(AVG(DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE)), 1) as lead_time,
                   MAX(DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE)) as max_lead_time,
                   DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), MAX(CAST("Data Solicitação" AS DATE))) as span_dias,
                   COUNT(DISTINCT CASE WHEN "Risco Cor" IN ('VERMELHO', 'LARANJA', 'AMARELO') THEN Protocolo END) as pac_urgentes,
                   COUNT(DISTINCT CASE WHEN DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE) > 180 THEN Protocolo END) as pac_vencidos
            FROM gercon WHERE {final_where}
        """)

        p90_metrics = self._query(f"""
            SELECT 
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_fila) as p90_lead_time,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_esquecido) as p90_esquecido
            FROM (
                SELECT 
                    Protocolo,
                    DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), CURRENT_DATE) as dias_fila,
                    DATEDIFF('day', MAX(CAST(Data_Evolucao AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
                FROM gercon
                WHERE {final_where}
                GROUP BY Protocolo
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

    def get_distribution_data(self, filters: FilterCriteria) -> pd.DataFrame:
        final_where = filters.get_where_clause()
        return self._query(f"""
            SELECT 
                DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), CURRENT_DATE) as dias_fila,
                DATEDIFF('day', MAX(CAST(Data_Evolucao AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
            FROM gercon
            WHERE {final_where}
            GROUP BY Protocolo
        """)

    def get_dynamic_options(self, column: str, current_where: str) -> List[Any]:
        try:
            q = f"SELECT DISTINCT \"{column}\" FROM gercon WHERE {current_where} AND \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1"
            return self._query(q)[column].tolist()
        except Exception:
            return []

    def get_global_bounds(self, column: str, is_date: bool = False) -> Tuple[Any, Any]:
        cast = "DATE" if is_date else "INTEGER"
        try:
            df = self._query(f"SELECT MIN(TRY_CAST(\"{column}\" AS {cast})) as vmin, MAX(TRY_CAST(\"{column}\" AS {cast})) as vmax FROM gercon")
            return df['vmin'].iloc[0], df['vmax'].iloc[0]
        except:
            return None, None

    def execute_custom_query(self, sql: str) -> pd.DataFrame:
        return self._query(sql)
