"""
Utility SRE: SQLite to Parquet (Clean Break Schema)

Lê os payloads brutos do SQLite (gercon_raw_data.db), processa todos
via o novo Event Sourcing Mapper (solicitacao_mapper.py) e grava 
o resultado final otimizado em Parquet para o DuckDB/Analytics.
"""
import sqlite3
import json
import pandas as pd
import logging
import gc
from src.domain.solicitacao_mapper import flatten_solicitacao, clean_data_row
from src.infrastructure.telemetry.logger import setup_structured_logger

logger = setup_structured_logger("sqlite_to_parquet")

def run_conversion():
    db_path = "gercon_raw_data.db"
    parquet_out = "gercon_consolidado.parquet"
    
    logger.info(f"🔄 Iniciando conversão de {db_path} para {parquet_out}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Conta o volume total
        cursor.execute("SELECT COUNT(1) FROM solicitacoes_raw")
        total = cursor.fetchone()[0]
        logger.info(f"📊 Total de registros brutos no SQLite: {total}")
        
        if total == 0:
            logger.warning("Nenhum dado encontrado no SQLite para converter.")
            return

        cursor.execute("SELECT protocolo, conteudo_json, origem_lista FROM solicitacoes_raw")
        
        logger.info("⚙️ Iniciando processamento pelo Mapper DDD (Clean Break) em chunks (SRE)...")
        
        batch_size = 5000
        processed_total = 0
        errors = 0
        writer = None
        
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            records = []
            for row in rows:
                protocolo_raw, json_str, origem_lista = row
                try:
                    j_dict = json.loads(json_str)
                    flat = flatten_solicitacao(j_dict, origem_lista)
                    limpo = clean_data_row(flat)
                    
                    if limpo.get("numeroCMCE"):
                        records.append(limpo)
                    else:
                        errors += 1
                except Exception as e:
                    errors += 1
            
            if not records:
                continue
                
            # Transforma chunk em DataFrame
            df_chunk = pd.DataFrame(records)
            
            # --- SRE FIX: Schema Parity & Type Persistence ---
            # PyArrow infere "null" se o primeiro chunk estiver vazio para uma coluna.
            # Forçamos tipos estritos para garantir que o schema do arquivo seja consistente.
            
            # 1. Datas
            colunas_data = [
                "dataSolicitacao", "usuarioSUS_dataNascimento", "dataCadastro",
                "dataPrimeiroAgendamento", "dataPrimeiraAutorizacao"
            ]
            for c_date in colunas_data:
                if c_date in df_chunk.columns:
                    df_chunk[c_date] = pd.to_datetime(df_chunk[c_date], format="%d/%m/%Y %H:%M:%S", errors='coerce')
                    
            # 2. Categorias (Economia de RAM e DuckDB performance)
            cols_cat = ['situacao', 'entidade_complexidade', 'entidade_classificacaoRisco_cor']
            for col in cols_cat:
                if col in df_chunk.columns:
                    df_chunk[col] = df_chunk[col].astype('category')
            
            # 3. Booleans (Garantir que sejam literais bool, não object)
            cols_bool = [c for c in df_chunk.columns if any(kw in c for kw in [
                'SLA_Desfecho_Atingido', 'SLA_Marco_', 'matriciamento', 'teleconsulta',
                'entidade_especialidade_ativa', 'entidade_especialidade_tipoOCI',
                'entidade_semClassificacao', 'entidade_classificacaoRisco_reclassificadaSolicitante',
                'entidade_foraDaRegionalizacao', 'entidade_possuiDita', 'regularizacaoAcesso'
            ])]
            for col in cols_bool:
                df_chunk[col] = df_chunk[col].fillna(False).astype(bool)

            # --- Type Safety Explicito para Métricas SLA ---
            float_cols = ['SLA_Tempo_Solicitante_Dias', 'SLA_Tempo_Regulador_Dias', 'SLA_Lead_Time_Total_Dias']
            for col in float_cols:
                if col in df_chunk.columns:
                    # Converte para float, valores inválidos viram NaN, depois preenche com 0.0
                    df_chunk[col] = pd.to_numeric(df_chunk[col], errors='coerce').fillna(0.0)
                    
            if 'SLA_Interacoes_Regulacao' in df_chunk.columns:
                df_chunk['SLA_Interacoes_Regulacao'] = pd.to_numeric(df_chunk['SLA_Interacoes_Regulacao'], errors='coerce').fillna(0).astype(int)
                
            if 'SLA_Desfecho_Atingido' in df_chunk.columns:
                df_chunk['SLA_Desfecho_Atingido'] = df_chunk['SLA_Desfecho_Atingido'].astype(bool)

            # 4. Strings (O "Pulo do Gato" SRE): Evita inferência de tipo "null"
            # Qualquer coluna que ainda seja "object" ou contenha apenas Nones é forçada para string.
            for col in df_chunk.columns:
                if col not in colunas_data and col not in cols_cat and col not in cols_bool and col not in float_cols and col != 'SLA_Interacoes_Regulacao':
                    # Força cast para string e limpa representações de nulo
                    df_chunk[col] = df_chunk[col].astype(str).replace(['None', 'nan', '<NA>'], '')
                    
            table = pa.Table.from_pandas(df_chunk)
            
            # Inicializa o writer com o schema do primeiro chunk (que agora está tipado)
            if writer is None:
                writer = pq.ParquetWriter(parquet_out, table.schema, compression='snappy')
                
            writer.write_table(table)
            
            processed_total += len(records)
            logger.info(f"⚡ Chunk processado. Total acumulado: {processed_total} registros. Erros: {errors}")
            
            # Liberar memória do chunk
            del records, df_chunk, table
            gc.collect()

        if writer:
            writer.close()
            
        conn.close()
        
        if processed_total == 0:
            logger.error("❌ Nenhum registro válido extraído!")
            return
            
        logger.info(f"🎉 Pipeline SRE concluído! Arquivo gerado: {parquet_out} ({processed_total} registros)")

    except Exception as e:
        logger.error(f"❌ Falha crítica: {e}")

if __name__ == "__main__":
    run_conversion()
    print('done')