# --- SOTA PIPELINE ORCHESTRATOR: Modular Monolith Service (Stateless & Observável) ---
# Now refactored for Kubernetes (K8s) orchestration with Enterprise Telemetry.
import sys
import os
import uuid
import time
from prometheus_client import start_http_server

# -- Injeção das Bibliotecas de Telemetria (SRE Pilar) --
from src.infrastructure.telemetry.logger import setup_structured_logger, correlation_id_var
from src.infrastructure.telemetry.metrics import INGEST_PIPELINE_DURATION, PIPELINE_LAST_SUCCESS_TIMESTAMP
from src.infrastructure.telemetry.tracing import tracer

from master_scraper import main as run_scraper
try:
    from data_processor import GerconPipeline
except ImportError:
    GerconPipeline = None

logger = setup_structured_logger("pipeline_worker")

@tracer.start_as_current_span("orchestrate_daily_pipeline")
def run_orchestrated_job():
    """Executa o ciclo de vida extraindo telemetria e gerando traces transacionais."""
    # Gera um UUID de Correlação para agrupar TODOS os logs (Scraper -> DuckDB)
    run_id = str(uuid.uuid4())
    correlation_id_var.set(run_id)

    os.environ["HEADLESS"] = "True"
    
    start_time = time.time()
    logger.info("--- 🚀 STARTING KUBERNETES DAILY SYNCHRONIZATION PIPELINE ---", extra={"status": "INICIANDO"})

    try:
        # Phase 1: Ingestion (Master Scraper - Incremental/Full)
        with tracer.start_as_current_span("phase_1_ingestion"):
            logger.info("Triggering Master Scraper (Ingestion)...")
            run_scraper() 
            logger.info("Ingestion completed successfully.")

        # Phase 2: Processing (Data Processor - Clean/Anon/Parquet)
        with tracer.start_as_current_span("phase_2_transformation"):
            logger.info("Triggering Data Processor (Transformation)...")
            if GerconPipeline:
                pipeline = GerconPipeline()
                pipeline.run()
                logger.info("Transformation completed successfully.")
            else:
                logger.warning("GerconPipeline not found. Skipping Phase 2.")
        
        # Sucesso: Armazena as Métricas Time-Series
        PIPELINE_LAST_SUCCESS_TIMESTAMP.set_to_current_time()
        logger.info("--- ✅ PIPELINE COMPLETED SUCCESSFULLY ---", extra={"status": "SUCESSO"})

    except Exception as e:
        logger.error(f"❌ CRITICAL PIPELINE FAILURE: {e}", exc_info=True, extra={"status": "FALHA"})
        # Exit com código 1 decreta CronJob Failed
        sys.exit(1)
    finally:
        total_time = time.time() - start_time
        INGEST_PIPELINE_DURATION.observe(total_time)
        logger.info(f"Pipeline desligado após {total_time:.1f} segundos.", extra={"duration_secs": total_time})

if __name__ == "__main__":
    # Exposição declarativa e assíncrona das Golden Signals para o Prometheus K8s
    # Porta 8000 é standard no Scrape Config
    start_http_server(8000)
    logger.info("🖥️ Prometheus Metrics Server exposto na porta 8000.")

    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    run_orchestrated_job()
