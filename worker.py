# --- SOTA PIPELINE ORCHESTRATOR: Modular Monolith Service (Stateless & Observável) ---
# Now refactored for Kubernetes (K8s) orchestration with Enterprise Telemetry.
import sys
import os
import uuid
import time
from prometheus_client import start_http_server, CollectorRegistry, multiprocess

from src.infrastructure.telemetry.logger import setup_structured_logger, correlation_id_var
from src.infrastructure.telemetry.metrics import INGEST_PIPELINE_DURATION, PIPELINE_LAST_SUCCESS_TIMESTAMP
from src.infrastructure.telemetry.tracing import tracer

from master_scraper import main as run_scraper
try:
    from data_processor import GerconPipeline
except ImportError:
    GerconPipeline = None

logger = setup_structured_logger("pipeline_worker")

def init_prometheus():
    """Inicia o servidor de telemetria lidando nativamente com Multi-process Workers"""
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if multiproc_dir:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        start_http_server(8000, registry=registry)
        logger.info(f"🖥️ Prometheus Metrics Server (MultiProc Mode) exposto na porta 8000. Dir: {multiproc_dir}")
    else:
        start_http_server(8000)
        logger.info("🖥️ Prometheus Metrics Server (SingleProc) exposto na porta 8000.")

@tracer.start_as_current_span("orchestrate_daily_pipeline")
def run_orchestrated_job():
    run_id = str(uuid.uuid4())
    correlation_id_var.set(run_id)

    os.environ["HEADLESS"] = "True"
    
    start_time = time.time()
    logger.info("--- 🚀 STARTING KUBERNETES DAILY SYNCHRONIZATION PIPELINE ---", extra={"status": "INICIANDO"})

    try:
        with tracer.start_as_current_span("phase_1_ingestion"):
            run_scraper() 

        with tracer.start_as_current_span("phase_2_transformation"):
            if GerconPipeline:
                pipeline = GerconPipeline()
                pipeline.run()
        
        PIPELINE_LAST_SUCCESS_TIMESTAMP.set_to_current_time()
        logger.info("--- ✅ PIPELINE COMPLETED SUCCESSFULLY ---", extra={"status": "SUCESSO"})

    except Exception as e:
        logger.error(f"❌ CRITICAL PIPELINE FAILURE: {e}", exc_info=True, extra={"status": "FALHA"})
        sys.exit(1)
    finally:
        total_time = time.time() - start_time
        INGEST_PIPELINE_DURATION.observe(total_time)

if __name__ == "__main__":
    init_prometheus()
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    run_orchestrated_job()
