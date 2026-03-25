# --- SOTA PIPELINE ORCHESTRATOR: Modular Monolith Service ---
# This script acts as the Driving Adapter for the daily sync process.
# It ensures Atomicity: Scraper SUCCESS -> Processor SUCCESS.

import logging
import sys
import os
import fcntl
from master_scraper import main as run_scraper
from data_processor import GerconPipeline

# --- SRE OBSERVABILITY CONFIGURATION ---
# Using structured logging to facilitate integration with Grafana Loki later.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline_worker.log", encoding='utf-8')
    ]
)
logger = logging.getLogger("PipelineWorker")

def run_orchestrated_job():
    """
    Executes the full data lifecycle: Ingestion -> Transformation -> Egress.
    Follows fail-fast principles for reliable orchestration.
    """
    # Force Headless for server/container environments
    os.environ["HEADLESS"] = "True"
    
    # --- CONCURRENCY LOCK (SRE Guard) ---
    # Prevents multiple workers from running simultaneously and causing resource exhaustion.
    lock_file_path = "/tmp/pipeline_worker.lock"
    lock_file = open(lock_file_path, "w")
    
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logger.warning("⚠️ Another instance of PipelineWorker is already running. Aborting current execution.")
        sys.exit(0) # Exit gracefully without error to avoid Ofelia spam

    try:
        logger.info("--- 🚀 STARTING DAILY SYNCHRONIZATION PIPELINE ---")
        
        # Phase 1: Ingestion (Master Scraper - Incremental/Full)
        # The scraper manages its own state (SQLite Watermarks) for resilience.
        logger.info("Phase 1/2: Triggering Master Scraper (Ingestion)...")
        run_scraper() 
        logger.info("Phase 1/2: Ingestion completed successfully.")
        
        # Phase 2: Processing (Data Processor - Clean/Anon/Parquet)
        # Transforms CSVs into high-performance Parquet for the Analytics BFF.
        logger.info("Phase 2/2: Triggering Data Processor (Transformation)...")
        pipeline = GerconPipeline()
        pipeline.run()
        logger.info("Phase 2/2: Transformation completed successfully.")
        
        logger.info("--- ✅ PIPELINE COMPLETED SUCCESSFULLY ---")
        
    except Exception as e:
        logger.error(f"❌ CRITICAL PIPELINE FAILURE: {e}", exc_info=True)
        # In a production SRE environment, this would trigger a PagerDuty/Webhook alert.
        # We exit with code 1 so the orchestrator (Ofelia/K8s) records the failure.
        sys.exit(1)
    finally:
        # Release the lock and cleanup
        try:
            fcntl.lockf(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
        except Exception:
            pass

if __name__ == "__main__":
    # Ensure current directory is in path for imports if needed
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    run_orchestrated_job()
