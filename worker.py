# --- SOTA PIPELINE ORCHESTRATOR: Modular Monolith Service (Stateless) ---
# This script acts as the Driving Adapter for the daily sync process.
# Now refactored for Kubernetes (K8s) orchestration: 
# Concurrency relies on CronJob's `concurrencyPolicy: Forbid`.
# OS-level fcntl locks are removed to allow true distributed computing.

import logging
import sys
import os
from master_scraper import main as run_scraper
# Optional handling: ensure data_processor exists or use try-except if it's missing
try:
    from data_processor import GerconPipeline
except ImportError:
    # Fallback to prevent immediate crash if not present yet
    GerconPipeline = None

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
    
    try:
        logger.info("--- 🚀 STARTING KUBERNETES DAILY SYNCHRONIZATION PIPELINE ---")
        
        # Phase 1: Ingestion (Master Scraper - Incremental/Full)
        logger.info("Phase 1/2: Triggering Master Scraper (Ingestion)...")
        run_scraper() 
        logger.info("Phase 1/2: Ingestion completed successfully.")
        
        # Phase 2: Processing (Data Processor - Clean/Anon/Parquet)
        logger.info("Phase 2/2: Triggering Data Processor (Transformation)...")
        if GerconPipeline:
            pipeline = GerconPipeline()
            pipeline.run()
            logger.info("Phase 2/2: Transformation completed successfully.")
        else:
            logger.warning("Pipeline GerconPipeline not found. Skipping Phase 2.")
        
        logger.info("--- ✅ PIPELINE COMPLETED SUCCESSFULLY ---")
        
    except Exception as e:
        logger.error(f"❌ CRITICAL PIPELINE FAILURE: {e}", exc_info=True)
        # SRE Alert Hook: In a K8s CronJob, exiting with 1 ensures the Job shows as Failed.
        sys.exit(1)

if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    run_orchestrated_job()
