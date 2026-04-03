import logging
from prometheus_client import Histogram, Counter, start_http_server, REGISTRY

logger = logging.getLogger(__name__)

def get_or_create_histogram(name, documentation, labelnames=()):
    try:
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Histogram(name, documentation, labelnames)
    except Exception:
        pass

def get_or_create_counter(name, documentation, labelnames=()):
    try:
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Counter(name, documentation, labelnames)
    except Exception:
        pass

RENDER_LATENCY = get_or_create_histogram(
    "streamlit_render_latency_seconds", 
    "Render time for UI components", 
    ["component"]
)

SILENT_ERRORS = get_or_create_counter(
    "streamlit_silent_errors_total", 
    "Silent errors caught by UI", 
    ["component"]
)

# Start global HTTP server for metrics
_started = False
def init_telemetry(port=8001):
    global _started
    if not _started:
        try:
            start_http_server(port)
            _started = True
            logger.info(f"Prometheus metrics exposed on port {port}")
        except Exception as e:
            logger.warning(f"Could not start Prometheus server on port {port}: {e}")
