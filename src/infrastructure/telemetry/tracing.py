import logging

logger = logging.getLogger(__name__)

# --- SRE FIX: DEGRADAÇÃO GRACIOSA PARA TELEMETRIA ---
class DummySpan:
    """Span falso para evitar quebra de código se o OpenTelemetry falhar."""
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass
    def set_attribute(self, *args, **kwargs): pass
    def record_exception(self, *args, **kwargs): pass
    def set_status(self, *args, **kwargs): pass

class DummyTracer:
    """Tracer falso para devolver Spans nulos e manter o sistema rodando."""
    def start_as_current_span(self, name, *args, **kwargs):
        return DummySpan()

try:
    from opentelemetry import trace
    # Inicializa o Tracer real
    tracer = trace.get_tracer(__name__)
except ImportError:
    logger.warning("⚠️ OpenTelemetry não detectado. Usando DummyTracer (Degradação Graciosa).")
    tracer = DummyTracer()
