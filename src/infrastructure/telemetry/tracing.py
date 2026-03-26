from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased, ParentBased

def init_tracer() -> trace.Tracer:
    """Configura o OpenTelemetry provider global para a aplicação."""
    
    # === ESTRATÉGIA DE SAMPLING SRE ===
    # Arquiteturalmente, transferimos o Tail-Based Sampling (Buffer em memória) 
    # pro OpenTelemetry Collector Sidecar (Go). 
    # Aqui fazemos Head-Based Sampling:
    # Capturamos 100% dos spans localmente no Worker p/ enviar. O backend Collector lida com a retenção fina 
    # baseada na % de Sucesso/Falha sem arriscar OOMKills no Python.
    sampler = ParentBased(root=TraceIdRatioBased(1.0))
    provider = TracerProvider(sampler=sampler)
    
    # Exemplo: Enviar para OTEL_EXPORTER_OTLP_ENDPOINT nativo
    # processor = SimpleSpanProcessor(ConsoleSpanExporter())
    # provider.add_span_processor(processor)
    
    trace.set_tracer_provider(provider)
    return trace.get_tracer("gercon.pipeline")

tracer = init_tracer()
