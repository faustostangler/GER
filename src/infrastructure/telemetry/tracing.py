from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

def init_tracer() -> trace.Tracer:
    """Configura o OpenTelemetry provider global para a aplicação."""
    provider = TracerProvider()
    
    # Em produção, aqui seria configurado o OTLPExporter mandando para Jaeger, Honeycomb ou Grafana Tempo.
    # Exemplo temporário para saída em Console (desativar em Prod se muito verboso)
    # processor = SimpleSpanProcessor(ConsoleSpanExporter())
    # provider.add_span_processor(processor)
    
    trace.set_tracer_provider(provider)
    return trace.get_tracer("gercon.pipeline")

tracer = init_tracer()
