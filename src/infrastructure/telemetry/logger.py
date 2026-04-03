import logging
import sys
from contextvars import ContextVar
from pythonjsonlogger import jsonlogger

# Variável de Contexto Global para Tracking Correlacionado
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="SYSTEM")


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        # Injeta o ID da corrida exata de forma automática e silenciosa via ContextVar
        log_record["run_id"] = correlation_id_var.get()
        log_record["level"] = record.levelname
        log_record["module"] = record.module


def setup_structured_logger(name: str) -> logging.Logger:
    """Configura um logger SOTA (12-Factor App) direcionado EXCLUSIVAMENTE para stdout em JSON."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Previne duplicidade de handlers em hot-reloads
    if logger.handlers:
        logger.handlers.clear()

    # Saída nativa para contêineres: stdout
    handler = logging.StreamHandler(sys.stdout)
    formatter = CustomJsonFormatter(
        "%(timestamp)s %(level)s %(name)s %(message)s", timestamp=True
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Opcional: Garante que logs do nível superior (root) não usem formatação paralela e "sujem" o JSON
    logger.propagate = False

    return logger
