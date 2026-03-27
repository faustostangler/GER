import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("keycloak_events_consumer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KEYCLOAK_EVENTS_TOPIC = "keycloak.events.register"
KEYCLOAK_EVENTS_DLQ = "keycloak.events.dlq"

# Mocked Idempotency Store (e.g. DuckDB/SQLite or Redis)
processed_events = set()

# Global DLQ Producer (Long-lived connection to avoid Socket Exhaustion)
dlq_producer: AIOKafkaProducer = None

def validate_cfm_api(crm_numero: str, crm_uf: str) -> bool:
    """
    Simula uma chamada de rede para validação na API do CFM.
    Se falhar (timeout ou erro 500), lançamos Exception para testar o DLQ.
    """
    # Exemplo simulado - lança exceção em caso de indisponibilidade
    if crm_numero == "00000": # Simulando um Poison Pill / Falha de Rede
        raise ConnectionError("Timeout na API do CFM")
    return True

async def send_to_dlq(payload: dict, error_msg: str):
    global dlq_producer
    dead_letter = {
        "original_payload": payload,
        "error": error_msg
    }
    await dlq_producer.send_and_wait(KEYCLOAK_EVENTS_DLQ, json.dumps(dead_letter).encode('utf-8'))
    logger.error(f"Mensagem enviada para DLQ. Erro: {error_msg}")

async def consume_keycloak_events():
    global dlq_producer
    
    # 1. Inicie o Producer da DLQ uma ÚNICA vez no ciclo de vida
    dlq_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await dlq_producer.start()
    
    consumer = AIOKafkaConsumer(
        KEYCLOAK_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="gercon_identity_group",
        enable_auto_commit=False  # Controle manual para DLQ e Idempotência
    )
    
    await consumer.start()
    logger.info("Kafka Consumer iniciado, aguardando eventos do Keycloak SPI...")
    
    try:
        async for msg in consumer:
            event_data = json.loads(msg.value.decode('utf-8'))
            event_id = event_data.get("id")
            
            # 1. Idempotency Check
            if event_id in processed_events:
                logger.info(f"Evento {event_id} já processado. Ignorando.")
                await consumer.commit()
                continue
                
            event_type = event_data.get("type")
            if event_type != "REGISTER":
                # Apenas nos importamos com registros para este Bounded Context
                await consumer.commit()
                continue
                
            user_id = event_data.get("userId")
            details = event_data.get("details", {})
            crm_numero = details.get("crm_numero")
            crm_uf = details.get("crm_uf")
            
            logger.info(f"Iniciando validação de CFM via evento assíncrono para o usuário {user_id}")
            
            # 2. Processamento com Resiliência e DLQ
            max_retries = 3
            success = False
            for attempt in range(max_retries):
                try:
                    if crm_numero and crm_uf:
                        is_valid = validate_cfm_api(crm_numero, crm_uf)
                        if is_valid:
                            logger.info(f"CRM {crm_numero}/{crm_uf} validado com sucesso para {user_id}. Criando DoctorProfile.")
                            # TODO: Criar DoctorProfile no Banco Principal
                    success = True
                    break
                except Exception as e:
                    logger.warning(f"Tentativa {attempt + 1} falhou para o evento {event_id}: {e}")
                    await asyncio.sleep(2 ** attempt) # Exponential backoff
            
            if not success:
                # 3. Dead Letter Queue Pattern
                await send_to_dlq(event_data, "Falha de processamento final após retries.")
            
            # 4. Commit Offset (Mesmo se for para DLQ, a partição não deve travar)
            processed_events.add(event_id)
            await consumer.commit()
            
    finally:
        await consumer.stop()
        if dlq_producer:
            await dlq_producer.stop()

if __name__ == "__main__":
    asyncio.run(consume_keycloak_events())
