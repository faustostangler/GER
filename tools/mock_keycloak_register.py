import asyncio
import json
import logging
import uuid
import sys
from typing import Optional

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock_keycloak_register")

# Configuration matching the consumer
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KEYCLOAK_EVENTS_TOPIC = "keycloak.events.register"


async def main(user_id: Optional[str] = None, crm_numero: str = "12345", crm_uf: str = "SP", is_register: bool = True):
    """
    Simulate a Keycloak SPI event injection to the Kafka topic.
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    
    if user_id is None:
        user_id = str(uuid.uuid4())
        
    event_id = str(uuid.uuid4())
    
    event_payload = {
        "id": event_id,
        "type": "REGISTER" if is_register else "OTHER",
        "userId": user_id,
        "details": {
            "crm_numero": crm_numero,
            "crm_uf": crm_uf
        }
    }
    
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Injecting mock Keycloak event to topic '{KEYCLOAK_EVENTS_TOPIC}':\n{json.dumps(event_payload, indent=2)}")
    
    try:
        await producer.send_and_wait(
            KEYCLOAK_EVENTS_TOPIC, 
            json.dumps(event_payload).encode("utf-8")
        )
        logger.info("Event successfully sent.")
    except Exception as e:
        logger.error(f"Failed to send event: {e}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    # Allow passing arguments or use defaults
    # Usage: python mock_keycloak_register.py [user_id] [crm_numero] [crm_uf]
    user_id_arg = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
    crm_num_arg = sys.argv[2] if len(sys.argv) > 2 else "12345"
    crm_uf_arg = sys.argv[3] if len(sys.argv) > 3 else "SP"
    
    asyncio.run(main(user_id=user_id_arg, crm_numero=crm_num_arg, crm_uf=crm_uf_arg))
