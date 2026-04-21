import asyncio
import json
import uuid
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

# Add the 'src' path resolution for tests
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from infrastructure.events.keycloak_kafka_consumer import consume_keycloak_events, KEYCLOAK_EVENTS_DLQ

@pytest.mark.asyncio
async def test_consumer_forwards_to_dlq_after_max_retries():
    """
    Integration Test: Consumer Resilience & DLQ
    """
    user_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    
    event_payload = {
        "id": event_id,
        "type": "REGISTER",
        "userId": user_id,
        "details": {
            "crm_numero": "00000", # Poison pill
            "crm_uf": "SP"
        }
    }
    
    mock_msg = MagicMock()
    mock_msg.value = json.dumps(event_payload).encode("utf-8")
    
    class MockConsumer:
        def __init__(self, *args, **kwargs): self.yielded = False
        async def start(self): pass
        async def stop(self): pass
        async def commit(self): pass
        def __aiter__(self): return self
        async def __anext__(self):
            if not self.yielded:
                self.yielded = True
                return mock_msg
            raise StopAsyncIteration

    with patch("infrastructure.events.keycloak_kafka_consumer.AIOKafkaConsumer", new=MockConsumer), \
         patch("infrastructure.events.keycloak_kafka_consumer.AIOKafkaProducer") as MockProducer, \
         patch("infrastructure.events.keycloak_kafka_consumer.validate_cfm_api") as mock_validate, \
         patch("infrastructure.events.keycloak_kafka_consumer.is_already_processed") as mock_idempotency, \
         patch("infrastructure.events.keycloak_kafka_consumer.asyncio.sleep", return_value=None):
        
        mock_idempotency.return_value = False
        mock_validate.side_effect = ConnectionError("CFM API Down")
        
        mock_producer_instance = AsyncMock()
        MockProducer.return_value = mock_producer_instance
        
        await consume_keycloak_events()
        
        assert mock_validate.call_count == 3
        assert mock_producer_instance.send_and_wait.called

@pytest.mark.asyncio
async def test_consumer_skips_non_register_events():
    """
    Test that the consumer ignores events that are not of type 'REGISTER'.
    """
    event_payload = {
        "id": str(uuid.uuid4()),
        "type": "LOGIN",
        "userId": "some_user"
    }
    
    mock_msg = MagicMock()
    mock_msg.value = json.dumps(event_payload).encode("utf-8")
    
    class MockConsumer:
        def __init__(self, *args, **kwargs): self.yielded = False
        async def start(self): pass
        async def stop(self): pass
        async def commit(self): pass
        def __aiter__(self): return self
        async def __anext__(self):
            if not self.yielded:
                self.yielded = True
                return mock_msg
            raise StopAsyncIteration

    with patch("infrastructure.events.keycloak_kafka_consumer.AIOKafkaConsumer", new=MockConsumer), \
         patch("infrastructure.events.keycloak_kafka_consumer.AIOKafkaProducer") as MockProducer, \
         patch("infrastructure.events.keycloak_kafka_consumer.is_already_processed") as mock_idempotency, \
         patch("infrastructure.events.keycloak_kafka_consumer._process_register_event") as mock_process:
        
        mock_idempotency.return_value = False
        mock_producer_instance = AsyncMock()
        MockProducer.return_value = mock_producer_instance
        
        await consume_keycloak_events()
        
        assert mock_process.call_count == 0
