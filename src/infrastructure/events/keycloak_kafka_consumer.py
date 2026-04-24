"""Kafka consumer for Keycloak USER_REGISTERED events.

WHY (DDD / Event-Driven Authorization): Keycloak handles password authentication
(Identity). This consumer handles CRM verification (Domain Authorization), triggered
by the USER_REGISTERED event emitted by the Keycloak SPI Event Listener.

Pipeline:
  Keycloak SPI → Redpanda topic ``keycloak.events.register``
    → consume_keycloak_events()
      → validate_cfm_api() (CFM mock / real integration)
        → save DoctorProfile(crm_verified=True) to domain store
          → jwt_validator can now authorize the user on next API call

SRE Resilience:
  - Idempotency: processed_events set prevents double-processing on rebalance.
  - DLQ: unrecoverable events (poison pills, CFM timeouts) are forwarded to
    ``keycloak.events.dlq`` with the original payload + error for post-mortem.
  - Manual commit: offset is committed AFTER DoctorProfile persistence OR DLQ
    forwarding, never silently skipped, to prevent partition stalls.
  - Exponential backoff: up to 3 retries with 2^n second delays before DLQ.

Ref: docs/adr/ADR-006-iam-zero-trust-crm-authorization.md
"""
import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import redis.asyncio as redis

from infrastructure.config import settings
from domain.identity import DoctorProfile, MedicalCouncilRegistration
from infrastructure.repositories.doctor_profile_repository import SQLDoctorProfileRepository
from infrastructure.adapters.cfm_client import CFMClient
from application.use_cases.interfaces import ICFMClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("keycloak_events_consumer")

KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_URL
KEYCLOAK_EVENTS_TOPIC = "keycloak.events.register"
KEYCLOAK_EVENTS_DLQ = "keycloak.events.dlq"

# Global Redis client for async operations
_redis_client = redis.Redis(
    host=settings.redis.host,
    port=settings.redis.port,
    db=0,
    decode_responses=True,
    socket_connect_timeout=2,
)


async def is_already_processed(event_id: str) -> bool:
    """Check if the event was already processed using Redis as a distributed store.

    WHY (ADR-006): Replacing the in-memory set with Redis ensures cross-replica safety.
    Uses 'SET NX' to check and lock atomically with a 7-day TTL.
    """
    if not event_id:
        return False
    # nx=True returns True if set, None if already exists
    result = await _redis_client.set(
        f"processed_event:{event_id}", "1", nx=True, ex=604800
    )
    return result is None

# WHY (long-lived producer): Creating a new AIOKafkaProducer per DLQ message
# would exhaust TCP sockets under load (socket exhaustion). One shared producer
# per consumer lifecycle is the correct pattern.
dlq_producer: Optional[AIOKafkaProducer] = None


async def validate_cfm_api(crm_numero: str, crm_uf: str, cfm_client: ICFMClient) -> bool:
    """Validate CRM registration against the CFM (Conselho Federal de Medicina) API.

    WHY (ACL boundary): This function is the Anti-Corruption Layer between our domain
    and the external CFM registry. All API-specific error handling and retries are
    encapsulated here; the consumer only receives a bool or an exception.

    Args:
        crm_numero: CRM registration number (digits only).
        crm_uf:     Federation unit (2-letter uppercase string).
        cfm_client: The production CFM client adapter.

    Returns:
        bool: True if CFM confirms the registration is active and valid.

    Raises:
        ConnectionError: On network timeout or CFM API unavailability.
        ValueError:      If CFM returns an invalid/malformed response.
    """
    return await cfm_client.validate(crm_numero, crm_uf)


async def _persist_doctor_profile(
    profile: DoctorProfile, repo: SQLDoctorProfileRepository
) -> None:
    """Persist a verified DoctorProfile to the domain store via PostgreSQL/Redis.

    WHY (SOTA Persistence): delegates to the SQLDoctorProfileRepository which implements
    the write-through cache pattern (SQL commit + Redis update). Wrapping in
    asyncio.to_thread ensures the background worker remains responsive during I/O.

    Args:
        profile: Fully constructed DoctorProfile with crm_verified=True.
    """
    try:
        await asyncio.to_thread(repo.save, profile)
        logger.info(
            "DoctorProfile successfully persisted for user_id=%s (CRM %s/%s).",
            profile.user_id,
            profile.crm.crm_numero,
            profile.crm.crm_uf,
        )
    except Exception as e:
        logger.error("Critical failure during DoctorProfile persistence: %s", e)
        raise


async def send_to_dlq(payload: dict, error_msg: str) -> None:
    """Forward an unprocessable event to the Dead Letter Queue for post-mortem analysis.

    Args:
        payload:   Original event payload that could not be processed.
        error_msg: Human-readable description of the failure cause.
    """
    global dlq_producer
    dead_letter = {"original_payload": payload, "error": error_msg}
    await dlq_producer.send_and_wait(
        KEYCLOAK_EVENTS_DLQ, json.dumps(dead_letter).encode("utf-8")
    )
    logger.error("Event forwarded to DLQ. Cause: %s", error_msg)


async def _process_register_event(
    event_data: dict, repo: SQLDoctorProfileRepository, cfm_client: ICFMClient
) -> None:
    """Process a single USER_REGISTERED event from the Keycloak SPI.

    Extracts CRM fields from the event details, runs CFM validation,
    and persists a DoctorProfile(crm_verified=True) if validation succeeds.

    WHY (extracted function): Separating event parsing from consumer lifecycle
    (start/stop, commit) makes this logic independently testable without a
    running Kafka broker — a Chaos Engineering priority.

    Args:
        event_data: Decoded JSON payload from the Keycloak SPI event message.

    Raises:
        ValueError:      If CRM fields are absent or malformed.
        ConnectionError: If CFM API is unreachable after max_retries.
    """
    user_id: str = event_data.get("userId", "")
    details: dict = event_data.get("details", {})
    crm_numero: Optional[str] = details.get("crm_numero")
    crm_uf: Optional[str] = details.get("crm_uf")

    if not crm_numero or not crm_uf:
        raise ValueError(
            f"Missing CRM fields in USER_REGISTERED event for user_id={user_id}. "
            "Event sent to DLQ for manual review."
        )

    # Construct and validate the VO early — fail fast before hitting the network.
    # WHY: MedicalCouncilRegistration validators (digits-only, 2-letter UF) run
    # at construction time. A malformed CRM from Keycloak is caught here, not
    # silently stored as garbage in the domain store.
    crm_registration = MedicalCouncilRegistration(
        crm_numero=crm_numero, crm_uf=crm_uf
    )

    logger.info(
        "Starting async CFM validation for user_id=%s CRM=%s/%s",
        user_id,
        crm_numero,
        crm_uf,
    )

    is_valid = await validate_cfm_api(
        crm_registration.crm_numero, crm_registration.crm_uf, cfm_client
    )

    if is_valid:
        # Domain authorization: only the consumer pipeline sets crm_verified=True.
        profile = DoctorProfile(
            user_id=user_id,
            crm=crm_registration,
            crm_verified=True,
        )
        await _persist_doctor_profile(profile, repo)
        logger.info(
            "CRM %s/%s verified and DoctorProfile persisted for user_id=%s.",
            crm_numero,
            crm_uf,
            user_id,
        )


async def consume_keycloak_events() -> None:
    """Async entry point for the Keycloak USER_REGISTERED event consumer.

    Lifecycle:
      1. Start DLQ producer (long-lived — one connection per consumer process).
      2. Subscribe to ``keycloak.events.register`` topic.
      3. For each message: idempotency check → type filter → process → commit.
      4. On unrecoverable failure: forward to DLQ → commit (no partition stall).
    """
    global dlq_producer

    # Long-lived DLQ producer — started once, closed in the finally block.
    dlq_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await dlq_producer.start()

    consumer = AIOKafkaConsumer(
        KEYCLOAK_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="gercon_identity_group",
        # WHY (manual commit): Offsets are committed AFTER DoctorProfile persistence
        # or DLQ forwarding. Auto-commit risks losing events if the process crashes
        # between poll() and processing — the "at-least-once" SRE guarantee.
        enable_auto_commit=False,
    )

    await consumer.start()
    logger.info("Kafka Consumer started — awaiting Keycloak SPI events.")

    # Initialize repository and client inside the loop to ensure fresh DB session/engine
    repo = SQLDoctorProfileRepository()
    cfm_client = CFMClient()

    try:
        async for msg in consumer:
            event_data: dict = json.loads(msg.value.decode("utf-8"))
            event_id: Optional[str] = event_data.get("id")

            # ── Idempotency guard ─────────────────────────────────────────────
            if await is_already_processed(event_id):
                logger.info(
                    "Event %s already processed — skipping (idempotency).", event_id
                )
                await consumer.commit()
                continue

            event_type: str = event_data.get("type", "")
            if event_type != "REGISTER":
                # Only REGISTER events are relevant to this Bounded Context.
                await consumer.commit()
                continue

            # ── Processing with retry + DLQ ───────────────────────────────────
            max_retries = 3
            success = False

            for attempt in range(max_retries):
                try:
                    await _process_register_event(event_data, repo, cfm_client)
                    success = True
                    break
                except Exception as exc:
                    logger.warning(
                        "Attempt %d/%d failed for event %s: %s",
                        attempt + 1,
                        max_retries,
                        event_id,
                        exc,
                    )
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)  # Exponential backoff

            if not success:
                await send_to_dlq(
                    event_data,
                    f"Processing failed after {max_retries} retries — sent to DLQ.",
                )

            # ── Commit offset (always, even on DLQ path) ─────────────────────
            # WHY: Committing after DLQ ensures the partition advances. Without this,
            # a single bad message would halt the entire consumer group forever.
            # NOTE: Redis entry was already created in the idempotency guard (SET NX).
            await consumer.commit()

    finally:
        await consumer.stop()
        if dlq_producer:
            await dlq_producer.stop()


if __name__ == "__main__":
    asyncio.run(consume_keycloak_events())
