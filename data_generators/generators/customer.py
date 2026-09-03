"""Customer registration + profile lifecycle. verified_account/
disabled_account are tracked as two independent booleans, not folded into a
Lifecycle status enum, since they vary independently (a verified account can
later be disabled) - see redesign plan §3b.

Registration is ongoing (creation_rate_per_minute in config.yaml), not a
fixed upfront pool like products - see config.yaml's own header comment."""
from __future__ import annotations

import logging
import random
import uuid

from kafka_producer import send_event

from .helpers import now_iso

logger = logging.getLogger(__name__)

TOPIC = "iot.customer_events"

_FIRST_NAMES = [
    "Anna", "Marco", "Lukas", "Sophie", "Julia", "Luca", "Emma", "Noah", "Mia", "Leon",
    "Laura", "Matteo", "Elena", "Fabian", "Nina", "Simon", "Chiara", "David", "Livia", "Yann",
]
_LAST_NAMES = [
    "Muller", "Rossi", "Meier", "Dubois", "Keller", "Bianchi", "Weber", "Fischer",
    "Schneider", "Moreau", "Zimmermann", "Ferrari", "Baumann", "Roux", "Steiner", "Conti",
]
_SEGMENTS = [
    "retail", "small_business", "vip", "wholesale", "student", "senior",
    "doctor", "dentist", "teacher", "engineer", "lawyer", "nurse",
    "architect", "accountant", "electrician", "pharmacist",
]
_ADDRESSES = [
    "Bahnhofstrasse", "Seestrasse", "Marktgasse", "Loewenstrasse", "Kramgasse",
    "Spitalgasse", "Poststrasse", "Museumstrasse", "Gartenstrasse", "Bergstrasse",
    "Dorfstrasse", "Kirchgasse", "Schulstrasse", "Industriestrasse", "Freie Strasse",
    "Rue du Rhone", "Rue de Lausanne", "Rue du Marche", "Via Nassa", "Piazza Grande",
]


class Customer:
    def __init__(
        self,
        customer_id: str,
        name: str,
        email: str,
        address: str,
        tel: str,
        country: str,
        city: str,
        segment: str,
        verified_account: bool = False,
        disabled_account: bool = False,
        created_at: str | None = None,
    ):
        self.id = customer_id
        self.customer_id = customer_id
        self.name = name
        self.email = email
        self.address = address
        self.tel = tel
        self.country = country
        self.city = city
        self.segment = segment
        self.verified_account = verified_account
        self.disabled_account = disabled_account
        self.created_at = created_at or now_iso()
        self.updated_at = self.created_at
        # In-memory only, rebuilt on resume from silver.purchase_orders_current
        # - not itself persisted, just bookkeeping for the per-customer cap.
        self.open_order_ids: set[str] = set()

    def _emit(self, producer, event_type: str) -> None:
        event = {
            "event_id": str(uuid.uuid4()),
            "customer_id": self.customer_id,
            "event_type": event_type,
            "name": self.name,
            "email": self.email,
            "address": self.address,
            "tel": self.tel,
            "country": self.country,
            "city": self.city,
            "segment": self.segment,
            "verified_account": self.verified_account,
            "disabled_account": self.disabled_account,
            "event_at": now_iso(),
        }
        send_event(producer, TOPIC, key=self.customer_id, value=event)

    @classmethod
    def create(
        cls, producer, cfg: dict, scheduler, zones: list[dict], existing_emails: set[str]
    ) -> "Customer":
        zone = random.choice(zones)
        first = random.choice(_FIRST_NAMES)
        last = random.choice(_LAST_NAMES)
        # email is the business key (unique) - derived from name, not random,
        # so it reads like a real address; regenerate the numeric suffix on
        # any collision rather than allowing a duplicate to be created.
        while True:
            email = f"{first.lower()}.{last.lower()}.{random.randint(100, 999)}@example.com"
            if email not in existing_emails:
                break

        customer = cls(
            customer_id=str(uuid.uuid4()),
            name=f"{first} {last}",
            email=email,
            address=f"{random.randint(1, 200)} {random.choice(_ADDRESSES)}, {zone['city']}",
            tel=f"+41{random.randint(700000000, 799999999)}",
            country=zone["country"],
            city=zone["city"],
            segment=random.choice(_SEGMENTS),
        )
        customer._emit(producer, "created")
        logger.debug("Customer created %s (%s) city=%s", email, customer.customer_id, zone["city"])

        delay_cfg = cfg["customers"]["verification_delay_seconds"]
        delay = random.uniform(delay_cfg["min"], delay_cfg["max"])
        scheduler.schedule(delay, lambda: customer.verify_account(producer))
        logger.debug("Customer %s (%s) verification scheduled in %.1fs", email, customer.customer_id, delay)
        return customer

    def verify_account(self, producer) -> None:
        self.verified_account = True
        self.updated_at = now_iso()
        self._emit(producer, "account_verified")
        logger.debug("Customer %s (%s) account_verified", self.email, self.customer_id)

    def maybe_update(self, producer, cfg: dict, zones: list[dict], elapsed_seconds: float) -> None:
        customers_cfg = cfg["customers"]

        probability = (customers_cfg["attribute_update_rate_per_hour"] / 3600.0) * elapsed_seconds
        if random.random() < probability:
            zone = random.choice(zones)
            self.city = zone["city"]
            self.segment = random.choice(_SEGMENTS)
            self.updated_at = now_iso()
            self._emit(producer, "updated")
            logger.debug(
                "Customer %s (%s) updated: city=%s segment=%s",
                self.email, self.customer_id, self.city, self.segment,
            )

        if not self.disabled_account:
            disable_probability = (customers_cfg["disable_rate_per_hour"] / 3600.0) * elapsed_seconds
            if random.random() < disable_probability:
                self.disabled_account = True
                self.updated_at = now_iso()
                self._emit(producer, "account_disabled")
                logger.debug("Customer %s (%s) account_disabled", self.email, self.customer_id)
        else:
            enable_probability = (customers_cfg["re_enable_rate_per_hour"] / 3600.0) * elapsed_seconds
            if random.random() < enable_probability:
                self.disabled_account = False
                self.updated_at = now_iso()
                self._emit(producer, "account_enabled")
                logger.debug("Customer %s (%s) account_enabled", self.email, self.customer_id)

    def can_create_purchase_order(self, cfg: dict) -> bool:
        max_open = cfg["orders"]["max_concurrent_orders_per_customer"]
        return (
            self.verified_account
            and not self.disabled_account
            and len(self.open_order_ids) < max_open
        )
