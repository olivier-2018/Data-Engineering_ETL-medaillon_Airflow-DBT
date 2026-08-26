"""Invoice lifecycle - settles a purchase order's payment, including the
reminder mechanism. Folds in what a separate payment domain used to cover
(redesign plan decision #6): the invoice lifecycle is already a strict
superset of authorize/capture/fail.

Status lifecycle: created -> pending -> settled, or cancelled (after
max_reminders unpaid reminders). The self-loop on "pending" models a
reminder: payment_reminder/due_at change but status itself doesn't - see
redesign plan §3b."""
from __future__ import annotations

import logging
import random
import uuid
from datetime import timedelta, timezone

from .helpers import now_iso, utc_now
from .lifecycle import Lifecycle

logger = logging.getLogger(__name__)

TOPIC = "iot.invoice_events"

_TRANSITIONS = {
    None: {"created"},
    "created": {"pending"},
    "pending": {"pending", "settled", "cancelled"},
    "settled": set(),
    "cancelled": set(),
}


class Invoice(Lifecycle):
    def __init__(self, invoice_id: str, purchase_order, amount: float, products: dict):
        super().__init__(_TRANSITIONS)
        self.id = invoice_id
        self.invoice_id = invoice_id
        self.purchase_order_id = purchase_order.purchase_order_id
        self.customer_id = purchase_order.customer_id
        self.amount = amount
        self.payment_reminder = 0
        self.due_at: str | None = None
        self.created_at = now_iso()
        self._purchase_order = purchase_order
        self._products = products

    def _log_label(self) -> str:
        email = getattr(self._purchase_order, "customer_email", None)
        if email:
            return f"{self.id} (PO={self.purchase_order_id}, {email})"
        return f"{self.id} (PO={self.purchase_order_id})"

    def _event(self) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "invoice_id": self.invoice_id,
            "purchase_order_id": self.purchase_order_id,
            "customer_id": self.customer_id,
            "amount": self.amount,
            "status": self.status,
            "payment_reminder": self.payment_reminder,
            "due_at": self.due_at,
            "event_at": now_iso(),
        }

    @classmethod
    def create(cls, producer, cfg: dict, scheduler, purchase_order, amount: float, products: dict) -> "Invoice":
        invoice = cls(str(uuid.uuid4()), purchase_order, amount, products)
        invoice.set_status("created", producer, TOPIC, lambda i: i._event())
        invoice._start_due_timer(producer, cfg, scheduler)
        logger.debug("Invoice %s created: amount=%.2f due_at=%s", invoice._log_label(), amount, invoice.due_at)
        return invoice

    def _start_due_timer(self, producer, cfg: dict, scheduler) -> None:
        due_minutes = cfg["invoices"]["due_minutes"]
        self.due_at = (utc_now() + timedelta(minutes=due_minutes)).isoformat()
        self.set_status("pending", producer, TOPIC, lambda i: i._event())
        scheduler.schedule(due_minutes * 60, lambda: self._on_due(producer, cfg, scheduler))

    def _on_due(self, producer, cfg: dict, scheduler) -> None:
        if self.status != "pending":
            return  # already settled/cancelled by other means
        if random.random() < cfg["invoices"]["non_payment_probability"]:
            logger.debug("Invoice %s missed its due date - reminding", self._log_label())
            self._remind(producer, cfg, scheduler)
        else:
            self.settle(producer)

    def _remind(self, producer, cfg: dict, scheduler) -> None:
        max_reminders = cfg["invoices"]["max_reminders"]
        if self.payment_reminder >= max_reminders:
            # Already reminded max_reminders times with no payment - cancel
            # instead of sending yet another reminder.
            logger.debug(
                "Invoice %s exhausted %d reminders with no payment - cancelling",
                self._log_label(), self.payment_reminder,
            )
            self.cancel(producer)
            return
        self.payment_reminder += 1
        due_minutes = cfg["invoices"]["due_minutes"]
        self.due_at = (utc_now() + timedelta(minutes=due_minutes)).isoformat()
        self.set_status("pending", producer, TOPIC, lambda i: i._event())
        logger.debug(
            "Invoice %s reminder #%d sent, new due_at=%s", self._log_label(), self.payment_reminder, self.due_at
        )
        scheduler.schedule(due_minutes * 60, lambda: self._on_due(producer, cfg, scheduler))

    def settle(self, producer) -> None:
        self.set_status("settled", producer, TOPIC, lambda i: i._event())
        self._purchase_order.mark_paid(producer, self._products)

    def cancel(self, producer) -> None:
        self.set_status("cancelled", producer, TOPIC, lambda i: i._event())
        self._purchase_order.cancel(producer)

    @classmethod
    def from_resume(cls, row: dict, purchase_order, scheduler, producer, cfg: dict, products: dict) -> "Invoice":
        """Rehydrates an Invoice from a silver.invoices_current row at
        generator startup (only ever called for status='pending' rows - see
        db.py) - reschedules the due-timer against the *actual* persisted
        due_payment_date rather than restarting a fresh window, since that
        timestamp is genuinely available (redesign plan §3c)."""
        invoice = cls(str(row["invoice_id"]), purchase_order, float(row["amount"]), products)
        invoice.status = "pending"
        invoice.payment_reminder = row["payment_reminder"]
        invoice.created_at = row["created_at"].isoformat() if row["created_at"] else invoice.created_at
        due_at = row["due_payment_date"]
        # Postgres TIMESTAMP columns come back naive from psycopg2 - this
        # project's convention is that they're implicitly UTC (no TIMESTAMPTZ
        # anywhere), so localize before doing arithmetic against utc_now().
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=timezone.utc)
        invoice.due_at = due_at.isoformat() if due_at else now_iso()
        remaining_seconds = (due_at - utc_now()).total_seconds() if due_at else 0.0
        scheduler.schedule(max(remaining_seconds, 0.0), lambda: invoice._on_due(producer, cfg, scheduler))
        return invoice
