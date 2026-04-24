from pydantic import BaseModel
from typing import Optional


class Transaction(BaseModel):
    transaction_id: str
    amount_minor: int        # always integer minor units — never float
    currency: str
    type: str
    channel: str
    entry_mode: str
    mcc: str
    is_3ds: bool


class Card(BaseModel):
    card_id: str
    last_four: str
    scheme: str
    is_virtual: bool


class Customer(BaseModel):
    customer_id: str
    account_age_days: int
    kyc_tier: str


class Merchant(BaseModel):
    merchant_id: str
    name: str
    country: str
    city: str
    terminal_id: str


class Location(BaseModel):
    ip_country: Optional[str] = None
    device_id: Optional[str] = None


class Network(BaseModel):
    rrn: str
    auth_code: str
    response_code: str
    acquirer_bin: str


class TransactionAuthorisationEvent(BaseModel):
    event_type: str = "transaction.authorisation"
    event_id: str
    schema_version: str = "2.4.0"
    emitted_at: str
    transaction: Transaction
    card: Card
    customer: Customer
    merchant: Merchant
    location: Location
    network: Network