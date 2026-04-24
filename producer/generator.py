import random
import string
from datetime import datetime, timezone
from faker import Faker
import ulid

from producer.models import (
    TransactionAuthorisationEvent, Transaction, Card,
    Customer, Merchant, Location, Network,
)

fake = Faker(["en_GB", "en_US", "de_DE", "fr_FR"])

# Realistic MCC codes with weights (higher weight = more common)
MCC_POOL = [
    ("5411", 25),   # grocery
    ("5812", 20),   # restaurants
    ("5541", 10),   # fuel
    ("5912", 8),    # pharmacy
    ("4111", 6),    # transport
    ("5311", 6),    # department stores
    ("7011", 5),    # hotels
    ("5734", 4),    # electronics
    ("7995", 2),    # gambling — high risk
    ("6011", 2),    # ATM
    ("4829", 1),    # money transfer — very high risk
]
MCC_CODES  = [m[0] for m in MCC_POOL]
MCC_WEIGHTS = [m[1] for m in MCC_POOL]

CURRENCIES = ["GBP", "EUR", "USD", "GBP", "GBP"]  # GBP-heavy
SCHEMES = ["visa", "visa", "mastercard", "mastercard", "amex"]
KYC_TIERS = ["full", "full", "full", "basic", "none"]
CHANNELS = ["card_present", "card_present", "ecommerce", "contactless", "atm"]
ENTRY_MODES = {
    "card_present": ["chip", "swipe"],
    "ecommerce": ["manual_entry", "token"],
    "contactless": ["tap"],
    "atm": ["chip", "swipe"],
}
COUNTRIES = ["GBR", "GBR", "GBR", "USA", "DEU", "FRA", "ESP", "NLD"]


def _rand_id(prefix: str, length: int = 8) -> str:
    return f"{prefix}_{''.join(random.choices(string.hexdigits[:16], k=length))}"


def _amount_for_mcc(mcc: str, is_fraud: bool) -> int:
    """Return a realistic amount in minor units for the given MCC."""
    ranges = {
        "5411": (300, 8000),    # grocery £3–£80
        "5812": (800, 6000),    # restaurant £8–£60
        "5541": (2000, 8000),   # fuel £20–£80
        "5912": (200, 4000),    # pharmacy £2–£40
        "4111": (150, 3000),    # transport £1.50–£30
        "5311": (1000, 30000),  # dept store £10–£300
        "7011": (5000, 80000),  # hotel £50–£800
        "5734": (2000, 150000), # electronics £20–£1500
        "7995": (1000, 50000),  # gambling £10–£500
        "6011": (2000, 30000),  # ATM £20–£300
        "4829": (5000, 200000), # money transfer £50–£2000
    }
    lo, hi = ranges.get(mcc, (500, 10000))
    if is_fraud:
        # Fraud tends toward high-end amounts
        return random.randint(hi // 2, hi * 3)
    return random.randint(lo, hi)


def generate_event(is_fraud: bool = False) -> TransactionAuthorisationEvent:
    mcc = random.choices(MCC_CODES, weights=MCC_WEIGHTS)[0]
    channel = random.choice(CHANNELS)
    currency = random.choice(CURRENCIES)
    country = random.choice(COUNTRIES)

    # Fraud pattern: new accounts, high-risk MCCs, card-not-present
    if is_fraud:
        mcc = random.choices(["4829", "7995", "5734", "6011"], weights=[4, 3, 2, 1])[0]
        channel = random.choices(["ecommerce", "atm"], weights=[7, 3])[0]
        account_age_days = random.randint(0, 14)   # new account pattern
        kyc_tier = random.choices(["none", "basic", "full"], weights=[4, 3, 1])[0]
        # Geo mismatch: customer country vs transaction country
        country = random.choice(["NGA", "ROU", "BGD", "PAK", "VNM"])
    else:
        account_age_days = random.randint(10, 2000)
        kyc_tier = random.choices(KYC_TIERS, weights=[60, 20, 5, 10, 5])[0]

    entry_mode = random.choice(ENTRY_MODES.get(channel, ["chip"]))
    now = datetime.now(timezone.utc)

    return TransactionAuthorisationEvent(
        event_id=str(ulid.new()),
        emitted_at=now.isoformat(),
        transaction=Transaction(
            transaction_id=_rand_id("txn"),
            amount_minor=_amount_for_mcc(mcc, is_fraud),
            currency=currency,
            type="purchase",
            channel=channel,
            entry_mode=entry_mode,
            mcc=mcc,
            is_3ds=random.random() < 0.3,
        ),
        card=Card(
            card_id=_rand_id("card"),
            last_four=str(random.randint(1000, 9999)),
            scheme=random.choice(SCHEMES),
            is_virtual=is_fraud and random.random() < 0.4,
        ),
        customer=Customer(
            customer_id=_rand_id("cust", 6),
            account_age_days=account_age_days,
            kyc_tier=kyc_tier,
        ),
        merchant=Merchant(
            merchant_id=_rand_id("mch"),
            name=fake.company().upper(),
            country=country,
            city=fake.city(),
            terminal_id=_rand_id("TRM", 4),
        ),
        location=Location(
            ip_country=country if channel == "ecommerce" else None,
            device_id=_rand_id("dev", 4) if channel == "ecommerce" else None,
        ),
        network=Network(
            rrn=now.strftime("%y%m%d%H%M%S"),
            auth_code="A" + str(random.randint(10000, 99999)),
            response_code="00",
            acquirer_bin=str(random.randint(400000, 499999)),
        ),
    )