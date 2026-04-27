from dataclasses import dataclass
from producer.models import EnrichedTransactionEvent


@dataclass
class RuleResult:
    triggered: bool
    rule_name: str | None
    reason: str | None
    suggested_decision: str  # ALLOW, REVIEW, BLOCK


HIGH_RISK_COUNTRIES = {"NGA", "ROU", "BGD", "PAK", "VNM", "PRK"}
BLOCK_MCCS = {"4829"}   # money transfer — always review
REVIEW_MCCS = {"7995", "6011"}  # gambling, ATM


def run_rules(enriched: EnrichedTransactionEvent) -> RuleResult:
    """
    Run deterministic fraud rules. Returns the most severe triggered rule.
    Rules are evaluated in order — first BLOCK rule wins.
    """
    event = enriched.event
    features = enriched.features
    txn = event.transaction
    customer = event.customer
    merchant = event.merchant

    # --- BLOCK rules ---

    # New account + high amount
    if customer.account_age_days < 7 and txn.amount_minor > 50000:
        return RuleResult(
            triggered=True,
            rule_name="NEW_ACCOUNT_HIGH_AMOUNT",
            reason=f"Account age {customer.account_age_days}d, amount {txn.amount_minor}",
            suggested_decision="BLOCK",
        )

    # Extreme velocity
    if features.txn_count_1h > 10:
        return RuleResult(
            triggered=True,
            rule_name="HIGH_VELOCITY",
            reason=f"{features.txn_count_1h} transactions in last hour",
            suggested_decision="BLOCK",
        )

    # Multi-country in 24h + high risk country
    if (features.distinct_countries_24h >= 3
            and merchant.country in HIGH_RISK_COUNTRIES):
        return RuleResult(
            triggered=True,
            rule_name="GEO_ANOMALY_HIGH_RISK_COUNTRY",
            reason=f"{features.distinct_countries_24h} countries, merchant in {merchant.country}",
            suggested_decision="BLOCK",
        )

    # Money transfer MCC — always escalate
    if txn.mcc in BLOCK_MCCS:
        return RuleResult(
            triggered=True,
            rule_name="HIGH_RISK_MCC_BLOCK",
            reason=f"MCC {txn.mcc} requires mandatory review",
            suggested_decision="BLOCK",
        )

    # --- REVIEW rules ---

    # Amount >> average
    if features.amount_vs_avg_ratio > 5.0:
        return RuleResult(
            triggered=True,
            rule_name="AMOUNT_SPIKE",
            reason=f"Amount is {features.amount_vs_avg_ratio}x customer average",
            suggested_decision="REVIEW",
        )

    # Cold start + high amount
    if features.is_cold_start and txn.amount_minor > 20000:
        return RuleResult(
            triggered=True,
            rule_name="COLD_START_HIGH_AMOUNT",
            reason=f"Unknown customer, amount {txn.amount_minor}",
            suggested_decision="REVIEW",
        )

    # Gambling / ATM MCC
    if txn.mcc in REVIEW_MCCS:
        return RuleResult(
            triggered=True,
            rule_name="REVIEW_MCC",
            reason=f"MCC {txn.mcc} flagged for review",
            suggested_decision="REVIEW",
        )

    # New account moderate amount
    if customer.account_age_days < 14 and txn.amount_minor > 20000:
        return RuleResult(
            triggered=True,
            rule_name="NEW_ACCOUNT_MODERATE_AMOUNT",
            reason=f"Account age {customer.account_age_days}d, amount {txn.amount_minor}",
            suggested_decision="REVIEW",
        )

    # Multi-country with unknown MCC
    if features.distinct_countries_24h >= 2 and not features.is_known_mcc:
        return RuleResult(
            triggered=True,
            rule_name="GEO_ANOMALY_UNKNOWN_MCC",
            reason=f"{features.distinct_countries_24h} countries, unknown MCC",
            suggested_decision="REVIEW",
        )

    return RuleResult(
        triggered=False,
        rule_name=None,
        reason=None,
        suggested_decision="ALLOW",
    )