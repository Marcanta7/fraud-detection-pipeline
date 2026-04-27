from dataclasses import dataclass
from scorer.rules import RuleResult

DECISION_PRIORITY = {"BLOCK": 3, "REVIEW": 2, "ALLOW": 1}


@dataclass
class FinalDecision:
    decision: str           # ALLOW | REVIEW | BLOCK
    fraud_probability: float
    ml_decision: str
    rule_triggered: bool
    rule_name: str | None
    rule_reason: str | None
    decision_source: str    # "ML" | "RULE" | "ML+RULE"


def route(ml_result: dict, rule_result: RuleResult) -> FinalDecision:
    """
    Combine ML score and rule engine output.
    Most severe decision wins. If both agree, note both sources.
    """
    ml_decision = ml_result["ml_decision"]
    rule_decision = rule_result.suggested_decision

    # Take the most severe decision
    if DECISION_PRIORITY[ml_decision] >= DECISION_PRIORITY[rule_decision]:
        final = ml_decision
        source = "ML"
    else:
        final = rule_decision
        source = "RULE"

    # If both flagged, note both
    if rule_result.triggered and ml_decision != "ALLOW":
        source = "ML+RULE"

    return FinalDecision(
        decision=final,
        fraud_probability=ml_result["fraud_probability"],
        ml_decision=ml_decision,
        rule_triggered=rule_result.triggered,
        rule_name=rule_result.rule_name,
        rule_reason=rule_result.reason,
        decision_source=source,
    )