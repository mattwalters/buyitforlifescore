"""
Shared evaluation metrics for the BIFL pipeline.

All eval scripts (offline discovery, offline triage, online discovery) import
from here so that metric calculation logic is defined once and can never drift.
"""

from dataclasses import dataclass


@dataclass
class EvalMetrics:
    """Holds standard classification metrics."""

    tp: int
    fp: int
    fn: int
    tn: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    accuracy: float = 0.0


def calculate_eval_metrics(tp: int, fp: int, fn: int, tn: int = 0) -> EvalMetrics:
    """
    Computes precision, recall, F1, and accuracy from raw confusion counts.

    This is the single source of truth for metric calculation across all
    evaluation scripts. Both offline deterministic evals and online blind
    judge evals should use this function.

    Args:
        tp: True positives.
        fp: False positives.
        fn: False negatives.
        tn: True negatives (optional, used for accuracy in binary classification evals like triage).

    Returns:
        An EvalMetrics dataclass with all derived scores.
    """
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    total = tp + fp + fn + tn
    accuracy = (tp + tn) / total if total > 0 else 0.0

    return EvalMetrics(
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        precision=precision,
        recall=recall,
        f1=f1,
        accuracy=accuracy,
    )
