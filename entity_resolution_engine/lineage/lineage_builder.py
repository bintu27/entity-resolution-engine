from typing import Any, Dict


def build_lineage(
    source_type: str,
    alpha_id: Any,
    beta_id: Any,
    confidence: float,
    breakdown: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "sources": [
            {"source": "ALPHA", "id": str(alpha_id)},
            {"source": "BETA", "id": str(beta_id)},
        ],
        "confidence": confidence,
        "confidence_breakdown": breakdown,
        "entity_type": source_type,
    }
