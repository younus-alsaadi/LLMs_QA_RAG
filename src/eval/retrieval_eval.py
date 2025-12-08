from typing import List, Dict, Any, Callable, Iterable
from statistics import mean
from .models import EvalItem


def recall_at_k(retrieved_ids: List[str], gt_ids: List[str], k: int) -> float:
    """
    How many of the ground-truth doc IDs appear in the top-k retrieved IDs?
    1.0 = all ground-truth docs are found in top-k.
    """
    top_k = retrieved_ids[:k]
    if not gt_ids:
        return 0.0
    hits = sum(1 for doc_id in gt_ids if doc_id in top_k)
    return hits / len(gt_ids)


def hit_rate_at_k(retrieved_ids: List[str], gt_ids: List[str], k: int) -> float:
    """
    1.0 if at least one ground-truth doc is in top-k, else 0.0.
    """
    top_k = retrieved_ids[:k]
    return 1.0 if any(doc_id in top_k for doc_id in gt_ids) else 0.0


def mrr_at_k(retrieved_ids: List[str], gt_ids: List[str], k: int) -> float:
    """
    Mean Reciprocal Rank: 1/rank_of_first_relevant_doc.
    Higher = relevant docs appear earlier.
    """
    top_k = retrieved_ids[:k]
    for idx, doc_id in enumerate(top_k, start=1):
        if doc_id in gt_ids:
            return 1.0 / idx
    return 0.0

def evaluate_retrieval(
    eval_items: List[EvalItem],
    rag_retrieve_fn: Callable[[str, int], Iterable[Any]],
    k_values = (1, 3, 5),
):
    """
    Runs your retriever on each eval item and returns per-query metrics.

    rag_retrieve_fn(query, k) should return a list of Documents
    each with metadata["doc_id"].
    """
    results = []

    for item in eval_items:
        # call your retriever (we'll wire this later)
        docs = rag_retrieve_fn(item.query, max(k_values))
        retrieved_ids = [d.metadata.get("doc_id") for d in docs]

        metrics_for_item = {"query": item.query}