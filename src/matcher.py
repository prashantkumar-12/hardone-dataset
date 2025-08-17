
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
import pandas as pd
from difflib import SequenceMatcher
from .textnorm import normalize_text, tokens

@dataclass
class MatchResult:
    top_matches: List[Dict[str, Any]]
    debug: List[Dict[str, Any]]

def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def token_ngram_partial_ratio(prod_tokens: List[str], text_tokens: List[str]) -> float:
    if not prod_tokens or not text_tokens:
        return 0.0
    n = len(prod_tokens)
    prod_str = " ".join(prod_tokens)
    if len(text_tokens) < n:
        return seq_ratio(prod_str, " ".join(text_tokens))
    best = 0.0
    for i in range(len(text_tokens) - n + 1):
        chunk = " ".join(text_tokens[i:i+n])
        r = seq_ratio(prod_str, chunk)
        if r > best:
            best = r
    return best

class RepositoryIndex:
    def __init__(self, repo_df: pd.DataFrame):
        self.repo = repo_df.copy()
        self.repo["product_key"] = self.repo["product_name"].apply(normalize_text)
        self.repo["vendor_key"] = self.repo["vendor_name"].apply(normalize_text)
        self.repo["product_tokens"] = self.repo["product_key"].apply(tokens)

        self.vendor_to_idx = {}
        for i, row in self.repo.iterrows():
            self.vendor_to_idx.setdefault(row["vendor_key"], []).append(i)
        self.all_idx = list(self.repo.index)

    def guess_candidates(self, text_norm: str):
        cands = set()
        for vk, idxs in self.vendor_to_idx.items():
            if vk and vk in text_norm:
                cands.update(idxs)
        if not cands:
            cands.update(self.all_idx)
        return list(cands)

def score_product(text_norm: str, supplier_norm: str, text_tokens, repo_row) -> Tuple[float, Dict[str, float]]:
    pk = repo_row["product_key"]
    vk = repo_row["vendor_key"]
    ptoks = repo_row["product_tokens"]
    s_direct = seq_ratio(pk, text_norm)
    s_partial = token_ngram_partial_ratio(ptoks, text_tokens)
    s = max(s_direct, s_partial)
    vendor_bonus = 0.0
    if vk and (vk in text_norm or vk in supplier_norm):
        vendor_bonus = 0.07
    return min(1.0, s + vendor_bonus), {"s_direct": s_direct, "s_partial": s_partial, "vendor_bonus": vendor_bonus}

def match_row(row: pd.Series, repo_index: 'RepositoryIndex', top_k=4, threshold=0.76) -> MatchResult:
    text = f"{row.get('item','')} {row.get('description','')}"
    supplier = f"{row.get('supplier','')}"
    text_norm = normalize_text(text)
    supplier_norm = normalize_text(supplier)
    text_tokens = tokens(text)

    cand_idxs = repo_index.guess_candidates(text_norm)
    scored = []
    details = []
    for idx in cand_idxs:
        r = repo_index.repo.loc[idx]
        score, parts = score_product(text_norm, supplier_norm, text_tokens, r)
        scored.append((score, idx))
        details.append({
            "repo_index": int(idx),
            "repo_id": r["id"],
            "product_name": r["product_name"],
            "vendor_name": r["vendor_name"],
            "score": float(score),
            **parts
        })

    scored.sort(reverse=True, key=lambda x: x[0])
    top = [(float(s), int(i)) for s,i in scored[:top_k] if s >= threshold]

    return MatchResult(
        top_matches=[
            {
                "repo_id": repo_index.repo.loc[i]["id"],
                "product_name": repo_index.repo.loc[i]["product_name"],
                "vendor_name": repo_index.repo.loc[i]["vendor_name"],
                "score": float(s)
            } for s,i in top
        ],
        debug=details
    )
