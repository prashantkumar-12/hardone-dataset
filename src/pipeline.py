
import argparse, os, time, json
import pandas as pd
from typing import Dict, Any
from .matcher import RepositoryIndex, match_row
from .attempts_logger import log_attempt, make_context

def load_yaml(path: str) -> Dict[str, Any]:
    out = {}
    sec = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.strip().startswith("#"): 
                continue
            if line.endswith(":") and not line.strip().startswith("-"):
                sec = line.split(":", 1)[0].strip()
                out[sec] = {}
            elif ":" in line and sec:
                k, v = line.split(":", 1)
                out[sec][k.strip()] = v.strip().strip("'\"")
    if "match" in out:
        m = out["match"]
        try: m["threshold"] = float(m.get("threshold", 0.76))
        except: pass
        try: m["topk"] = int(m.get("topk", 4))
        except: pass
    return out

def stamp(msg): 
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def stage_load(cfg):
    stamp("loading csvs")
    repo = pd.read_csv(cfg["paths"]["repo"])
    spend = pd.read_csv(cfg["paths"]["spend"])
    return repo, spend

def stage_index(repo):
    stamp("building index")
    return RepositoryIndex(repo)

def stage_match(spend, index, threshold, topk):
    stamp("matching rows")
    records, debug = [], []
    for _, row in spend.iterrows():
        res = match_row(row, index, top_k=topk, threshold=threshold)
        records.append({
            "spend_id": row["id"],
            "supplier": row["supplier"],
            "raw_item": row["item"],
            "matched_repo_ids": ";".join([m["repo_id"] for m in res.top_matches]) if res.top_matches else "",
            "matched_products": ";".join([m["product_name"] for m in res.top_matches]) if res.top_matches else "",
            "matched_vendors":  ";".join([m["vendor_name"] for m in res.top_matches]) if res.top_matches else "",
            "scores": ";".join([f"{m['score']:.3f}" for m in res.top_matches]) if res.top_matches else ""
        })
        debug.append({
            "spend_id": row["id"],
            "supplier": row["supplier"],
            "item": row["item"],
            "description": row.get("description",""),
            "scores": res.debug
        })
    return records, debug

def stage_preview(records, n=8):
    stamp(f"preview first {n} rows")
    for r in records[:n]:
        print("-"*80)
        print(r["spend_id"], "|", r["supplier"])
        print(" item:", (r["raw_item"] or "")[:140])
        print(" matches:", r["matched_products"] or "(none)")
        print(" scores:", r["scores"] or "(none)")

def stage_write(records, debug, outdir):
    stamp("writing outputs")
    os.makedirs(outdir, exist_ok=True)
    out_csv = os.path.join(outdir, "hard_matches.csv")
    out_jsonl = os.path.join(outdir, "hard_matches_detailed.jsonl")
    pd.DataFrame(records).to_csv(out_csv, index=False, encoding="utf-8")
    with open(out_jsonl, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(d, ensure_ascii=False) for d in debug))
    stamp(f"wrote: {out_csv}")
    stamp(f"wrote: {out_jsonl}")

def main():
    ap = argparse.ArgumentParser(description="Prashant's simple pipeline (trying hardone preview or write)")
    ap.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    ap.add_argument("--peek", type=int, default=8, help="how many preview rows")
    ap.add_argument("--trying-hardone", dest="trying_hardone", action="store_true",
                    help="practice run)")
    ap.add_argument("--write", action="store_true", help="write outputs to outdir from config")
    ap.add_argument("--log", action="store_true", help="append a timestamped entry to debug-diary/attempts.*")
    ap.add_argument("--note", type=str, default="", help="short note for the attempt log")
    ap.add_argument("--tags", nargs="*", default=[], help="space-separated tags")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    paths = cfg["paths"]
    m = cfg["match"]

    t0 = time.time()
    repo, spend = stage_load(cfg)
    index = stage_index(repo)
    records, debug = stage_match(spend, index, threshold=m["threshold"], topk=m["topk"])
    stage_preview(records, n=args.peek)

    if args.trying_hardone and not args.write:
        stamp(f"[trying-hardone] would write to: {paths['outdir']} (csv rows={len(records)}, jsonl lines={len(debug)})")
    elif args.write:
        stage_write(records, debug, outdir=paths["outdir"])
    else:
        stamp("no write flags given (add --trying-hardone or --write)")

    if args.log or args.note or args.tags:
        log_attempt(note=args.note or f"Ran pipeline (trying_hardone={args.trying_hardone}, write={args.write})",
                    tags=args.tags,
                    context=make_context(args, cfg))

    stamp(f"done in {time.time()-t0:.2f}s")

if __name__ == "__main__":
    main()
