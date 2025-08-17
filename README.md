# Cybersecurity Spend Matching — simple pipeline (VS Code)

hi, i'm **prashant (25)**. this is my simple, readable pipeline for the *hard* dataset.

- default flow: **trying hardone** (practice run) — preview only, **no files written**.
- when happy, run the **write** profile to output CSV + JSONL.
- optional: add a note to your Debug Diary automatically when you run write.

## setup (windows friendly)
```powershell
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
put your files into `data/`:
- `product_repository.csv_`
- `cybersecurity_spend_records_hard.csv`

## run (trying hardone)
```bash
python -m src.pipeline --config config/pipeline.yaml --trying-hardone
```

## run (write)
```bash
python -m src.pipeline --config config/pipeline.yaml --write
```

## run (write + log note)
```bash
python -m src.pipeline --config config/pipeline.yaml --write --log --note "short note here"
```

## what's inside
- `src/` — code for text cleanup, matching, attempts logger, and pipeline runner
- `config/pipeline.yaml` — edit file paths and thresholds here
- `debug-diary/` — Debug Diary with notes and prompts
- `.vscode/` — launch + tasks (module mode so imports work)
- `scripts/` — quick scripts for both flows
