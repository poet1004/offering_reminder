# Setup troubleshooting

## 1) `pip install -r requirements.txt` fails inside Anaconda base

If you see errors like:
- `Invalid version: '4.0.0-unsupported'`
- `Invalid version: 'cpython'`

that is usually caused by broken package metadata in the current Anaconda/base environment, not by this project itself.

Recommended fix:

```bash
conda env create -f environment.yml
conda activate gongmoju-radar
```

If the environment already exists:

```bash
conda env update -f environment.yml --prune
conda activate gongmoju-radar
```

## 2) Only KOSPI/KOSDAQ appear in the market board

This build no longer depends on the `yfinance` Python package.
Overseas index / futures / FX / commodity quotes are fetched through Yahoo Finance's chart HTTP endpoint with `requests`.

Run:

```bash
python scripts/diagnose_live_sources.py
python scripts/refresh_live_cache.py
```

Check `data/runtime/live_source_diagnostic.json` for provider-level errors.

## 3) `38 refresh failed: The truth value of a Series is ambiguous`

This was caused by duplicate column labels from the 38 schedule table.
This build deduplicates columns before parsing and scalarizes duplicated cell values.
