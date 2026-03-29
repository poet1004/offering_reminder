# Free deployment guide

## Recommended: Streamlit Community Cloud

### What to push to GitHub
Commit the app code and only the lightweight generated CSV outputs you want the hosted app to read.

Recommended generated files to commit after running the integrated lab locally:
- `integrated_lab/ipo_lockup_unified_lab/workspace/dataset_out/synthetic_ipo_events.csv`
- `integrated_lab/ipo_lockup_unified_lab/workspace/unlock_out/unlock_events_backtest_input.csv` (if created)
- `integrated_lab/ipo_lockup_unified_lab/workspace/signal_out/turnover_signals.csv` (if created)
- `integrated_lab/ipo_lockup_unified_lab/workspace/turnover_backtest_out/*.csv` (summary/trades if created)

### What not to push
- `.env.local`
- real API keys
- minute DB files
- cache/log folders
- bulky temp files

### Secrets on Streamlit Cloud
Put these into the app's **Secrets** UI, not in Git:
- `KIS_APP_KEY`
- `KIS_APP_SECRET`
- `KIS_ENV`
- `KIS_ACCOUNT_NO`
- `KIS_CANO`
- `KIS_ACNT_PRDT_CD`
- `DART_API_KEY`

### Local vs deployed behavior
- Local: `.env.local` is enough. `.streamlit/secrets.toml` is optional.
- Cloud: Git repo + Secrets UI is the simplest route.

## Simple flow
1. Run the integrated lab locally if you need lockup/bridge pages populated.
2. Commit the selected generated CSV outputs.
3. Push to GitHub.
4. Create a Streamlit Community Cloud app from the repo.
5. Set secrets in the Streamlit Cloud dashboard.
6. Deploy using `app.py` as the entry point.
