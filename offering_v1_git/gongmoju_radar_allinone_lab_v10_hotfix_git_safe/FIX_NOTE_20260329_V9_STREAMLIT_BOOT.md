# V9 Streamlit boot fix

## Fixed
- `st.set_page_config()` is now executed immediately after importing `streamlit`, before any other Streamlit access.
- Local runs no longer require `.streamlit/secrets.toml`; if `.env.local` already provides keys, the app skips `st.secrets` access entirely.
- `st.secrets` access is now guarded so missing secrets on local runs do not crash the app.

## Validated
- `python -m compileall app.py src scripts integrated_lab`
- `python scripts/smoke_test.py`

## Local run
1. Copy your working `.env.local` into the project root if it is not there yet.
2. Run `run_app.bat`.

## Streamlit Community Cloud
- Do **not** commit `.env.local` or real API keys.
- Put KIS/DART keys into the app's **Secrets** section in the Streamlit Cloud dashboard.
- Commit generated integrated-lab CSV outputs if you want lockup/bridge pages populated in the deployed app.
