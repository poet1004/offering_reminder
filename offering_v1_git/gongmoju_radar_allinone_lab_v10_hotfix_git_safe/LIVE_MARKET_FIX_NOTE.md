# Live market data fix note

This build changes the market snapshot pipeline in two ways.

1. KOSPI / KOSDAQ still use KIS first.
2. Overseas indices / futures / FX / commodities no longer depend on the `yfinance` Python package. They use Yahoo Finance chart HTTP requests through `requests`.

## Why the old build failed

- the old build depended on importing `yfinance`
- when that import failed, every non-KIS market instrument became unavailable
- the app could only show KOSPI / KOSDAQ if KIS was working

## What changed

- removed `yfinance` from `requirements.txt`
- added provider-level diagnostics for Yahoo HTTP responses
- kept last successful live cache behavior
- reduced the chance of partial-empty market boards

## 38 schedule parser fix

The 38 schedule parser now deduplicates duplicate column names and scalarizes duplicate-cell values before parsing. This prevents errors such as:

`The truth value of a Series is ambiguous`
