import pandas as pd, datetime as dt, os

URL = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
OUTDIR = "projects/farside-btc-etf/processed"
OUTFILE = os.path.join(OUTDIR, "btc_etf_flows_master.csv")
os.makedirs(OUTDIR, exist_ok=True)

# nacti HTML tabulky a vyber tu hlavni
df = pd.read_html(URL)[0]
df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
df["snapshot_date"] = pd.Timestamp.utcnow().date()

# append s deduplikaci (definuj klice dle tabulky; casto 'Date' + 'Fund/Ticker')
keys = [c for c in df.columns if c.lower() in ("date","fund","ticker","symbol")]
if os.path.exists(OUTFILE):
    old = pd.read_csv(OUTFILE)
    combined = pd.concat([old, df], ignore_index=True)
    subset = (keys + ["snapshot_date"]) if keys else None
    combined = combined.drop_duplicates(subset=subset)
else:
    combined = df

combined.to_csv(OUTFILE, index=False)
print(f"Rows total: {len(combined)}")
