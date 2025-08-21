# Pull TR.SharesHeld + TR.InvestorType + ISIN (+ extra dates) for ASOF snapshots (2000–2024) using ISIN universe
import os, time, re, math
import pandas as pd
import eikon as ek

# ---------- CONFIG ----------
ASOFS = [f"{y}-12-31" for y in range(2024, 1999, -1)]  # 2024 … 2000
BASE  = r"C:\Users\habim\OneDrive - Hanken Svenska handelshogskolan\Desktop\LSEG Workspace\BN_2018"
IN_XLS = os.path.join(BASE, "USA1_ISIN.xlsx")  # curated USA ISINs (first col)
os.makedirs(BASE, exist_ok=True)

# Eikon/Workspace connection (prefer env var)
APP_KEY = os.getenv("08006cebba014d638dfdf00043c3c664da393699") or "08006cebba014d638dfdf00043c3c664da393699"
ek.set_app_key(APP_KEY)
ek.set_timeout(120000)  # ms

# Data fields
FIELDS = [
    "TR.HoldingsDate",
    "TR.EarliestHoldingsDate",
    "TR.prevHoldingsDate",
    "TR.ConsHoldFilingDate",
    "TR.SharesHeld",
    "TR.InvestorType",
    "TR.ISIN",
]

# Stata export
CHUNK_ROWS = 1_000_000
STATA_VERSION = 118
ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

# ---------- HELPERS ----------
def chunks(seq, n=20):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def pull(univ, params, tries=3, pause=1.0):
    """Call ek.get_data with light retry."""
    for t in range(tries):
        df, err = ek.get_data(univ, FIELDS, parameters=params)
        if not err and df is not None and len(df):
            return df
        time.sleep(pause * (t + 1))
    return pd.DataFrame()

def ensure_dt64(df, cols):
    """Ensure these columns are datetime64[ns] (not Python 'object' dates)."""
    out = df.copy()
    for c in cols:
        if c in out.columns and not pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def write_stata_chunked(df_snapshot: pd.DataFrame, df_bytype: pd.DataFrame, base_no_ext: str):
    """
    Always write Stata .dta files.
      - snapshot → base.dta (or base_part01.dta, base_part02.dta, … if very large)
      - by_type  → base_bytype.dta
    Dates are kept as datetime64 and converted via convert_dates={'col':'td'}.
    """
    date_cols = ["date", "earliest_date", "prev_date", "cons_filing_date"]
    snap = ensure_dt64(df_snapshot, date_cols)
    convert_map = {c: "td" for c in date_cols if c in snap.columns}

    n = len(snap)
    if n == 0:
        raise ValueError("Snapshot is empty; nothing to write.")

    if n <= CHUNK_ROWS:
        snap.to_stata(base_no_ext + ".dta", write_index=False,
                      version=STATA_VERSION, convert_dates=convert_map)
        print(f"OK → {base_no_ext}.dta")
    else:
        parts = math.ceil(n / CHUNK_ROWS)
        for i in range(parts):
            lo, hi = i * CHUNK_ROWS, min((i + 1) * CHUNK_ROWS, n)
            snap.iloc[lo:hi].to_stata(
                base_no_ext + f"_part{(i+1):02d}.dta",
                write_index=False, version=STATA_VERSION, convert_dates=convert_map
            )
            print(f"OK → {base_no_ext}_part{(i+1):02d}.dta  [{lo:,}:{hi:,}]")

    # by_type is typically small (no date columns needed)
    df_bytype.to_stata(base_no_ext + "_bytype.dta", write_index=False, version=STATA_VERSION)
    print(f"OK → {base_no_ext}_bytype.dta")

# ---------- LOAD ISIN UNIVERSE ----------
assert os.path.exists(IN_XLS), f"Input file not found: {IN_XLS}"
ids_raw = pd.read_excel(IN_XLS, sheet_name=0)
first_col = ids_raw.columns[0]
isins = (ids_raw[first_col].dropna().astype(str).str.strip().str.upper().unique().tolist())
isins = [i for i in isins if ISIN_RE.fullmatch(i)]
if not isins:
    raise SystemExit("No valid ISINs found in the first column of USA1_ISIN.xlsx")
print(f"Universe size (ISINs): {len(isins)}")

# ---------- YEAR-END LOOP ----------
for asof in ASOFS:
    try:
        year = asof[:4]
        base_no_ext = os.path.join(BASE, f"SharesHeld{year}")  # output base name (no extension)

        params_asof = {"SDate": asof, "EDate": asof}
        params_fb   = {"SDate": f"{year}-01-01", "EDate": asof, "Frq": "Q"}

        # Pull AS-OF, then fallback if needed
        parts = [pull(ch, params_asof) for ch in chunks(isins, 20)]
        raw = pd.concat([p for p in parts if p is not None and len(p)], ignore_index=True) if parts else pd.DataFrame()

        used_fallback = False
        if raw.empty:
            parts = [pull(ch, params_fb) for ch in chunks(isins, 20)]
            raw = pd.concat([p for p in parts if p is not None and len(p)], ignore_index=True) if parts else pd.DataFrame()
            if raw.empty:
                raise ValueError(f"No rows returned for {asof} (as-of & fallback).")
            used_fallback = True

        # Normalize headers to consistent names
        COLS = {
            "Instrument": "Instrument",

            "TR.HoldingsDate": "date", "Holdings Date": "date", "Date": "date",
            "TR.EarliestHoldingsDate": "earliest_date", "Earliest Holdings Date": "earliest_date",
            "TR.prevHoldingsDate": "prev_date", "Previous Holdings Date": "prev_date", "Prev Holdings Date": "prev_date",
            "TR.ConsHoldFilingDate": "cons_filing_date", "Consolidated Holdings Filing Date": "cons_filing_date",
            "Cons Hold Filing Date": "cons_filing_date",

            "TR.SharesHeld": "SharesHeld", "Investor Shares Held": "SharesHeld",
            "TR.InvestorType": "InvestorType", "Investor Type": "InvestorType", "Investor Type Description": "InvestorType",

            "TR.ISIN": "ISIN", "ISIN": "ISIN", "ISIN Code": "ISIN",
        }
        raw = raw.rename(columns={k: v for k, v in COLS.items() if k in raw.columns})

        # Ensure / parse dates (KEEP as datetime64[ns])
        if "date" not in raw.columns:
            raw["date"] = pd.to_datetime(asof)
        else:
            raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
            if raw["date"].isna().all():
                raw["date"] = pd.to_datetime(asof)
        for c in ["earliest_date", "prev_date", "cons_filing_date"]:
            if c in raw.columns:
                raw[c] = pd.to_datetime(raw[c], errors="coerce")

        # Core validations
        need = {"Instrument", "date", "SharesHeld", "InvestorType"}
        missing = sorted(list(need - set(raw.columns)))
        if missing:
            raise ValueError(f"{asof}: Missing required columns {missing}.")

        raw["SharesHeld"]   = pd.to_numeric(raw["SharesHeld"], errors="coerce")
        raw["InvestorType"] = raw["InvestorType"].astype(str)
        if raw["SharesHeld"].isna().all():
            raise ValueError(f"{asof}: TR.SharesHeld all NaN.")
        if (raw["InvestorType"].str.strip() == "").all():
            raise ValueError(f"{asof}: TR.InvestorType empty.")

        # Inputs are ISINs; anchor ISIN to input, prefer TR.ISIN when present
        raw["ISIN_in"] = raw["Instrument"].astype(str).str.upper()
        if "ISIN" in raw.columns:
            raw["ISIN"] = raw["ISIN"].astype(str).str.upper()
            raw["ISIN"] = raw["ISIN"].where(raw["ISIN"].str.strip() != "", raw["ISIN_in"])
        else:
            raw["ISIN"] = raw["ISIN_in"]

        # If fallback, keep latest record per ISIN up to ASOF
        if used_fallback and raw["date"].notna().any():
            raw = raw.loc[raw["date"].eq(raw.groupby("ISIN")["date"].transform("max"))].copy()

        # ----- OUTPUTS (STATA) -----
        cols_snapshot = ["ISIN", "date", "earliest_date", "prev_date", "cons_filing_date",
                         "InvestorType", "SharesHeld"]
        cols_snapshot = [c for c in cols_snapshot if c in raw.columns]

        snapshot = (raw[cols_snapshot]
                    .sort_values(["ISIN", "InvestorType"] if "InvestorType" in cols_snapshot else ["ISIN"])
                    .reset_index(drop=True))

        by_type = (snapshot.groupby(["ISIN", "InvestorType"], as_index=False)["SharesHeld"]
                   .sum()
                   .rename(columns={"SharesHeld": f"SharesHeld_{year}_AsOf"}))

        if snapshot.empty or by_type.empty:
            raise ValueError(f"{asof}: Processed frames are empty.")

        write_stata_chunked(snapshot, by_type, base_no_ext)

    except Exception as e:
        print(f"[ERROR] {asof}: {e}")
