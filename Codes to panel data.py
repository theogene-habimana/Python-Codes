

import os
import pandas as pd
from typing import List

# ========= USER SETTINGS =========
BASE_DIR = r"C:\Users\habim\OneDrive - Hanken Svenska handelshogskolan\Desktop\LSEG Workspace\Western Asia"  # folder that CONTAINS Central Asia.xlsx
INPUT_FILE = "Western Asia.xlsx"
SHEET_NAME = "Western Asia"
OUTPUT_FILE = "Western Asia_Panel.xlsx"

# Name of the metric in this sheet (rename if you like)
METRIC_NAME = "Value"

# Year span enforced for every ISIN
YEAR_MIN, YEAR_MAX = 2000, 2024
YEARS_ALL = list(range(YEAR_MIN, YEAR_MAX + 1))
# =================================

def clean_col_label(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return " ".join(str(x).replace("\n", " ").strip().split())

def is_int_year(x) -> bool:
    try:
        y = int(str(x).strip())
        return YEAR_MIN <= y <= YEAR_MAX
    except Exception:
        return False

def main():
    input_path = os.path.join(BASE_DIR, INPUT_FILE)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Could not find: {input_path}")

    # File layout:
    # row 0: metadata headers + "year" repeated for time-series columns
    # row 1: mostly blank
    # row 2: actual years under those "year" columns (2000..2024)
    # data from row 3 downward
    raw = pd.read_excel(input_path, sheet_name=SHEET_NAME, header=None)

    if raw.shape[0] < 4:
        raise ValueError("The sheet looks too short; expected at least 4 rows (3 header rows + data).")

    header_row0 = raw.iloc[0].tolist()
    header_row2 = raw.iloc[2].tolist()

    # Year columns: where top header == 'year' and row-2 has a valid year in range
    year_col_idx: List[int] = []
    year_labels: List[int] = []
    for j, top in enumerate(header_row0):
        if isinstance(top, str) and top.strip().lower() == "year":
            yr = header_row2[j]
            if is_int_year(yr):
                year_col_idx.append(j)
                year_labels.append(int(str(yr).strip()))

    if not year_col_idx:
        raise ValueError("No valid year columns (2000–2024) were found. Check the header layout.")

    # Metadata columns: anything not 'year' (non-empty)
    meta_cols_idx: List[int] = []
    meta_names: List[str] = []
    for j, top in enumerate(header_row0):
        if not (isinstance(top, str) and top.strip().lower() == "year"):
            name = clean_col_label(top)
            if name:
                meta_cols_idx.append(j)
                meta_names.append(name)

    # Slice data rows
    data = raw.iloc[3:].reset_index(drop=True).copy()
    keep_idx = meta_cols_idx + year_col_idx
    data = data.iloc[:, keep_idx].copy()

    # Rename columns: metadata names + integer years
    rename_map = {}
    for i, _j in enumerate(meta_cols_idx):
        rename_map[data.columns[i]] = meta_names[i]
    for k, _j in enumerate(year_col_idx):
        pos = len(meta_cols_idx) + k
        rename_map[data.columns[pos]] = int(year_labels[k])
    data = data.rename(columns=rename_map)

    # Find ISIN column (case-insensitive exact)
    isin_candidates = [c for c in data.columns if str(c).strip().lower() == "isin"]
    if not isin_candidates:
        raise KeyError(f"Could not find an 'ISIN' column among: {list(data.columns)}")
    ISIN_COL = isin_candidates[0]

    # Keep only rows with an ISIN
    data = data[data[ISIN_COL].notna()].copy()

    # Trim string metadata
    for col in meta_names:
        if col in data.columns and pd.api.types.is_string_dtype(data[col]):
            data[col] = data[col].astype("string").str.strip()

    # Coerce year columns to numeric (they may contain text)
    year_cols = [c for c in data.columns if isinstance(c, int) and YEAR_MIN <= c <= YEAR_MAX]
    for yc in year_cols:
        data[yc] = pd.to_numeric(data[yc], errors="coerce")

    # Long format
    id_vars = [col for col in meta_names if col in data.columns]  # existing metadata cols
    long = data.melt(id_vars=id_vars, value_vars=year_cols, var_name="Year", value_name=METRIC_NAME)

    # Ensure Year is Int64
    long["Year"] = pd.to_numeric(long["Year"], errors="coerce").astype("Int64")

    # ---- ENSURE FULL ISIN x YEAR GRID (2000..2024) ----
    # List of unique ISINs
    isins = (
        long[ISIN_COL]
        .dropna()
        .astype("string")
        .str.strip()
        .unique()
        .tolist()
    )

    # Build complete grid
    grid = pd.MultiIndex.from_product([isins, YEARS_ALL], names=[ISIN_COL, "Year"]).to_frame(index=False)

    # Keep one row of metadata per ISIN to re-attach (first non-null per ISIN)
    # IMPORTANT: exclude ISIN from the metadata set to avoid duplicate column names
    meta_cols_present = [c for c in id_vars if c in long.columns and c != ISIN_COL]
    meta_first = (
        long[[ISIN_COL] + meta_cols_present]
        .drop_duplicates(subset=[ISIN_COL])
        .groupby(ISIN_COL, as_index=False)
        .first()
    )

    # Merge values onto grid
    values_only = long[[ISIN_COL, "Year", METRIC_NAME]].copy()
    panel = grid.merge(values_only, on=[ISIN_COL, "Year"], how="left")

    # Attach metadata
    panel = panel.merge(meta_first, on=ISIN_COL, how="left")

    # ---- REPLACE missing / non-numeric with "." ----
    panel[METRIC_NAME] = pd.to_numeric(panel[METRIC_NAME], errors="coerce")
    panel[METRIC_NAME] = panel[METRIC_NAME].astype(object).where(panel[METRIC_NAME].notna(), ".")

    # Order columns nicely: ISIN, Year, key metadata, then value
    preferred_meta = [
        "Identifier",
        "Company Name",
        "Country of Headquarters",
        "RIC",
        "TRBC Industry Name",
    ]
    ordered_meta = [c for c in preferred_meta if c in panel.columns]
    extra_meta = [c for c in meta_cols_present if c not in ordered_meta]

    panel = panel[[ISIN_COL, "Year"] + ordered_meta + extra_meta + [METRIC_NAME]]

    # Sort for readability
    panel = panel.sort_values([ISIN_COL, "Year"]).reset_index(drop=True)

    # Write output
    out_path = os.path.join(BASE_DIR, OUTPUT_FILE)
    panel.to_excel(out_path, index=False)
    print(f"Panel (ISIN-Year) written to:\n{out_path}")
    print(f"Rows: {len(panel):,} | Cols: {len(panel.columns):,}")
    print(f"Years enforced: {YEAR_MIN}–{YEAR_MAX}")

if __name__ == "__main__":
    main()
