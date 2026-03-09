# Databricks notebook source
# MAGIC %md
# MAGIC # ERP Image Validator — Internal Brands
# MAGIC
# MAGIC Ports the web-app validation pipeline to run natively in Databricks.
# MAGIC Reads image filenames from mounted cloud storage, matches them against
# MAGIC `sportsdirect_sql.dbo.ap21_product`, and writes success/failure CSVs.
# MAGIC
# MAGIC **Stages**
# MAGIC 1. List image files from storage path
# MAGIC 2. Parse filenames → candidate ERP codes
# MAGIC 3. Load ERP data directly via `spark.sql()`
# MAGIC 4. Match: direct → multi-colour resolution → fuzzy → colour-hint fallback
# MAGIC 5. Write results to DBFS / Delta

# COMMAND ----------
# MAGIC %md ## Configuration — edit these before running

# COMMAND ----------

# Input path: root folder containing brand sub-folders on mounted storage
# Example: "dbfs:/mnt/imagery/" or "abfss://container@account.dfs.core.windows.net/imagery/"
INPUT_PATH = "dbfs:/mnt/imagery/"

# Only process these brands (folder names under INPUT_PATH).
# Set to None or [] to process every folder found.
BRANDS_TO_PROCESS = [
    # "NIKE",
    # "ADIDAS",
    # "HOKA",
]

# Output folder for result CSVs (written as single-file CSVs via coalesce)
OUTPUT_PATH = "dbfs:/mnt/erp-validator/results/"

# Image extensions to include
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tiff", ".tif",
                    ".svg", ".avif", ".bmp"}

# COMMAND ----------
# MAGIC %md ## Imports & constants

# COMMAND ----------

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Constants (mirrors constants.ts)
# ---------------------------------------------------------------------------

COLOR_MAPPINGS = {
    "BK":    "BLK",
    "WNV":   "NVY",
    "WH":    "WHT",
    "BL":    "BLU",
    "RD":    "RED",
    "GR":    "GRN",
    "WT":    "WHT",
    "OR":    "ORG",
    "GY":    "GRY",
    "PK":    "PNK",
    "VOL":   "VOLT",
    "FSA":   "FUSCHIA",
    "MULTI": "MULTI",
}

BRANDS_WITH_DASH_RULE = {"SKECHERS", "HOKA", "SAUCONY", "2XU"}

STRIP_PREFIXES = ["MRLW"]

NOISE_WORDS = {
    "NIKE", "ADIDAS", "SKECHERS", "ASICS", "HOKA", "SAUCONY", "2XU",
    "AURORA", "AIR", "MAX", "VAPOR", "FLYKNIT", "PLUS", "IMAGERY",
    "SAMPLE", "FINAL", "EDIT", "WEB", "HIGH", "LOW", "RETRO", "RUN",
    "TRAINING", "FOOTWEAR", "APPAREL", "CORE", "COMP",
}

DELIMITERS = re.compile(r"[_.\-\s]+")

# COMMAND ----------
# MAGIC %md ## Filename parsing (mirrors utils/parser.ts)

# COMMAND ----------

def normalize_color(code: str) -> str:
    upper = code.upper()
    return COLOR_MAPPINGS.get(upper, upper)


def _is_likely_color(token: str) -> bool:
    upper = token.upper()
    return (
        len(upper) == 3
        or upper in COLOR_MAPPINGS
        or upper == "MULTI"
        or bool(re.fullmatch(r"[A-Z]{3,4}", upper))
    )


def parse_filename(file_name: str, file_path: str = "") -> dict:
    """
    Returns a dict with keys:
      file_name, file_path, brand_hint, candidate_codes, color_code
    """
    # Strip known prefixes
    cleaned = file_name
    for prefix in STRIP_PREFIXES:
        if re.match(rf"^{re.escape(prefix)}-", cleaned, re.IGNORECASE):
            cleaned = re.sub(rf"^{re.escape(prefix)}-", "", cleaned, flags=re.IGNORECASE)
            break

    # Remove extension
    last_dot = cleaned.rfind(".")
    name_no_ext = cleaned[:last_dot] if last_dot != -1 else cleaned

    # Brand hint from path  (e.g. "Imagery - NIKE/...")
    brand_match = re.search(r"Imagery - ([^/\\]+)", file_path, re.IGNORECASE)
    brand_hint = brand_match.group(1).upper() if brand_match else "UNKNOWN"

    all_tokens = [t for t in DELIMITERS.split(name_no_ext) if t]

    valid_tokens = [
        t for t in all_tokens
        if t.upper() != brand_hint and t.upper() not in NOISE_WORDS
    ]

    candidates: list[str] = []
    color_code = ""

    if valid_tokens:
        s1 = valid_tokens[0].upper()
        candidates.append(s1)

        if len(valid_tokens) > 1:
            s2 = valid_tokens[1].upper()
            candidates.append(f"{s1}-{s2}")
            candidates.append(f"{s1}{s2}")

            if len(valid_tokens) > 2:
                s3 = valid_tokens[2].upper()
                candidates.append(f"{s1}-{s2}-{s3}")
                candidates.append(f"{s1}{s2}{s3}")

    # Fallback: long numeric segment  e.g. 90765640 → 907656-40
    if valid_tokens and len(valid_tokens[0]) >= 8 and valid_tokens[0].isdigit():
        s = valid_tokens[0]
        candidates.append(f"{s[:6]}-{s[6:]}")

    # Proactive color extraction
    color_candidate = next(
        (t for i, t in enumerate(all_tokens) if i > 0 and _is_likely_color(t)),
        None,
    )
    if color_candidate:
        color_code = normalize_color(color_candidate)
    elif len(valid_tokens) > 1 and len(valid_tokens[1]) <= 5:
        color_code = valid_tokens[1].upper()

    unique_candidates = list(dict.fromkeys(c for c in candidates if c))

    return {
        "file_name":       file_name,
        "file_path":       file_path,
        "brand_hint":      brand_hint,
        "candidate_codes": unique_candidates,
        "color_code":      color_code,
    }

# COMMAND ----------
# MAGIC %md ## Fuzzy matching (mirrors utils/fuzzy.ts)

# COMMAND ----------

def levenshtein(a: str, b: str) -> int:
    """Standard Levenshtein distance."""
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j - 1], dp[i][j - 1], dp[i - 1][j])
    return dp[m][n]


def find_best_fuzzy_match(
    target: str, candidates: list[str]
) -> Optional[tuple[str, int]]:
    """
    Structural fuzzy match — base style code must match EXACTLY;
    suffix may differ by ≤ 1 edit.
    Returns (matched_code, distance) or None.
    """
    upper_target = target.upper()
    target_parts = upper_target.split("-", 1)
    target_base   = target_parts[0]
    target_suffix = target_parts[1] if len(target_parts) > 1 else ""

    best_code: Optional[str] = None
    min_dist = float("inf")

    for candidate in candidates:
        uc = candidate.upper()
        if uc == upper_target:
            continue  # exact match handled elsewhere

        cparts = uc.split("-", 1)
        c_base   = cparts[0]
        c_suffix = cparts[1] if len(cparts) > 1 else ""

        if target_base != c_base:
            continue

        if target_suffix and c_suffix:
            dist = levenshtein(target_suffix, c_suffix)
            if dist <= 1 and dist < min_dist:
                min_dist = dist
                best_code = candidate
        elif target_suffix and not c_suffix and len(target_suffix) <= 1:
            if 1 < min_dist:
                min_dist = 1
                best_code = candidate
        elif not target_suffix and c_suffix and len(c_suffix) <= 1:
            if 1 < min_dist:
                min_dist = 1
                best_code = candidate

    if best_code is not None and min_dist <= 1:
        return (best_code, min_dist)
    return None

# COMMAND ----------
# MAGIC %md ## Load ERP data

# COMMAND ----------

erp_df = spark.sql("""
    SELECT
        Name,
        StyleBrand,
        StyleCategory,
        CLRIDX,
        CLRCode,
        CLRName,
        StyleGender,
        EndUse,
        Model,
        StyleRange,
        SubCategory,
        StyleProdType,
        StyleCode,
        StyleColour,
        StyleSubRange,
        ProductAgeGroup,
        STYLEIDX
    FROM sportsdirect_sql.dbo.ap21_product
    WHERE CLRName IS NOT NULL
      AND CLRCode IS NOT NULL
      AND CLRCode != 'NULL'
      AND TRIM(COALESCE(CLRCode, '')) != ''
""")

erp_df.cache()
print(f"ERP records loaded: {erp_df.count():,}")

# COMMAND ----------
# MAGIC %md ## Build in-memory lookup maps

# COMMAND ----------

def _clean_code(code: str) -> str:
    return re.sub(r"[\[\]()]+", "", (code or "")).strip().upper()


# Collect to driver — typically ~300k rows, fits comfortably in memory
erp_rows = erp_df.collect()

# style_code → first matching row  (for direct lookup)
erp_by_style: dict[str, object] = {}
# clr_code    → first matching row
erp_by_clr:   dict[str, object] = {}
# style_code  → list of all colour variants
style_variants: dict[str, list] = {}
# clr_name (upper) → clr_code  (for colour-hint fallback)
color_name_map: dict[str, str]  = {}

for row in erp_rows:
    sc = _clean_code(row["StyleCode"] or "")
    cc = _clean_code(row["CLRCode"]   or "")
    cn = (row["CLRName"] or "").upper()

    if sc and sc not in erp_by_style:
        erp_by_style[sc] = row
    if cc and cc not in erp_by_clr:
        erp_by_clr[cc] = row
    if sc:
        style_variants.setdefault(sc, []).append(row)
    if cn and cc:
        color_name_map[cn] = cc

all_erp_codes = list(erp_by_style.keys())

# Pre-compute distinct colour count per style
style_color_count: dict[str, int] = {
    sc: len({_clean_code(r["CLRCode"]) for r in rows})
    for sc, rows in style_variants.items()
}

print(f"Unique style codes : {len(erp_by_style):,}")
print(f"Unique colour codes: {len(erp_by_clr):,}")

# COMMAND ----------
# MAGIC %md ## Multi-colour resolution helper

# COMMAND ----------

def resolve_color_variant(
    candidates: list[str],
    color_hint: str,
    matched_style: str,
) -> Optional[object]:
    """
    Given a style code with multiple colour variants, try to pin down
    the exact colour from the filename's color_hint.
    Returns the matching ERP row, or None.
    """
    variants = style_variants.get(matched_style, [])
    ch = color_hint.upper()
    if not ch:
        return None

    # Strategy 1 — exact CLRCode match
    for r in variants:
        if _clean_code(r["CLRCode"]) == ch:
            return r

    # Strategy 2 — colour name lookup
    mapped = color_name_map.get(ch)
    if mapped:
        for r in variants:
            if _clean_code(r["CLRCode"]) == mapped:
                return r

    # Strategy 3 — partial CLRCode
    for r in variants:
        cc = _clean_code(r["CLRCode"])
        if cc.startswith(ch) or ch in cc:
            return r

    # Strategy 4 — reverse lookup via COLOR_MAPPINGS
    normalized = COLOR_MAPPINGS.get(ch)
    if normalized:
        for r in variants:
            if _clean_code(r["CLRCode"]) == normalized:
                return r

    # Strategy 5 — first segment of CLRCode
    for r in variants:
        cc = _clean_code(r["CLRCode"])
        if cc.split("-")[0] == ch:
            return r

    return None

# COMMAND ----------
# MAGIC %md ## Core matching logic

# COMMAND ----------

def match_file(parsed: dict) -> dict:
    """
    Runs the full 4-stage match for one file.
    Returns a result dict.
    """
    file_name   = parsed["file_name"]
    file_path   = parsed["file_path"]
    brand_hint  = parsed["brand_hint"]
    candidates  = parsed["candidate_codes"]
    color_code  = parsed["color_code"]

    base_result = {
        "file_name":           file_name,
        "brand_hint":          brand_hint,
        "attempted_codes":     ", ".join(candidates),
        "product_code":        None,
        "style_code":          None,
        "clr_code":            None,
        "name":                None,
        "brand":               None,
        "category":            None,
        "gender":              None,
        "product_type":        None,
        "style_sub_range":     None,
        "style_colour":        None,
        "product_end_use":     None,
        "product_model":       None,
        "product_sub_category":None,
        "clr_idx":             None,
        "age_group":           None,
        "clr_name":            None,
        "style_idx":           None,
        "product_range":       None,
        "color_variant_count": 0,
        "status":              "FAILURE",
        "reason":              "No match found",
        "is_fuzzy":            False,
    }

    def _fill(result: dict, row, style_code: str) -> dict:
        cc = _clean_code(row["CLRCode"])
        result.update({
            "product_code":         style_code,
            "style_code":           style_code,
            "clr_code":             cc,
            "name":                 row["Name"],
            "brand":                (row["StyleBrand"] or "").upper(),
            "category":             row["StyleCategory"],
            "gender":               row["StyleGender"],
            "product_type":         row["StyleProdType"],
            "style_sub_range":      row["StyleSubRange"],
            "style_colour":         row["StyleColour"],
            "product_end_use":      row["EndUse"],
            "product_model":        row["Model"],
            "product_sub_category": row["SubCategory"],
            "clr_idx":              row["CLRIDX"],
            "age_group":            row["ProductAgeGroup"],
            "clr_name":             row["CLRName"],
            "style_idx":            row["STYLEIDX"],
            "product_range":        row["StyleRange"],
            "color_variant_count":  style_color_count.get(style_code, 0),
        })
        return result

    # ----------------------------------------------------------------
    # Stage A: direct match (longest candidate first)
    # ----------------------------------------------------------------
    for cand in sorted(candidates, key=len, reverse=True):
        row = erp_by_style.get(cand) or erp_by_clr.get(cand)
        if row is None:
            continue

        matched_style = _clean_code(row["StyleCode"])
        count = style_color_count.get(matched_style, 1)

        if count <= 1:
            # Single colour — straightforward success
            result = _fill(dict(base_result), row, matched_style)
            result["status"] = "SUCCESS"
            result["reason"] = f"Direct match on '{cand}'"
            return result

        # Multi-colour — try to resolve which variant
        resolved = resolve_color_variant(candidates, color_code, matched_style)
        if resolved:
            result = _fill(dict(base_result), resolved, matched_style)
            result["status"] = "SUCCESS"
            result["reason"] = f"Multi-colour resolved via color hint '{color_code}'"
            return result

        if not color_code:
            result = _fill(dict(base_result), row, matched_style)
            result["status"] = "Multi Colour AP21 - No Reference"
            result["reason"] = "Matched style has multiple colours but no colour hint in filename"
            return result

        result = _fill(dict(base_result), row, matched_style)
        result["status"] = "Multi Colour AP21 - Cant Find"
        result["reason"] = f"Colour hint '{color_code}' could not be matched to a variant"
        return result

    # ----------------------------------------------------------------
    # Stage B: fuzzy match
    # ----------------------------------------------------------------
    for cand in candidates:
        fuzzy = find_best_fuzzy_match(cand, all_erp_codes)
        if fuzzy:
            matched_code, dist = fuzzy
            row = erp_by_style[matched_code]
            matched_style = matched_code
            result = _fill(dict(base_result), row, matched_style)
            result["status"]      = "FUZZY"
            result["reason"]      = f"Fuzzy match '{cand}' → '{matched_code}' (distance {dist})"
            result["is_fuzzy"]    = True
            result["product_code"] = matched_code
            return result

    return base_result

# COMMAND ----------
# MAGIC %md ## List image files

# COMMAND ----------

def _list_images(root: str, brands: list[str]) -> list[tuple[str, str]]:
    """
    Recursively lists image files under `root`.
    If `brands` is non-empty only descend into those sub-folders.
    Returns list of (file_name, full_path).
    """
    results = []
    try:
        top_level = dbutils.fs.ls(root)
    except Exception as e:
        print(f"Could not list {root}: {e}")
        return results

    for entry in top_level:
        folder_name = entry.name.rstrip("/")
        if brands and folder_name.upper() not in {b.upper() for b in brands}:
            continue
        _recurse(entry.path, results)

    return results


def _recurse(path: str, out: list):
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        return
    for e in entries:
        if e.isDir():
            _recurse(e.path, out)
        else:
            ext = "." + e.name.rsplit(".", 1)[-1].lower() if "." in e.name else ""
            if ext in IMAGE_EXTENSIONS:
                out.append((e.name, e.path))


image_files = _list_images(INPUT_PATH, BRANDS_TO_PROCESS)
print(f"Image files found: {len(image_files):,}")

# COMMAND ----------
# MAGIC %md ## Run validation

# COMMAND ----------

results = []
for file_name, file_path in image_files:
    parsed = parse_filename(file_name, file_path)
    result = match_file(parsed)
    results.append(result)

success = [r for r in results if r["status"] not in ("FAILURE",)]
failure = [r for r in results if r["status"] == "FAILURE"]

print(f"Results  : {len(results):,}")
print(f"  Success: {len(success):,}")
print(f"  Failure: {len(failure):,}")

# COMMAND ----------
# MAGIC %md ## Write outputs

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

SUCCESS_SCHEMA = StructType([
    StructField("file_name",            StringType()),
    StructField("id",                   StringType()),
    StructField("status",               StringType()),
    StructField("color_variant_count",  IntegerType()),
    StructField("reason",               StringType()),
    StructField("product_code",         StringType()),
    StructField("clr_code",             StringType()),
    StructField("name",                 StringType()),
    StructField("product_range",        StringType()),
    StructField("brand",                StringType()),
    StructField("category",             StringType()),
    StructField("gender",               StringType()),
    StructField("product_type",         StringType()),
    StructField("style_sub_range",      StringType()),
    StructField("style_colour",         StringType()),
    StructField("product_end_use",      StringType()),
    StructField("product_model",        StringType()),
    StructField("product_sub_category", StringType()),
    StructField("clr_idx",              StringType()),
    StructField("age_group",            StringType()),
    StructField("clr_name",             StringType()),
    StructField("style_idx",            StringType()),
    StructField("is_fuzzy",             BooleanType()),
])

FAILURE_SCHEMA = StructType([
    StructField("file_name",       StringType()),
    StructField("brand_hint",      StringType()),
    StructField("attempted_codes", StringType()),
    StructField("reason",          StringType()),
])


def _id(file_name: str) -> str:
    return file_name.rsplit(".", 1)[0] if "." in file_name else file_name


success_rows = [
    (
        r["file_name"],
        _id(r["file_name"]),
        r["status"],
        r["color_variant_count"] or 0,
        r["reason"],
        r["product_code"],
        r["clr_code"],
        r["name"],
        r["product_range"],
        r["brand"],
        r["category"],
        r["gender"],
        r["product_type"],
        r["style_sub_range"],
        r["style_colour"],
        r["product_end_use"],
        r["product_model"],
        r["product_sub_category"],
        r["clr_idx"],
        r["age_group"],
        r["clr_name"],
        r["style_idx"],
        r["is_fuzzy"],
    )
    for r in success
]

failure_rows = [
    (r["file_name"], r["brand_hint"], r["attempted_codes"], r["reason"])
    for r in failure
]

success_df = spark.createDataFrame(success_rows, schema=SUCCESS_SCHEMA)
failure_df = spark.createDataFrame(failure_rows, schema=FAILURE_SCHEMA)

# Write as single CSV files
(
    success_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_PATH + "validation_success")
)

(
    failure_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_PATH + "validation_failed")
)

print(f"Results written to {OUTPUT_PATH}")
display(success_df)
