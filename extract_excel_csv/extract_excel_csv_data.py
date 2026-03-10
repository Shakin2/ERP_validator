# Databricks notebook source
# MAGIC %md
# MAGIC # Extract Excel / CSV Data — Product Field Mapper
# MAGIC
# MAGIC Reads an uploaded Excel (`.xlsx` / `.xls`) or CSV file, extracts data from the
# MAGIC first sheet, maps columns to target product fields, and upserts into a Delta
# MAGIC `products` table with ERP-match and AP21-export tracking.
# MAGIC
# MAGIC **Stages**
# MAGIC 1. Upload & read the file
# MAGIC 2. Load into a pandas DataFrame (first sheet for Excel)
# MAGIC 3. Convert to JSON & validate
# MAGIC 4. Map columns to target product fields; identify missing fields per row
# MAGIC 5. Create / upsert into `products` Delta table
# MAGIC 6. Produce summary JSON

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

# Catalog / schema where the products table lives — update these for your environment
CATALOG = "product_images"
SCHEMA = "imageprocessing"
PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.products"

# COMMAND ----------
# MAGIC %md ## Widgets — file path

# COMMAND ----------

dbutils.widgets.text("file_path", "", "Path to Excel or CSV file")
file_path = dbutils.widgets.get("file_path").strip()

if not file_path:
    raise ValueError(
        "file_path widget is empty — provide the DBFS or Volumes path to the Excel/CSV file."
    )

print(f"Input file: {file_path}")

# COMMAND ----------
# MAGIC %md ## Imports

# COMMAND ----------

import json
import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

# COMMAND ----------
# MAGIC %md ## Step 1 — Read the uploaded file into a pandas DataFrame

# COMMAND ----------

def read_file_to_dataframe(path: str) -> pd.DataFrame:
    """
    Read an Excel (.xlsx/.xls) or CSV file into a pandas DataFrame.
    For Excel files, only the first sheet is read.
    """
    _, ext = os.path.splitext(path.lower())

    # Resolve DBFS paths so pandas can read them
    local_path = path
    if path.startswith("dbfs:"):
        local_path = path.replace("dbfs:", "/dbfs", 1)
    elif path.startswith("/Volumes"):
        local_path = path  # Unity Catalog Volumes are already FUSE-mounted

    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(local_path, sheet_name=0, dtype=str)
        print(f"Read Excel file (first sheet): {df.shape[0]} rows, {df.shape[1]} columns")
    elif ext == ".csv":
        df = pd.read_csv(local_path, dtype=str)
        print(f"Read CSV file: {df.shape[0]} rows, {df.shape[1]} columns")
    else:
        raise ValueError(f"Unsupported file type '{ext}'. Provide .xlsx, .xls, or .csv")

    # Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]
    return df


raw_df = read_file_to_dataframe(file_path)
display(raw_df.head(10))

# COMMAND ----------
# MAGIC %md ## Step 2 — Convert DataFrame to JSON & validate

# COMMAND ----------

raw_json_str = raw_df.to_json(orient="records", force_ascii=False)
raw_records = json.loads(raw_json_str)

print(f"Converted {len(raw_records)} records to JSON")
# Quick validation: ensure we got a list of dicts
assert isinstance(raw_records, list) and all(isinstance(r, dict) for r in raw_records), \
    "JSON conversion failed — expected a list of objects"
print("JSON validation passed")

# COMMAND ----------
# MAGIC %md ## Step 3 — Map columns to target product fields
# MAGIC
# MAGIC The mapping uses a best-effort heuristic: for each target field we check a
# MAGIC list of common header synonyms (case-insensitive). Unmapped target fields are
# MAGIC reported as missing for every row.

# COMMAND ----------

# Target fields we want every product to have
TARGET_FIELDS = [
    "Product Code",
    "Product Colour",
    "Product Description",
    "Category",
    "Brand",
    "Gender",
    "Product Range",
    "Product Type",
    "Product Sub Category",
    "Age Group",
    "Sub Range",
    "EAN/Barcode Code",
]

# Synonyms / alternate header names that map to each target field.
# Keys are target field names; values are lists of possible column headers
# (compared case-insensitively).
HEADER_SYNONYMS = {
    "Product Code": [
        "product code", "productcode", "style code", "stylecode",
        "sku", "item code", "itemcode", "product_code", "style_code",
        "article", "article number", "product id", "productid",
    ],
    "Product Colour": [
        "product colour", "product color", "colour", "color",
        "colour code", "color code", "colourcode", "colorcode",
        "clr code", "clrcode", "product_colour", "product_color",
    ],
    "Product Description": [
        "product description", "description", "product name", "name",
        "productname", "product_description", "product_name", "title",
    ],
    "Category": [
        "category", "style category", "stylecategory", "product category",
        "product_category", "style_category",
    ],
    "Brand": [
        "brand", "style brand", "stylebrand", "brand name",
        "brandname", "style_brand", "brand_name",
    ],
    "Gender": [
        "gender", "style gender", "stylegender", "sex",
        "style_gender",
    ],
    "Product Range": [
        "product range", "range", "style range", "stylerange",
        "product_range", "style_range",
    ],
    "Product Type": [
        "product type", "type", "style prod type", "styleprodtype",
        "product_type", "style_prod_type", "prod type",
    ],
    "Product Sub Category": [
        "product sub category", "sub category", "subcategory",
        "product_sub_category", "sub_category",
    ],
    "Age Group": [
        "age group", "agegroup", "product age group",
        "productagegroup", "age_group", "product_age_group",
    ],
    "Sub Range": [
        "sub range", "subrange", "style sub range", "stylesubrange",
        "sub_range", "style_sub_range",
    ],
    "EAN/Barcode Code": [
        "ean/barcode code", "ean", "barcode", "ean code", "barcode code",
        "ean/barcode", "upc", "gtin", "ean_code", "barcode_code",
    ],
}


def build_column_mapping(source_columns: list[str]) -> dict[str, str | None]:
    """
    Returns {target_field: source_column_name} for each target field.
    Value is None when no matching source column is found.
    """
    lower_to_original = {c.lower(): c for c in source_columns}
    mapping: dict[str, str | None] = {}

    for target, synonyms in HEADER_SYNONYMS.items():
        matched = None
        for syn in synonyms:
            if syn in lower_to_original:
                matched = lower_to_original[syn]
                break
        mapping[target] = matched

    return mapping


column_mapping = build_column_mapping(list(raw_df.columns))

print("Column mapping results:")
for target, source in column_mapping.items():
    status = f"-> '{source}'" if source else "** NOT FOUND **"
    print(f"  {target:30s} {status}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Extract mapped product data & identify missing fields

# COMMAND ----------

def extract_products(records: list[dict], mapping: dict[str, str | None]) -> list[dict]:
    """
    For each record, extract mapped field values and track which target fields
    are missing or empty.
    """
    products = []
    for idx, record in enumerate(records):
        product = {}
        missing_fields = []

        for target, source_col in mapping.items():
            if source_col is None:
                # No column mapped at all
                product[target] = None
                missing_fields.append(target)
            else:
                value = record.get(source_col)
                if value is None or str(value).strip() == "" or str(value).strip().lower() == "nan":
                    product[target] = None
                    missing_fields.append(target)
                else:
                    product[target] = str(value).strip()

        product["_row_index"] = idx
        product["_missing_fields"] = missing_fields
        products.append(product)

    return products


products = extract_products(raw_records, column_mapping)

print(f"Extracted {len(products)} products")
missing_summary = {}
for p in products:
    for f in p["_missing_fields"]:
        missing_summary[f] = missing_summary.get(f, 0) + 1

if missing_summary:
    print("\nMissing field counts across all products:")
    for field, count in sorted(missing_summary.items(), key=lambda x: -x[1]):
        print(f"  {field:30s} {count:>6d} / {len(products)}")

# Build the mapping summary JSON
mapping_summary = {
    "column_mappings": {
        target: {"source_column": source, "mapped": source is not None}
        for target, source in column_mapping.items()
    },
    "total_products": len(products),
    "products": [
        {
            "row_index": p["_row_index"],
            "data": {t: p[t] for t in TARGET_FIELDS},
            "missing_fields": p["_missing_fields"],
        }
        for p in products
    ],
}

mapping_json = json.dumps(mapping_summary, indent=2, ensure_ascii=False)
print("\nMapping summary JSON produced successfully")

# COMMAND ----------
# MAGIC %md ## Step 5 — Create `products` table & upsert records
# MAGIC
# MAGIC The table stores all target product fields plus two tracking booleans:
# MAGIC - `erp_matched` — set to `True` when the Product Code already exists in the ERP
# MAGIC - `exported_for_ap21` — set to `True` once the product has been exported

# COMMAND ----------

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Create the products table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PRODUCTS_TABLE} (
        product_code        STRING,
        product_colour      STRING,
        product_description STRING,
        category            STRING,
        brand               STRING,
        gender              STRING,
        product_range       STRING,
        product_type        STRING,
        product_sub_category STRING,
        age_group           STRING,
        sub_range           STRING,
        ean_barcode_code    STRING,
        erp_matched         BOOLEAN DEFAULT FALSE,
        exported_for_ap21   BOOLEAN DEFAULT FALSE
    )
    USING DELTA
""")

print(f"Table {PRODUCTS_TABLE} is ready")

# COMMAND ----------
# MAGIC %md ### Load ERP style codes for matching

# COMMAND ----------

# Load distinct style codes from the ERP to check for matches
erp_style_codes = set()
try:
    erp_codes_df = spark.sql("""
        SELECT DISTINCT UPPER(TRIM(StyleCode)) AS style_code
        FROM sportsdirect_sql.dbo.ap21_product
        WHERE StyleCode IS NOT NULL
          AND TRIM(COALESCE(StyleCode, '')) != ''
    """)
    erp_style_codes = {row["style_code"] for row in erp_codes_df.collect()}
    print(f"Loaded {len(erp_style_codes):,} distinct ERP style codes for matching")
except Exception as e:
    print(f"Warning: Could not load ERP style codes ({e}). erp_matched will default to False.")

# COMMAND ----------
# MAGIC %md ### Upsert products into the Delta table

# COMMAND ----------

# Build rows for the Spark DataFrame
PRODUCT_SCHEMA = StructType([
    StructField("product_code",        StringType()),
    StructField("product_colour",      StringType()),
    StructField("product_description", StringType()),
    StructField("category",            StringType()),
    StructField("brand",               StringType()),
    StructField("gender",              StringType()),
    StructField("product_range",       StringType()),
    StructField("product_type",        StringType()),
    StructField("product_sub_category", StringType()),
    StructField("age_group",           StringType()),
    StructField("sub_range",           StringType()),
    StructField("ean_barcode_code",    StringType()),
    StructField("erp_matched",         BooleanType()),
    StructField("exported_for_ap21",   BooleanType()),
])

skipped_no_code = []
rows_to_upsert = []

for p in products:
    code = p.get("Product Code")
    if not code:
        skipped_no_code.append(p["_row_index"])
        continue

    erp_matched = code.upper().strip() in erp_style_codes

    rows_to_upsert.append((
        code,
        p.get("Product Colour"),
        p.get("Product Description"),
        p.get("Category"),
        p.get("Brand"),
        p.get("Gender"),
        p.get("Product Range"),
        p.get("Product Type"),
        p.get("Product Sub Category"),
        p.get("Age Group"),
        p.get("Sub Range"),
        p.get("EAN/Barcode Code"),
        erp_matched,
        False,  # exported_for_ap21 defaults to False for new inserts
    ))

print(f"Products to upsert: {len(rows_to_upsert)}")
if skipped_no_code:
    print(f"Skipped (no Product Code): {len(skipped_no_code)} rows")

# COMMAND ----------

if rows_to_upsert:
    incoming_df = spark.createDataFrame(rows_to_upsert, schema=PRODUCT_SCHEMA)

    # Create a temp view for MERGE
    incoming_df.createOrReplaceTempView("incoming_products")

    # MERGE: update fields + erp_matched if product exists, otherwise insert
    spark.sql(f"""
        MERGE INTO {PRODUCTS_TABLE} AS target
        USING incoming_products AS source
        ON target.product_code = source.product_code
        WHEN MATCHED THEN UPDATE SET
            target.product_colour       = source.product_colour,
            target.product_description  = source.product_description,
            target.category             = source.category,
            target.brand                = source.brand,
            target.gender               = source.gender,
            target.product_range        = source.product_range,
            target.product_type         = source.product_type,
            target.product_sub_category = source.product_sub_category,
            target.age_group            = source.age_group,
            target.sub_range            = source.sub_range,
            target.ean_barcode_code     = source.ean_barcode_code,
            target.erp_matched          = source.erp_matched
        WHEN NOT MATCHED THEN INSERT (
            product_code, product_colour, product_description, category,
            brand, gender, product_range, product_type,
            product_sub_category, age_group, sub_range, ean_barcode_code,
            erp_matched, exported_for_ap21
        ) VALUES (
            source.product_code, source.product_colour, source.product_description,
            source.category, source.brand, source.gender, source.product_range,
            source.product_type, source.product_sub_category, source.age_group,
            source.sub_range, source.ean_barcode_code,
            source.erp_matched, source.exported_for_ap21
        )
    """)

    print("MERGE completed successfully")
else:
    print("No products with a Product Code to upsert")

# COMMAND ----------
# MAGIC %md ## Step 6 — Identify export candidates & build summary JSON

# COMMAND ----------

# Re-read the products table to get current state
products_table_df = spark.sql(f"""
    SELECT * FROM {PRODUCTS_TABLE}
""")

all_products = [row.asDict() for row in products_table_df.collect()]

# Classify products
export_candidates = [
    p for p in all_products
    if not p["erp_matched"] and not p["exported_for_ap21"]
]

already_in_erp = [
    p for p in all_products
    if p["erp_matched"]
]

# Build per-product missing fields detail from the current batch
missing_details = {}
for p in products:
    code = p.get("Product Code")
    if code and p["_missing_fields"]:
        missing_details[code] = p["_missing_fields"]

# Determine which codes were new vs updated
upserted_codes = {r[0] for r in rows_to_upsert}

newly_added_codes = [
    p["product_code"] for p in all_products
    if p["product_code"] in upserted_codes and not p["erp_matched"]
]

updated_codes = [
    p["product_code"] for p in all_products
    if p["product_code"] in upserted_codes and p["erp_matched"]
]

# Build the final summary JSON
summary = {
    "status": "success",
    "total_records_processed": len(products),
    "skipped_no_product_code": len(skipped_no_code),
    "total_in_table": len(all_products),
    "database_operations": {
        "upserted": len(rows_to_upsert),
        "skipped": len(skipped_no_code),
    },
    "missing_field_details": missing_details,
    "newly_added_products": newly_added_codes,
    "updated_products": updated_codes,
    "export_candidates": {
        "description": "Products where erp_matched=False AND exported_for_ap21=False",
        "count": len(export_candidates),
        "product_codes": [p["product_code"] for p in export_candidates],
    },
    "already_in_erp": {
        "description": "Products where erp_matched=True",
        "count": len(already_in_erp),
        "product_codes": [p["product_code"] for p in already_in_erp],
    },
}

summary_json = json.dumps(summary, indent=2, ensure_ascii=False, default=str)
print(summary_json)

# COMMAND ----------
# MAGIC %md ## Final output

# COMMAND ----------

# Return the summary JSON as the notebook result (can be captured by calling notebooks)
dbutils.notebook.exit(summary_json)
