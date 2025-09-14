"""Batch analysis routines.
This module provides helpers to load CSVs into DataFrames and run common analyses.
"""
from typing import Dict
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, DoubleType

def load_dataframes(spark: SparkSession, data_dir: str = "./data") -> Dict[str, object]:
    files = {
        'category': 'category.csv',
        'customer': 'customer.csv',
        'product': 'product.csv',
        'transactions': 'transactions.csv'
    }
    dfs = {}
    for key, fname in files.items():
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"Warning: {path} not found — please place CSV files in the data/ directory")
            dfs[key] = None
            continue
        df = spark.read.option('header', True).option('escape', '"').csv(path)
        dfs[key] = df
    return dfs

def prepare_transactions_df(transactions_df):
    """Parse product_metadata JSON and explode into rows with product_id, quantity, item_price."""
    if transactions_df is None:
        return None
    
    # schema for product_metadata JSON
    product_schema = ArrayType(StructType([
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("item_price", DoubleType(), True),
    ]))

    df = transactions_df.withColumn(
        "products", F.from_json(F.col("product_metadata"), product_schema)
    )

    # explode array -> one row per product in transaction
    df = df.withColumn("product", F.explode("products"))
    df = df.withColumn("product_id", F.col("product.product_id"))
    df = df.withColumn("quantity", F.col("product.quantity"))
    df = df.withColumn("price", F.col("product.item_price"))
    
    # compute revenue
    df = df.withColumn("revenue", F.col("price") * F.col("quantity"))
    
    return df

def top_categories(product_df, category_df, transactions_df, top_n=10):
    """Top categories by revenue."""
    if product_df is None or category_df is None or transactions_df is None:
        return None

    tx = prepare_transactions_df(transactions_df)

    joined = (
        tx.join(product_df.select(F.col("id").alias("product_id"), "category_id"), on="product_id", how="left")
          .join(category_df, on="category_id", how="left")
    )

    agg = (
        joined.groupBy("cat_level1", "cat_level2")
              .agg(F.sum("revenue").alias("total_revenue"))
              .orderBy(F.desc("total_revenue"))
              .limit(top_n)
    )
    return agg
