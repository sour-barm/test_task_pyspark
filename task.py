#!/usr/bin/python3
from pyspark.sql import SparkSession, Window, functions as F
import sys

def do_work(df, window_entity, window_item, out):
    (df
 .withColumn('total_signals',
             F.sum('signal_count').over(window_entity))
 .withColumn('min_month_id',
             F.min('month_id').over(window_entity))
 .withColumn('max_month_id',
             F.max('month_id').over(window_entity))
 .filter((F.col('month_id') == F.col('min_month_id')) | (F.col('month_id') == F.col('max_month_id')))
 .withColumn('row_number',
             F.row_number().over(window_item))
 .filter(F.col('row_number') == 1)
 .withColumn('oldest_item_id',
             F.first('item_id').over(window_entity))
 .withColumn('newest_item_id',
             F.last('item_id').over(window_entity))
 .drop('item_id', 'source', 'month_id', 'signal_count', 'min_month_id', 'max_month_id', 'row_number')
 .select('entity_id', 'oldest_item_id', 'newest_item_id', 'total_signals')
 .distinct()
 .orderBy('entity_id')
 .coalesce(1)
 .write
 .parquet(out, mode='overwrite'))

def main():
    spark = SparkSession.builder.getOrCreate()
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    df = spark.read.parquet(input_path)
    window_entity = Window.partitionBy('entity_id')
    window_item = (Window
                   .partitionBy('entity_id', 'month_id')
                   .orderBy('item_id'))
    do_work(df, window_entity, window_item, output_path)

if __name__ == "__main__":
    main()
