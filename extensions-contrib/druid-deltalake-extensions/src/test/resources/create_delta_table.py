#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import argparse
import delta
import pyspark
from pyspark.sql.types import StructType, StructField, ShortType, StringType, TimestampType, LongType, IntegerType, DoubleType, FloatType, DateType, BooleanType
from datetime import datetime, timedelta
import random


def config_spark_with_delta_lake():
    builder = (
        pyspark.sql.SparkSession.builder.appName("DeltaLakeApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_dataset(num_records):
    """
    Generate a mock employee dataset with different datatypes for testing purposes.

    Parameters:
    - num_records (int): Number of records to generate.

    Returns:
    - Tuple: A tuple containing a list of records and the corresponding schema.
      - List of Records: Each record is a tuple representing a row of data.
      - StructType: The schema defining the structure of the records.

    Example:
    ```python
    data, schema = create_dataset(10)
    ```
    """
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("birthday", DateType(), False),
        StructField("name", StringType(), True),
        StructField("age", ShortType(), True),
        StructField("salary", DoubleType(), True),
        StructField("bonus", FloatType(), True),
        StructField("yoe", IntegerType(), True),
        StructField("is_fulltime", BooleanType(), True),
        StructField("last_vacation_time", TimestampType(), True)
    ])

    data = []
    current_date = datetime.now()

    for i in range(num_records):
        birthday = current_date - timedelta(days=random.randint(365 * 18, 365 * 30))
        age = (current_date - birthday).days // 365
        is_fulltime = random.choice([True, False, None])
        record = (
            random.randint(1, 10000000000),
            birthday,
            f"Employee{i+1}",
            age,
            random.uniform(50000, 100000),
            random.uniform(1000, 5000) if is_fulltime else None,
            random.randint(1, min(20, age - 15)),
            is_fulltime,
            datetime.now() - timedelta(hours=random.randint(1, 90)) if is_fulltime else None,
        )
        data.append(record)
    return data, schema


def main():
    parser = argparse.ArgumentParser(description="Script to write a Delta Lake table.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--save_mode', choices=('append', 'overwrite'), default="overwrite",
                        help="Specify write mode (append/overwrite)")
    parser.add_argument('--save_path', default=os.path.join(os.getcwd(), "employee-delta-table"),
                        help="Save path for Delta table")
    parser.add_argument('--num_records', type=int, default=10,
                        help="Specify number of Delta records to write")

    args = parser.parse_args()

    save_mode = args.save_mode
    save_path = args.save_path
    num_records = args.num_records

    spark = config_spark_with_delta_lake()

    data, schema = create_dataset(num_records=num_records)
    df = spark.createDataFrame(data, schema=schema)
    df.write.format("delta").mode(save_mode).save(save_path)

    df.show()

    print(f"Generated Delta records to {save_path} in {save_mode} mode with {num_records} records.")


if __name__ == "__main__":
    main()
