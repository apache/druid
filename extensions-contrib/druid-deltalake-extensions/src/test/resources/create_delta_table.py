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

import argparse
from enum import Enum
from decimal import Decimal
from delta import *
import pyspark
from pyspark.sql.types import MapType, StructType, StructField, ShortType, StringType, TimestampType, LongType, IntegerType, DoubleType, FloatType, DateType, BooleanType, ArrayType, DecimalType
from pyspark.sql.functions import expr
from datetime import datetime, timedelta
import random
from delta.tables import DeltaTable

class TableType(Enum):
    SIMPLE = "simple"
    COMPLEX = "complex"
    SNAPSHOTS = "snapshots"


def config_spark_with_delta_lake():
    builder = (
        pyspark.sql.SparkSession.builder.appName("DeltaLakeApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_dataset_with_complex_types(num_records):
    """
    Create a mock dataset with records containing complex types like arrays, structs and maps.
    Parameters:
    - num_records (int): Number of records to generate.
    Returns:
    - Tuple: A tuple containing a list of records and the corresponding schema.
      - List of Records: Each record is a tuple representing a row of data.
      - StructType: The schema defining the structure of the records.
    Example:
    ```python
    data, schema = create_dataset_with_complex_types(10)
    ```
    """
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("array_info", ArrayType(IntegerType(), True), True),
        StructField("struct_info", StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True)
        ])),
        StructField("nested_struct_info", StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("nested", StructType([
                StructField("nested_int", IntegerType(), False),
                StructField("nested_double", DoubleType(), True),
                StructField("nested_decimal", DecimalType(4, 2), True),
            ]))
        ])),
        StructField("map_info", MapType(StringType(), FloatType()))
    ])

    data = []

    for idx in range(num_records):
        record = (
            idx,
            (idx, idx + 1, idx + 2, idx + 3),
            (idx, f"{idx}"),
            (idx, f"{idx}", (idx, idx + 1.0, Decimal(idx + 0.23))),
            {"key1": idx + 1.0, "key2": idx + 1.0}
        )
        data.append(record)
    return data, schema


def create_snapshots_table(num_records):
    """
    Create a mock dataset for snapshots.
    Parameters:
    - num_records (int): Number of records to generate.
    Returns:
    - Tuple: A tuple containing a list of records and the corresponding schema pertaining to a single snapshot.
    Example:
    ```python
    data, schema = create_snapshots_table(5)
    ```
    """
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("map_info", MapType(StringType(), IntegerType()))
    ])

    data = []

    for idx in range(num_records):
        record = (
            idx,
            {"snapshotVersion": 0}
        )
        data.append(record)
    return data, schema


def update_table(spark, schema, delta_table_path):
    """
    Update table at the specified delta path with updates: deletion, partial upsert, and insertion.
    Each update generates a distinct snapshot for the Delta table.
    """
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Snapshot 1: remove record with id = 2; result : (id=0, id=2)
    delta_table.delete(condition="id=1")

    # Snapshot 2: do a partial update of snapshotInfo map for id = 2 ; result : (id=2, id=0)
    delta_table.update(
        condition="id=2",
        set={"map_info": expr("map('snapshotVersion', 2)")}
    )

    # Snapshot 3: New records to be appended; result : (id=1, id=4, id=2, id=0)
    append_data = [
        (1, {"snapshotVersion": 3}),
        (4, {"snapshotVersion": 3})
    ]
    append_df = spark.createDataFrame(append_data, schema)
    append_df.write.format("delta").mode("append").save(delta_table_path)


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

    parser.add_argument('--delta_table_type', type=lambda t: TableType[t.upper()], choices=TableType,
                        default=TableType.SIMPLE, help='Choose a Delta table type to generate.')
    parser.add_argument('--save_path', default=None, required=True, help="Save path for Delta table")
    parser.add_argument('--save_mode', choices=('append', 'overwrite'), default="append",
                        help="Specify write mode (append/overwrite)")
    parser.add_argument('--partitioned_by', choices=("date", "name", "id"), default=None,
                        help="Column to partition the Delta table")
    parser.add_argument('--num_records', type=int, default=5, help="Specify number of Delta records to write")

    args = parser.parse_args()

    delta_table_type = args.delta_table_type
    save_mode = args.save_mode
    save_path = args.save_path
    num_records = args.num_records
    partitioned_by = args.partitioned_by

    spark = config_spark_with_delta_lake()

    if delta_table_type == TableType.SIMPLE:
        data, schema = create_dataset(num_records=num_records)
    elif delta_table_type == TableType.COMPLEX:
        data, schema = create_dataset_with_complex_types(num_records=num_records)
    elif delta_table_type == TableType.SNAPSHOTS:
        data, schema = create_snapshots_table(num_records)
    else:
        args.print_help()
        raise Exception("Unknown value specified for --delta_table_type")

    df = spark.createDataFrame(data, schema=schema)
    if not partitioned_by:
        df.write.format("delta").mode(save_mode).save(save_path)
    else:
        df.write.format("delta").partitionBy(partitioned_by).mode(save_mode).save(save_path)

    df.show()

    print(f"Generated Delta table records partitioned by {partitioned_by} in {save_path} in {save_mode} mode"
          f" with {num_records} records with {delta_table_type}.")

    if delta_table_type == TableType.SNAPSHOTS:
        update_table(spark, schema, save_path)


if __name__ == "__main__":
    main()
