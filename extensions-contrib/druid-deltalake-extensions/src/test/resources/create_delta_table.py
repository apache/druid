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
import mimesis
import pyspark


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
    fake = mimesis.Generic()
    output = []

    for _ in range(num_records):
        data = {
            "name": fake.person.name(),
            "surname": fake.person.surname(),
            "birthday": fake.datetime.datetime(1980, 2010),
            "email": fake.person.email(),
            "country": fake.address.country(),
            "state": fake.address.state(),
            "city": fake.address.city(),
        }
        output.append(data)

    return output


def main():
    parser = argparse.ArgumentParser(description="Script to write a Delta Lake table.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--save_mode', choices=('append', 'overwrite'), default="append",
                        help="Specify write mode (append/overwrite)")
    parser.add_argument('--save_path', default=os.path.join(os.getcwd(), "people-delta-table4"),
                        help="Save path for Delta table")
    parser.add_argument('--num_records', type=int, default=10,
                        help="Specify number of Delta records to write")

    args = parser.parse_args()

    save_mode = args.save_mode
    save_path = args.save_path
    num_records = args.num_records

    spark = config_spark_with_delta_lake()

    df = spark.createDataFrame(create_dataset(num_records=num_records))
    df = df.select(df.name, df.surname, df.birthday, df.email, df.country, df.state, df.city)

    df.write.format("delta").mode(save_mode).save(save_path)

    df.show()

    print(f"Generated Delta records to {save_path} in {save_mode} mode with {num_records} records.")


if __name__ == "__main__":
    main()
