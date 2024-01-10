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
import sys

import delta
import mimesis
import pyspark

from pyspark.sql import SparkSession


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

def create_dataset(i):
    fake = mimesis.Generic()
    output = []

    for _ in range(i):
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
    save_mode = "append"
    save_path = os.path.join(os.getcwd(), "people-delta-table4")
    num_records = 10

    if len(sys.argv) > 1:
        save_mode = sys.argv[1]

    if len(sys.argv) > 2:
        save_path = sys.argv[2]

    if len(sys.argv) > 3:
        num_records = sys.argv[3]

    spark = config_spark_with_delta_lake()

    df = spark.createDataFrame(create_dataset(i=num_records))

    df = df.select(
        df.name, df.surname, df.birthday, df.email, df.country, df.state, df.city
    )

    df.write.format("delta").mode(save_mode).save(save_path)

    df.show()

    print(f"Generated delta records to {save_path} in {save_mode} mode with {num_records} records.")


if __name__ == "__main__":
    main()
