/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.spark.v2

import org.apache.druid.common.config.NullHandling

import java.util.Optional
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class DruidDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport
  with DataSourceRegister with Logging {
  // We don't have Guice injections for modules so we need to initialize NullHandling ourselves, same as for Druid tests
  NullHandling.initializeForTests()

  override def shortName(): String = DruidDataSourceV2ShortName

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(dataSourceOptions)
  }

  override def createReader(schema: StructType,
                            dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(schema, dataSourceOptions)
  }

  /**
    * Create a writer to save a dataframe as a Druid table. Spark knows the partitioning information
    * for the dataframe, but won't share. Something like the DateBucketAndHashPartitioner from the
    * druid-spark-batch GitHub project can be used to ensure that all partitions have only one time
    * bucket they're responsible for while also setting a soft upper bound on the maximum size of a
    * segment. Otherwise, multiple indexing segments may be stored in memory, causing memory
    * pressure, and segments may contain up to the number of rows in a partition. We can't construct
    * multiple partitions for the same segment interval in a single DruidDataWriter because we can't
    * be sure we won't chose a partition number that isn't also being used for the same interval by
    * a separate DruidDataWriter.
    *
    * Additionally, while the caller knows how many partitions there are total for each segment, we
    * don't, and so the caller will need to provide the total number of partitions if necessary for
    * the desired shard spec (e.g. Numbered or HashedNumbered). Otherwise, we'll have to default to
    * a partition count of 1 in the shard spec, meaning that Numbered will degrade to Linear and
    * users won't have atomic loading of segments within an interval.
    *
    * To work around this, users can provide a broadcast map from (Spark) partition id to the
    * partition num and total partition count for the segment
    *
    * TODO: This may actually be bigger problem. Partition ids may not be contiguous for all segments
    *  in a bucket unless callers are meticulous about partitioning, in which case almost all shard
    *  specs will consider the output incomplete.
    *
    * @param uuid
    * @param schema
    * @param saveMode
    * @param dataSourceOptions
    * @return
    */
  override def createWriter(uuid: String,
                            schema: StructType,
                            saveMode: SaveMode,
                            dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    // Spark knows the partitioning information for the df, but it won't tell us. We also have very
    // limited ways to detect issues, so for now we'll need to trust that we're passed
    // TODO: Take advantage of the job id being provided (uuid in the args list)
    DruidDataSourceWriter(schema, saveMode, dataSourceOptions)
  }
}
