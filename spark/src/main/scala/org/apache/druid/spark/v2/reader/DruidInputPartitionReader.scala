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

package org.apache.druid.spark.v2.reader

import org.apache.druid.java.util.common.FileUtils
import org.apache.druid.query.filter.DimFilter
import org.apache.druid.segment.realtime.firehose.{IngestSegmentFirehose, WindowedStorageAdapter}
import org.apache.druid.segment.transform.TransformSpec
import org.apache.druid.segment.QueryableIndexStorageAdapter
import org.apache.druid.spark.configuration.{Configuration, SerializableHadoopConfiguration}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.SchemaUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

class DruidInputPartitionReader(
                                 segmentStr: String,
                                 schema: StructType,
                                 filter: Option[DimFilter],
                                 columnTypes: Option[Set[String]],
                                 broadcastedHadoopConf: Broadcast[SerializableHadoopConfiguration],
                                 conf: Configuration,
                                 useSparkConfForDeepStorage: Boolean,
                                 useCompactSketches: Boolean,
                                 useDefaultNullHandling: Boolean
                               )
  extends DruidBaseInputPartitionReader(
    segmentStr,
    columnTypes,
    broadcastedHadoopConf,
    conf,
    useSparkConfForDeepStorage,
    useCompactSketches,
    useDefaultNullHandling
  ) with InputPartitionReader[InternalRow] with Logging {

  private val firehose: IngestSegmentFirehose = DruidInputPartitionReader.makeFirehose(
    new WindowedStorageAdapter(
      new QueryableIndexStorageAdapter(queryableIndex), segment.getInterval
    ),
    filter.orNull,
    schema.fieldNames.toList
  )

  override def next(): Boolean = {
    firehose.hasMore
  }

  override def get(): InternalRow = {
    SchemaUtils.convertInputRowToSparkRow(firehose.nextRow(), schema, useDefaultNullHandling)
  }

  override def close(): Unit = {
    if (Option(firehose).nonEmpty) {
      firehose.close()
    }
    if (Option(queryableIndex).nonEmpty) {
      queryableIndex.close()
    }
    if (Option(tmpDir).nonEmpty) {
      FileUtils.deleteDirectory(tmpDir)
    }
  }
}

private[v2] object DruidInputPartitionReader {
  private def makeFirehose(
                            adapter: WindowedStorageAdapter,
                            filter: DimFilter,
                            columns: List[String]): IngestSegmentFirehose = {
    // This could be in-lined into the return, but this is more legible
    val availableDimensions = adapter.getAdapter.getAvailableDimensions.asScala.toSet
    val availableMetrics = adapter.getAdapter.getAvailableMetrics.asScala.toSet
    val dimensions = columns.filter(availableDimensions.contains).asJava
    val metrics = columns.filter(availableMetrics.contains).asJava

    new IngestSegmentFirehose(List(adapter).asJava, TransformSpec.NONE, dimensions, metrics, filter)
  }
}
