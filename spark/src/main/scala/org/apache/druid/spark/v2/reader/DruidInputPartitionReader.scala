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

import org.apache.druid.data.input.impl.{DimensionsSpec, TimestampSpec}
import org.apache.druid.data.input.{ColumnsFilter, InputEntityReader, InputRowSchema}
import org.apache.druid.indexing.input.{DruidSegmentInputEntity, DruidSegmentInputFormat}
import org.apache.druid.java.util.common.FileUtils
import org.apache.druid.query.filter.DimFilter
import org.apache.druid.segment.loading.SegmentLoader
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys, SerializableHadoopConfiguration}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.SchemaUtils
import org.apache.druid.spark.v2.INDEX_IO
import org.apache.druid.timeline.DataSegment
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.joda.time.Interval

import java.io.File
import scala.collection.JavaConverters.setAsJavaSetConverter

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

  private val availableColumns =
    segment.asQueryableIndex().getColumnNames + conf.get(DruidConfigurationKeys.timestampColumnDefaultReaderKey)

  private val inputEntityReaderRows = DruidInputPartitionReader.makeInputFormat(
    dataSegment,
    segmentLoader,
    tmpDir,
    filter.orNull,
    conf.get(DruidConfigurationKeys.timestampColumnDefaultReaderKey),
    conf.get(DruidConfigurationKeys.timestampFormatDefaultReaderKey),
    SchemaUtils.getDimensionsSpecFromIndex(segment.asQueryableIndex()),
    schema.fieldNames.toList.filter(availableColumns.contains(_))
  ).read()

  override def next(): Boolean = {
    inputEntityReaderRows.hasNext
  }

  override def get(): InternalRow = {
    SchemaUtils.convertInputRowToSparkRow(inputEntityReaderRows.next(), schema, useDefaultNullHandling)
  }

  override def close(): Unit = {
    try {
      if (Option(segment).nonEmpty) {
        segment.close()
      }
      if (Option(tmpDir).nonEmpty) {
        FileUtils.deleteDirectory(tmpDir)
      }
    } catch {
      case e: Exception =>
        // Since we're just going to rethrow e and tearing down the JVM will clean up the segment even if we can't, the
        // only leak we have to worry about is the temp file. Spark should clean up temp files as well, but rather than
        // rely on that we'll try to take care of it ourselves.
        logWarn("Encountered exception attempting to close a DruidInputPartitionReader!")
        if (Option(tmpDir).nonEmpty && tmpDir.exists()) {
          FileUtils.deleteDirectory(tmpDir)
        }
        throw e
    }
  }
}

private[v2] object DruidInputPartitionReader {
  private def makeInputFormat(
                               segment: DataSegment,
                               segmentLoader: SegmentLoader,
                               loadDir: File,
                               filter: DimFilter,
                               timestampColumnName: String,
                               timestampColumnFormat: String,
                               dimensionsSpec: DimensionsSpec,
                               columns: Seq[String]
                             ): InputEntityReader = {
    val inputFormat = new DruidSegmentInputFormat(INDEX_IO, filter)
    val timestampSpec = new TimestampSpec(timestampColumnName, timestampColumnFormat, null) // scalastyle:ignore null

    val inputSchema = new InputRowSchema(
      timestampSpec,
      dimensionsSpec,
      ColumnsFilter.inclusionBased(columns.toSet.asJava)
    )

    // The constructor for DruidSegmentInputEntity is package-private and so inaccesible here. Testing whether using
    // reflection here resolves an apparently unrelated but consistent failure in KafkaLookupExtractorFactoryTest.
    val druidSegmentInputEntityConstructor = classOf[DruidSegmentInputEntity].getDeclaredConstructor(
      classOf[SegmentLoader], classOf[DataSegment], classOf[Interval]
    )
    druidSegmentInputEntityConstructor.setAccessible(true)
    val inputSource = druidSegmentInputEntityConstructor.newInstance(
      segmentLoader,
      segment,
      segment.getInterval
    )

    inputFormat.createReader(
      inputSchema,
      inputSource,
      loadDir
    )
  }
}
