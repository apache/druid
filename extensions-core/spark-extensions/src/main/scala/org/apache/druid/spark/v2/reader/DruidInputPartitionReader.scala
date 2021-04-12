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

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.{FileUtils, ISE, StringUtils}
import org.apache.druid.query.filter.DimFilter
import org.apache.druid.segment.realtime.firehose.{IngestSegmentFirehose, WindowedStorageAdapter}
import org.apache.druid.segment.transform.TransformSpec
import org.apache.druid.segment.{QueryableIndex, QueryableIndexStorageAdapter}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.SerializableHadoopConfiguration
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.{ComplexMetricRegistry, SegmentReaderRegistry}
import org.apache.druid.spark.utils.SchemaUtils
import org.apache.druid.spark.v2.INDEX_IO
import org.apache.druid.timeline.DataSegment
import org.apache.druid.utils.CompressionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

import java.io.{File, IOException}
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

class DruidInputPartitionReader(
                                 segmentStr: String,
                                 schema: StructType,
                                 filter: Option[DimFilter],
                                 columnTypes: Option[Set[String]],
                                 broadcastedConf: Broadcast[SerializableHadoopConfiguration],
                                 useCompactSketches: Boolean,
                                 useDefaultNullHandling: Boolean
                               )
  extends InputPartitionReader[InternalRow] with Logging {

  if (columnTypes.isDefined) {
    // Callers will need to explicitly register any complex metrics not known to ComplexMetricRegistry by default
    columnTypes.get.foreach {
      ComplexMetricRegistry.registerByName(_, useCompactSketches)
    }
  } else {
    ComplexMetricRegistry.initializeDefaults()
  }
  ComplexMetricRegistry.registerSerdes()

  private val segment =
    MAPPER.readValue[DataSegment](segmentStr, new TypeReference[DataSegment] {})
  private val conf = broadcastedConf.value.value
  private val tmpDir: File = FileUtils.createTempDir
  private val queryableIndex: QueryableIndex = loadSegment(segment)
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

  // TODO: Rewrite this to use SegmentPullers instead of manually constructing URIs
  private def loadSegment(segment: DataSegment): QueryableIndex = {
    val path = new Path(SegmentReaderRegistry.load(segment.getLoadSpec))
    val segmentDir = new File(tmpDir, segment.getId.toString)
    if (!segmentDir.exists) {
      logInfo(
        StringUtils.format(
          "Fetching segment[%s] from[%s] to [%s].", segment.getId, path, segmentDir
        )
      )
      if (!segmentDir.mkdir) throw new ISE("Failed to make directory[%s]", segmentDir)
      unzip(path, segmentDir)
    }
    val index = INDEX_IO.loadIndex(segmentDir)
    logInfo(StringUtils.format("Loaded segment[%s].", segment.getId))
    index
  }

  def unzip(zip: Path, outDir: File): Unit = {
    val fileSystem = zip.getFileSystem(conf)
    try {
      CompressionUtils.unzip(fileSystem.open(zip), outDir)
    } catch {
      case exception@(_: IOException | _: RuntimeException) =>
        logError(s"Exception unzipping $zip!", exception)
        throw exception
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
