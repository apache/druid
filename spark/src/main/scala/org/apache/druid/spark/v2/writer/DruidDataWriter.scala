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

package org.apache.druid.spark.v2.writer

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.java.util.common.io.Closer
import org.apache.druid.java.util.common.parsers.TimestampParser
import org.apache.druid.java.util.common.{DateTimes, FileUtils, Intervals}
import org.apache.druid.segment.column.ValueType
import org.apache.druid.segment.data.{BitmapSerdeFactory, CompressionFactory, CompressionStrategy,
  ConciseBitmapSerdeFactory, RoaringBitmapSerdeFactory}
import org.apache.druid.segment.incremental.{IncrementalIndex, IncrementalIndexSchema,
  OnheapIncrementalIndex}
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.loading.DataSegmentPusher
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory
import org.apache.druid.segment.{IndexSpec, IndexableAdapter, QueryableIndexIndexableAdapter}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{DruidConfigurationKeys, DruidDataWriterConfig}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.{ComplexTypeRegistry, SegmentWriterRegistry,
  ShardSpecRegistry}
import org.apache.druid.spark.utils.NullHandlingUtils
import org.apache.druid.spark.v2.{INDEX_IO, INDEX_MERGER_V9}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.ArrayType

import java.io.Closeable
import java.util.{List => JList}
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A DruidDataWriter does the actual work of writing a partition of a dataframe to files Druid
  * knows how to read. Users should not use this class directly but should instead call write() on their dataframe
  * (e.g. df.write.format("druid").options(Map[String, String](...)).save()).
  *
  * TODO: Describe the writing logic (creating the map from bucket to flushed indexers (i.e. adapters
  *  and the current open index), how indices are merged and pushed to deep storage, partition number
  *  /counts concerns, etc.
  *
  * @param config An object holding necessary configuration settings for the writer passed along
  *               from the driver.
  */
class DruidDataWriter(config: DruidDataWriterConfig) extends DataWriter[InternalRow] with Logging {
  NullHandlingUtils.initializeDruidNullHandling(config.useDefaultNullHandling)

  private val tmpPersistDir = FileUtils.createTempDir("persist")
  private val tmpMergeDir = FileUtils.createTempDir("merge")
  private val closer = Closer.create()
  closer.register(
    new Closeable {
      override def close(): Unit = {
        FileUtils.deleteDirectory(tmpMergeDir)
        FileUtils.deleteDirectory(tmpPersistDir)
      }
    }
  )

  private val dataSchema: DataSchema =
    MAPPER.readValue[DataSchema](config.dataSchemaSerialized,
      new TypeReference[DataSchema] {})
  private val dimensions: JList[String] =
    dataSchema.getDimensionsSpec.getDimensions.asScala.map(_.getName).asJava
  private val tsColumn: String = dataSchema.getTimestampSpec.getTimestampColumn
  private val tsColumnIndex = config.schema.fieldIndex(tsColumn)
  private val timestampParser = TimestampParser
    .createObjectTimestampParser(dataSchema.getTimestampSpec.getTimestampFormat)

  private val partitionMap = config.partitionMap.map(_(config.partitionId))
    .getOrElse(Map[String, String]("partitionId" -> config.partitionId.toString))

  private val indexSpecConf = config.properties.dive(DruidConfigurationKeys.indexSpecPrefix)
  private val indexSpec: IndexSpec = new IndexSpec(
    DruidDataWriter.getBitmapSerde(
      indexSpecConf.get(DruidConfigurationKeys.bitmapTypeDefaultKey),
      indexSpecConf.getBoolean(
        DruidConfigurationKeys.compressRunOnSerializationKey,
        RoaringBitmapSerdeFactory.DEFAULT_COMPRESS_RUN_ON_SERIALIZATION)
    ),
    DruidDataWriter.getCompressionStrategy(
      indexSpecConf.get(
        DruidConfigurationKeys.dimensionCompressionKey,
        CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString)
    ),
    DruidDataWriter.getCompressionStrategy(
      indexSpecConf.get(
        DruidConfigurationKeys.metricCompressionKey,
        CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString)
    ),
    DruidDataWriter.getLongEncodingStrategy(
      indexSpecConf.get(
        DruidConfigurationKeys.longEncodingKey,
        CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY.toString
      )
    )
  )

  private val complexColumnTypes = {
    dataSchema.getAggregators.toSeq.filter(_.getType == ValueType.COMPLEX).map(_.getComplexTypeName)
  }

  if (complexColumnTypes.nonEmpty) {
    // Callers will need to explicitly register any complex metrics not known to ComplexTypeRegistry by default
    complexColumnTypes.foreach {
      ComplexTypeRegistry.registerByName(_, config.writeCompactSketches)
    }
  }
  ComplexTypeRegistry.registerSerdes()

  private val pusher: DataSegmentPusher = SegmentWriterRegistry.getSegmentPusher(
    config.deepStorageType, config.properties
  )

  // TODO: rewrite this without using IncrementalIndex, because IncrementalIndex bears a lot of overhead
  //  to support concurrent querying, that is not needed in Spark
  // We may have rows for multiple intervals here, so we need to keep a map of segment start time to
  // flushed adapters and currently incrementing indexers.
  private val bucketToIndexMap: mutable.Map[Long,
    (ArrayBuffer[IndexableAdapter], IncrementalIndex[_])
    ] = mutable.HashMap[Long, (ArrayBuffer[IndexableAdapter], IncrementalIndex[_])]()
    .withDefault(bucket => (ArrayBuffer.empty[IndexableAdapter], createInterval(bucket)))

  override def write(row: InternalRow): Unit = {
    val timestamp = timestampParser
      .apply(row.get(tsColumnIndex, config.schema(tsColumnIndex).dataType)).getMillis
    val bucket = getBucketForRow(timestamp)
    // Scala MutableMaps don't have a computeIfAbsent equivalent, so this is either a logical no-op or it creates the
    // entry following the function defined in the map's .withDefault declaration
    bucketToIndexMap(bucket) = bucketToIndexMap(bucket)
    var index = bucketToIndexMap(bucket)._2

    // Check index, flush if too many rows in memory and recreate
    if (index.size() == config.rowsPerPersist) {
      val adapters = bucketToIndexMap(bucket)._1 :+ flushIndex(index)
      index.close()
      bucketToIndexMap(bucket) = (adapters, createInterval(bucket))
      index = bucketToIndexMap(bucket)._2
    }
    index.add(
      index.formatRow(
        new MapBasedInputRow(
          timestamp,
          dimensions,
          // Convert to Java types that Druid knows how to handle
          config.schema
            .map(field => field.name -> (field.dataType, row.get(config.schema.indexOf(field), field.dataType))).toMap
            .mapValues {
              case (_, traversable: Traversable[_]) => traversable.toSeq.asJava
              case (dataType, unsafe: UnsafeArrayData) =>
                (0 until unsafe.numElements()).map(unsafe.get(_, dataType.asInstanceOf[ArrayType].elementType)).asJava
              case (_, x) => x
            }.asJava
        )
      )
    )
  }

  private[v2] def createInterval(startMillis: Long): IncrementalIndex[_] = {
    // Using OnHeapIncrementalIndex to minimize changes when migrating from IncrementalIndex. In the future, this should
    // be optimized further. See https://github.com/apache/druid/issues/10321 for more information.
    new OnheapIncrementalIndex.Builder()
      .setIndexSchema(
        new IncrementalIndexSchema.Builder()
          .withDimensionsSpec(dataSchema.getDimensionsSpec)
          .withQueryGranularity(
            dataSchema.getGranularitySpec.getQueryGranularity
          )
          .withMetrics(dataSchema.getAggregators: _*)
          .withTimestampSpec(dataSchema.getTimestampSpec)
          .withMinTimestamp(startMillis)
          .withRollup(dataSchema.getGranularitySpec.isRollup)
          .build()
      )
      .setMaxRowCount(config.rowsPerPersist)
      .build()
  }

  private[v2] def flushIndex(index: IncrementalIndex[_]): IndexableAdapter = {
    new QueryableIndexIndexableAdapter(
      closer.register(
        INDEX_IO.loadIndex(
          INDEX_MERGER_V9
            .persist(
              index,
              index.getInterval,
              tmpPersistDir,
              indexSpec,
              OnHeapMemorySegmentWriteOutMediumFactory.instance()
            )
        )
      )
    )
  }

  private[v2] def getBucketForRow(ts: Long): Long = {
    dataSchema.getGranularitySpec.getSegmentGranularity.bucketStart(DateTimes.utc(ts)).getMillis
  }

  override def commit(): WriterCommitMessage = {
    // Return segment locations on deep storage
    val specs = bucketToIndexMap.values.map { case (adapters, index) =>
      if (!index.isEmpty) {
        adapters += flushIndex(index)
        index.close()
      }
      if (adapters.nonEmpty) {
        // TODO: Merge adapters up to a total number of rows, and then split into new segments.
        //  The tricky piece will be determining the partition number for multiple segments (interpolate 0?)
        val finalStaticIndexer = INDEX_MERGER_V9
        val file = finalStaticIndexer.merge(
          adapters.asJava,
          true,
          dataSchema.getAggregators,
          tmpMergeDir,
          indexSpec,
          -1 // TODO: Make maxColumnsToMerge configurable
        )
        val allDimensions: JList[String] = adapters
          .map(_.getDimensionNames)
          .foldLeft(Set[String]())(_ ++ _.asScala)
          .toList
          .asJava
        val interval = adapters.map(_.getDataInterval)
          .reduce((l, r) => Intervals.utc(
            Math.min(l.getStartMillis, r.getStartMillis), Math.max(l.getEndMillis, r.getEndMillis)
          ))
        val shardSpec = ShardSpecRegistry.createShardSpec(
          config.shardSpec,
          partitionMap)
        val dataSegmentTemplate = new DataSegment(
          config.dataSource,
          dataSchema.getGranularitySpec.getSegmentGranularity.bucket(DateTimes.utc(interval.getStartMillis)),
          config.version,
          null, // scalastyle:ignore null
          allDimensions,
          dataSchema.getAggregators.map(_.getName).toList.asJava,
          shardSpec,
          -1,
          0L
        )
        val finalDataSegment = pusher.push(file, dataSegmentTemplate, true)
        Seq(MAPPER.writeValueAsString(finalDataSegment))
      } else {
        Seq.empty
      }
    }.toSeq.flatten
    closer.close()
    DruidWriterCommitMessage(specs)
  }

  override def abort(): Unit = {
    closer.close()
  }
}

object DruidDataWriter {
  def getBitmapSerde(serde: String, compressRunOnSerialization: Boolean): BitmapSerdeFactory = {
    if (serde == "concise") {
      new ConciseBitmapSerdeFactory
    } else {
      new RoaringBitmapSerdeFactory(compressRunOnSerialization)
    }
  }

  def getCompressionStrategy(strategy: String): CompressionStrategy = {
    if (CompressionStrategy.values().contains(strategy)) {
      CompressionStrategy.valueOf(strategy)
    } else {
      CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY
    }
  }

  def getLongEncodingStrategy(strategy: String): CompressionFactory.LongEncodingStrategy = {
    if (CompressionFactory.LongEncodingStrategy.values().contains(strategy)) {
      CompressionFactory.LongEncodingStrategy.valueOf(strategy)
    } else {
      CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY
    }
  }
}
