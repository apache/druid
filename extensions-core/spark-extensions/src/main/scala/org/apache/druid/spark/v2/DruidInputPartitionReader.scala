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

import java.io.{File, IOException}
import java.util.{Collection => JCollection}

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.data.input.InputRow
import org.apache.druid.java.util.common.{FileUtils, ISE, StringUtils}
import org.apache.druid.query.filter.{AndDimFilter, BoundDimFilter, DimFilter, InDimFilter,
  LikeDimFilter, NotDimFilter, OrDimFilter, RegexDimFilter, SelectorDimFilter}
import org.apache.druid.segment.{QueryableIndex, QueryableIndexStorageAdapter}
import org.apache.druid.segment.realtime.firehose.{IngestSegmentFirehose, WindowedStorageAdapter}
import org.apache.druid.segment.transform.TransformSpec
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.registries.{ComplexMetricRegistry, SegmentReaderRegistry}
import org.apache.druid.spark.utils.{Logging, SerializableHadoopConfiguration}
import org.apache.druid.timeline.DataSegment
import org.apache.druid.utils.CompressionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In,
  LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DoubleType, FloatType, LongType,
  StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter,
  iterableAsScalaIterableConverter, seqAsJavaListConverter, setAsJavaSetConverter}

class DruidInputPartitionReader(segmentStr: String,
                                schema: StructType,
                                filters: Array[Filter],
                                columnTypes: Option[Set[String]],
                                broadcastedConf: Broadcast[SerializableHadoopConfiguration],
                                useCompactSketches: Boolean = false
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
    // scalastyle:off null
    if (filters.isEmpty) null else DruidInputPartitionReader.mapFilters(filters),
    // scalastyle:on
    schema.fieldNames.toList
  )

  override def next(): Boolean = {
    firehose.hasMore
  }

  override def get(): InternalRow = {
    DruidInputPartitionReader.convertInputRowToSparkRow(firehose.nextRow(), schema)
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

object DruidInputPartitionReader {
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

  /**
    * Map an array of Spark filters FILTERS to a Druid filter.
    *
    * @param filters The spark filters to map to a Druid filter.
    * @return A Druid filter corresponding to the union of filter conditions enumerated in FILTERS.
    */
  def mapFilters(filters: Array[Filter]): DimFilter = {
    new AndDimFilter(filters.map(mapFilter).toList.asJava).optimize()
  }

  /**
    * Convert a Spark-style filter FILTER to a Druid-style filter.
    *
    * @param filter The Spark filter to map to a Druid filter.
    * @return The Druid filter corresponding to the filter condition described by FILTER.
    */
  def mapFilter(filter: Filter): DimFilter = {
    // scalastyle:off null
    filter match {
      case And(left, right) =>
        new AndDimFilter(List(mapFilter(left), mapFilter(right)).asJava)
      case Or(left, right) =>
        new OrDimFilter(List(mapFilter(left), mapFilter(right)).asJava)
      case Not(condition) =>
        new NotDimFilter(mapFilter(condition))
      case In(field, values) =>
        new InDimFilter(field, values.map(_.toString).toSet.asJava, null, null)
      case StringContains(field, value) =>
        // Not 100% sure what Spark's expectations are for regex, case insensitive, etc.
        // and not sure the relative efficiency of various Druid dim filters
        // Could also use a SearchQueryDimFilter here
        // new LikeDimFilter(field, s"%$value%", null, null)
        new RegexDimFilter(field, value, null, null)
      case StringStartsWith(field, value) =>
        // Not sure the trade-offs between LikeDimFilter and RegexDimFilter here
        // new LikeDimFilter(field, s"$value%", null, null, null)
        new RegexDimFilter(field, s"^$value", null, null)
      case StringEndsWith(field, value) =>
        // Not sure the trade-offs between LikeDimFilter and RegexDimFilter here
        // new LikeDimFilter(field, s"%$value", null, null, null)
        new RegexDimFilter(field, s"$value$$", null, null)
      case EqualTo(field, value) =>
        new SelectorDimFilter(field, value.toString, null, null)
      case LessThan(field, value) =>
        // Need to figure out how to do sorting here (e.g. strings should be lexicographic, nums should be numeric)
        // Perhaps moving this to the class and using schema + filter.references to guess at types?
        new BoundDimFilter(field, null, value.toString, false, true, null, null, null, null)
      case LessThanOrEqual(field, value) =>
        new BoundDimFilter(field, null, value.toString, false, false, null, null, null, null)
      case GreaterThan(field, value) =>
        new BoundDimFilter(field, value.toString, null, true, false, null, null, null, null)
      case GreaterThanOrEqual(field, value) =>
        new BoundDimFilter(field, value.toString, null, false, false, null, null, null, null)
    }
    // scalastyle:on
  }

  /**
    * Convert a Druid INPUTROW into a Spark InternalRow with schema SCHEMA.
    *
    * @param inputRow The Druid InputRow to convert into a Spark Row for loading into a dataframe.
    * @param schema The schema to map INPUTROW into.
    * @return A Spark InternalRow with schema SCHEMA and values parsed from INPUTROW.
    */
  def convertInputRowToSparkRow(inputRow: InputRow, schema: StructType): InternalRow = {
    InternalRow.fromSeq(schema.fieldNames.map { colName =>
      if (colName == "__time") {
        inputRow.getTimestampFromEpoch
      } else {
        val col = inputRow.getRaw(colName)
        if (col != null) {
          schema(colName).dataType match {
            case _: ArrayType =>
              val baseType = schema(colName).dataType.asInstanceOf[ArrayType].elementType
              col match {
                case collection: JCollection[_] =>
                  ArrayData.toArrayData(collection.asScala.map { elem =>
                    parseToScala(elem, baseType)
                  })
                case _ =>
                  // Single-element arrays won't be wrapped when read from Druid; need to do it here
                  ArrayData.toArrayData(List(parseToScala(col, baseType)))
              }
            case _ =>
              // This is slightly inefficient since some objects will already be the correct type
              parseToScala(col, schema(colName).dataType)
          }
        } else {
          null
        }
      }
    })
  }

  /**
    * Convert an object COL to the appropriate scala type for the given Spark DataType DT.
    *
    * @param col The object to convert to a suitable type.
    * @param dt The Spark DataType COL should be made compatible with.
    * @return COL parsed into a type compatible with DT.
    */
  def parseToScala(col: Any, dt: DataType): Any = {
    dt match {
      case StringType => UTF8String.fromString(col.toString)
      case LongType => col match {
        case _: java.lang.Long | Long => col
        case _: String => col.asInstanceOf[String].toLong
        case _ => throw new IllegalArgumentException(
          s"Unsure how to parse ${col.getClass.toString} into a Long!"
        )
      }
      case TimestampType => col // Timestamps should always come back from Druid as DateTimes
      case FloatType => col match {
        case _: java.lang.Float | Float => col
        case _: String => col.asInstanceOf[String].toFloat
        case _ => throw new IllegalArgumentException(
          s"Unsure how to parse ${col.getClass.toString} into a Float!"
        )
      }
      case DoubleType => col match {
        case _: java.lang.Double | Double => col
        case _: String => col.asInstanceOf[String].toDouble
        case _ => throw new IllegalArgumentException(
          s"Unsure how to parse ${col.getClass.toString} into a Double!"
        )
      }
      case BinaryType =>
        if (ComplexMetricRegistry.getRegisteredSerializedClasses.contains(col.getClass)) {
          ComplexMetricRegistry.deserialize(col)
        } else {
          col match {
            case arr: Array[Byte] =>
              arr
            case _ => throw new IllegalArgumentException(
              s"Unsure how to parse ${col.getClass.toString} into a ByteArray!"
            )
          }
        }
      case _ => throw new IllegalArgumentException(
        s"$dt currently unsupported!"
      )
    }
  }
}
