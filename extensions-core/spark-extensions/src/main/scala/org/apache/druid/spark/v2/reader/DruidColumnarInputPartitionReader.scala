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
import org.apache.druid.java.util.common.{FileUtils, IAE, ISE, StringUtils}
import org.apache.druid.query.dimension.DefaultDimensionSpec
import org.apache.druid.query.filter.DimFilter
import org.apache.druid.segment.column.ValueType
import org.apache.druid.segment.vector.{VectorColumnSelectorFactory, VectorCursor}
import org.apache.druid.segment.{QueryableIndex, QueryableIndexStorageAdapter, VirtualColumns}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.SerializableHadoopConfiguration
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.{ComplexMetricRegistry, SegmentReaderRegistry}
import org.apache.druid.spark.v2.INDEX_IO
import org.apache.druid.timeline.DataSegment
import org.apache.druid.utils.CompressionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, LongType, StringType,
  StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.io.{File, IOException}

class DruidColumnarInputPartitionReader(
                                         segmentStr: String,
                                         schema: StructType,
                                         filter: Option[DimFilter],
                                         columnTypes: Option[Set[String]],
                                         broadcastedConf: Broadcast[SerializableHadoopConfiguration],
                                         useCompactSketches: Boolean,
                                         useDefaultNullHandling: Boolean,
                                         batchSize: Int
                                       )
  extends InputPartitionReader[ColumnarBatch] with Logging {

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
  private val adapter = new QueryableIndexStorageAdapter(queryableIndex)

  private val cursor: VectorCursor = adapter.makeVectorCursor(
    filter.map(_.toOptimizedFilter).orNull,
    adapter.getInterval,
    VirtualColumns.EMPTY,
    false,
    batchSize,
    null) // scalastyle:ignore null

  private val columnVectors: Array[OnHeapColumnVector] = OnHeapColumnVector.allocateColumns(batchSize, schema)
  private val resultBatch: ColumnarBatch = new ColumnarBatch(columnVectors.map(_.asInstanceOf[ColumnVector]))

  override def next(): Boolean = {
    if (!cursor.isDone) {
      fillVectors()
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = {
    resultBatch
  }

  override def close(): Unit = {
    resultBatch.close()
    columnVectors.foreach(_.close())
    if (Option(cursor).nonEmpty){
      cursor.close()
    }
    if (Option(queryableIndex).nonEmpty) {
      queryableIndex.close()
    }
    if (Option(tmpDir).nonEmpty) {
      FileUtils.deleteDirectory(tmpDir)
    }
  }

  // TODO: Maybe ColumnProcessors can help here? Need to investigate
  private[reader] def fillVectors(): Unit = {
    columnVectors.foreach(_.reset())
    val selectorFactory = cursor.getColumnSelectorFactory

    schema.fields.zipWithIndex.foreach{case(col, i) =>
      val capabilities = selectorFactory.getColumnCapabilities(col.name)
      val columnVector = columnVectors(i)
      if (capabilities == null) { // scalastyle:ignore null
        fillNullVector(columnVector, col)
      } else {
        capabilities.getType match {
          case ValueType.FLOAT | ValueType.LONG | ValueType.DOUBLE =>
            fillNumericVector(capabilities.getType, selectorFactory, columnVector, col.name)
          case ValueType.STRING =>
            fillStringVector(selectorFactory, columnVector, col, capabilities.hasMultipleValues.isMaybeTrue)
          case ValueType.COMPLEX =>
            fillComplexVector(selectorFactory, columnVector, col)
          case _ => throw new IAE(s"Unrecognized ValueType ${capabilities.getType}!")
        }
      }
    }
    resultBatch.setNumRows(cursor.getCurrentVectorSize)
    cursor.advance()
  }

  /**
    * Fill a Spark ColumnVector with the values from a Druid VectorSelector containing numeric rows.
    * The general pattern is:
    *   1) If there are no null values in the Druid data, just copy the backing array over
    *   2) If there are nulls (the null vector is not itself null), for each index in the Druid vector check
    *        if the source vector is null at that index and if so insert the appropriate null value into the
    *        Spark vector. Otherwise, copy over the value at that index from the Druid vector.
    *
    * @param valueType The ValueType of the Druid column to fill COLUMNVECTOR from.
    * @param selectorFactory The Druid SelectorFactory backed by the data read in from segment files.
    * @param columnVector The Spark ColumnVector to fill with the data from SELECTORFACTORY.
    * @param name The name of the column in Druid to source data from.
    */
  private[reader] def fillNumericVector(
                                       valueType: ValueType,
                                       selectorFactory: VectorColumnSelectorFactory,
                                       columnVector: WritableColumnVector,
                                       name: String
                                       ): Unit = {
    val selector = selectorFactory.makeValueSelector(name)
    val vectorLength = selector.getCurrentVectorSize
    val nulls = selector.getNullVector

    valueType match {
      case ValueType.FLOAT =>
        val vector = selector.getFloatVector
        if (nulls == null) { // scalastyle:ignore null
          columnVector.putFloats(0, vectorLength, vector, 0)
        } else {
          (0 until vectorLength).foreach { i =>
            if (nulls(i)) {
              if (useDefaultNullHandling) {
                columnVector.putFloat(i, 0)
              } else {
                columnVector.putNull(i)
              }
            } else {
              columnVector.putFloat(i, vector(i))
            }
          }
        }
      case ValueType.LONG =>
        val vector = selector.getLongVector
        if (nulls == null) { // scalastyle:ignore null
          columnVector.putLongs(0, vectorLength, vector, 0)
        } else {
          (0 until vectorLength).foreach { i =>
            if (nulls(i)) {
              if (useDefaultNullHandling) {
                columnVector.putLong(i, 0)
              } else {
                columnVector.putNull(i)
              }
            } else {
              columnVector.putLong(i, vector(i))
            }
          }
        }
      case ValueType.DOUBLE =>
        val vector = selector.getDoubleVector
        if (nulls == null) { // scalastyle:ignore null
          columnVector.putDoubles(0, vectorLength, vector, 0)
        } else {
          (0 until vectorLength).foreach { i =>
            if (nulls(i)) {
              if (useDefaultNullHandling) {
                columnVector.putDouble(i, 0)
              } else {
                columnVector.putNull(i)
              }
            } else {
              columnVector.putDouble(i, vector(i))
            }
          }
        }
      case _ => throw new IAE(s"Must call fillNumericVector will a numeric value type; called with $valueType!")
    }
  }

  /**
    * Fill a Spark ColumnVector with the values from a Druid VectorSelector containing string rows.
    *
    * In theory, we could define a ColumnVector implementation that handled single- and multi-valued strings
    * intelligently while falling back to the existing behavior for other data types. Unfortunately, Spark marks
    * OnHeapColumnVector as final so we'd need to copy the underlying logic and maintain it ourselves or abuse
    * reflection. Additionally, Spark doesn't really do anything clever with columnar dataframes in 2.4. Specifically
    * for multi-valued string columns this means that under the hood Spark will immediately convert each sub-array
    * (e.g. row) into an Object[] and so we won't gain anything by maintaining the value dictionary. Instead, we define
    * a SingleValueDimensionDictionary to handle the single-valued case and reify multi-valued dimensions ourselves to
    * reduce complexity.
    *
    * There are also a couple of open questions to investigate:
    *
    * First, how does Spark expect nulls to be flagged from dictionaries? If dictionaries can happily return null, then
    * we can just drop the row vector in the dictionary creation and be on our way. If Spark expects nulls to be flagged
    * explicitly, then we'll need to figure out how the different Druid null handling strategies change both what gets
    * stored on disk and what we read here from the SingleValueDimensionSelector. In this case, based on
    * PossiblyNullDimensionSelector we'll likely need to iterate over the row vector returned by the selector and call
    * either putNull if the value at the index is 0 or putInt otherwise.
    *
    * Second, can Druid dictionaries change between parts of the segment file (i.e in different smooshes)? If they can,
    * we need to add checks for that case and fall back to putting byte arrays into the column vector directly for
    * single-valued dimensions.
    *
    * @param selectorFactory The Druid SelectorFactory backed by the data read in from segment files.
    * @param columnVector The Spark ColumnVector to fill with the data from SELECTORFACTORY.
    * @param column The Spark column schema we're filling.
    * @param maybeHasMultipleValues Whether or not the Druid column we're reading from may contain multiple values.
    */
  private[reader] def fillStringVector(
                                        selectorFactory: VectorColumnSelectorFactory,
                                        columnVector: WritableColumnVector,
                                        column: StructField,
                                        maybeHasMultipleValues: Boolean
                                      ): Unit = {
    if (maybeHasMultipleValues) {
      // Multi-valued string dimension that may contain multiple values in this batch
      val selector = selectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(column.name))
      val vector = selector.getRowVector
      val vectorLength =  selector.getCurrentVectorSize

      // This will store repeated strings multiple times. CPU should be more important than storage here, but
      // if the repeated strings are a problem and reducing the batch size doesn't help, we could implement our
      // own ColumnVector that tracks the row for each string in the lookup dict and then stores arrays of rowIds.
      // We'd need two vectors (the main ColumnVector, which would store an array of ints for each actual row id
      // and an arrayData column vector, which would store strings at each internal row id.) When we read in an
      // array of IndexedInts, we'd check to see if we'd already stored the corresponding string in arrayData and
      // if so just use the existing internal row. The ints in the main vector would point to the internal row ids
      // and we'd override ColumnVector#getArray(rowId: Int) to follow the logic on read. This would preserve the
      // space savings of the dictionary-encoding at the cost of possibly more CPU at read.

      val arrayData = columnVector.arrayData()
      // Note that offsets here are in rows, not bytes
      var columnVectorOffset = 0
      var arrayDataOffset = 0

      // Iterating over the populated elements of vector twice is faster than reserving additional capacity as
      // each new row is processed since reserving more capacity means copying arrays.
      val numberOfValuesInBatch = (0 until vectorLength).map(vector(_).size()).sum
      arrayData.reserve(numberOfValuesInBatch)

      (0 until vectorLength).foreach{i =>
        val arr = vector(i)
        if (arr == null) {
          // TODO: Is this possible? Need to test
          columnVector.putNull(i)
        } else {
          val numberOfValuesInRow = arr.size() // Number of values in this row
          (0 until numberOfValuesInRow).foreach { idx =>
            val id = arr.get(idx)
            val bytes = StringUtils.toUtf8(selector.lookupName(id))

            arrayData.putByteArray(arrayDataOffset, bytes)
            arrayDataOffset += 1
          }
          columnVector.putArray(i, columnVectorOffset, numberOfValuesInRow)
          columnVectorOffset += numberOfValuesInRow
        }
      }
    } else {
      // Multi-valued string dimension that does not contain multiple values in this batch
      val selector = selectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(column.name))
      val vector = selector.getRowVector
      val vectorLength = selector.getCurrentVectorSize

      if (column.dataType.isInstanceOf[ArrayType]) {
        // need to handle as if it were multi-dimensional so results are properly wrapped in arrays in spark
        val arrayData = columnVector.arrayData()

        // TODO: Work out null handling (see SingleValueDimensionDictionary as well)
        (0 until vectorLength).foreach{i =>
          val bytes = StringUtils.toUtf8(selector.lookupName(vector(i)))

          arrayData.putByteArray(i, bytes)
          columnVector.putArray(i, i,1)
        }
      } else {
        // Single-valued string dimension
        // TODO: There's got to be a better way to extract the lookups, but for now YOLO
        val cardinality = selector.getValueCardinality
        if (cardinality == -1) {
          throw new ISE("Encountered dictionary with unknown cardinality, vectorized reading not supported!")
        }
        val lookupMap = (0 until cardinality).map { id =>
          id -> selector.lookupName(id)
        }.toMap
        val colDict = new SingleValueDimensionDictionary(lookupMap)

        val dictionaryIds = columnVector.reserveDictionaryIds(vectorLength)
        dictionaryIds.appendInts(vectorLength, vector, 0)
        columnVector.setDictionary(colDict)
      }
    }
  }

  private[reader] def fillComplexVector(
                                         selectorFactory: VectorColumnSelectorFactory,
                                         columnVector: WritableColumnVector,
                                         column: StructField
                                       ): Unit = {
    val selector = selectorFactory.makeObjectSelector(column.name)
    val vector = selector.getObjectVector
    val vectorLength = selector.getCurrentVectorSize

    (0 until vectorLength).foreach{i =>
      val obj = vector(i)
      if (obj == null) { // scalastyle:ignore null
        columnVector.putNull(i)
      } else if (ComplexMetricRegistry.getRegisteredSerializedClasses.contains(obj.getClass)) {
        val bytes = ComplexMetricRegistry.deserialize(obj)
        columnVector.putByteArray(i, bytes)
      } else {
        obj match {
          case arr: Array[Byte] =>
            columnVector.putByteArray(i, arr)
          case _ => throw new IllegalArgumentException(
            s"Unable to parse ${column.getClass.toString} into a ByteArray! Try registering a ComplexMetric Plugin."
          )
        }
      }
    }
  }

  private[reader] def fillNullVector(columnVector: WritableColumnVector, column: StructField): Unit = {
    val vectorLength = cursor.getCurrentVectorSize
    if (useDefaultNullHandling) {
      column.dataType match {
        case FloatType =>
          columnVector.putFloats(0, vectorLength, 0)
        case LongType | TimestampType =>
          columnVector.putLongs(0, vectorLength, 0)
        case DoubleType =>
          columnVector.putDoubles(0, vectorLength, 0)
        case StringType =>
          (0 until vectorLength).foreach{i =>
            columnVector.putByteArray(i, Array.emptyByteArray)
          }
        case ArrayType(StringType, _) =>
          val arrayData = columnVector.arrayData()
          (0 until vectorLength).foreach{i =>
            arrayData.putByteArray(i, Array.emptyByteArray)
            columnVector.putArray(i, i,1)
          }
        case _ => // Complex Types use nulls regardless of null handling mode. Also nulling unknown types.
          columnVector.putNulls(0, vectorLength)
      }
    } else {
      columnVector.putNulls(0, cursor.getCurrentVectorSize)
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
