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

package org.apache.druid

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.{InjectableValues, Module, ObjectMapper}
import org.apache.druid.data.input.impl.DimensionSchema
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.java.util.common.granularity.GranularityType
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.metadata.PasswordProvider
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.query.expression.{LikeExprMacro, RegexpExtractExprMacro,
  TimestampCeilExprMacro, TimestampExtractExprMacro, TimestampFloorExprMacro,
  TimestampFormatExprMacro, TimestampParseExprMacro, TimestampShiftExprMacro, TrimExprMacro}
import org.apache.druid.segment.IndexSpec
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.model.DeepStorageConfig
import org.apache.druid.spark.partitioners.{HashBasedNumberedPartitioner, NumberedPartitioner,
  SingleDimensionPartitioner}
import org.apache.druid.spark.v2.DruidDataSourceV2ShortName
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder
import org.apache.druid.timeline.partition.HashPartitionFunction
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}

// We need to specify root here because org.apache.druid.java.util exists as well, and our package is org.apache.druid
import _root_.java.util.Properties
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.language.implicitConversions

package object spark {

  private[spark] val MAPPER: ObjectMapper = new DefaultObjectMapper()

  private val injectableValues: InjectableValues =
    new InjectableValues.Std()
      .addValue(classOf[ExprMacroTable], new ExprMacroTable(Seq(
        new LikeExprMacro(),
        new RegexpExtractExprMacro(),
        new TimestampCeilExprMacro(),
        new TimestampExtractExprMacro(),
        new TimestampFormatExprMacro(),
        new TimestampParseExprMacro(),
        new TimestampShiftExprMacro(),
        new TimestampFloorExprMacro(),
        new TrimExprMacro.BothTrimExprMacro(),
        new TrimExprMacro.LeftTrimExprMacro(),
        new TrimExprMacro.RightTrimExprMacro()).asJava))
      .addValue(classOf[ObjectMapper], MAPPER)
      .addValue(classOf[DataSegment.PruneSpecsHolder], PruneSpecsHolder.DEFAULT)

  MAPPER.setInjectableValues(injectableValues)

  /*
   * Utility methods for serializing and deserializing objects using the same object mapper used
   * internally to these connectors.
   */

  def serialize(obj: AnyRef): String = {
    MAPPER.writeValueAsString(obj)
  }

  def deserialize[T](json: String, typeReference: TypeReference[T]): T = {
    MAPPER.readValue[T](json, typeReference)
  }

  def registerModules(modules: Module*): ObjectMapper = {
    MAPPER.registerModules(modules: _*)
  }

  def registerSubType(subTypeClass: Class[_], name: String): Unit = {
    MAPPER.registerSubtypes(new NamedType(subTypeClass, name))
  }

  /*
   * Implicit classes and convertors to support more ergonomic configuration and use of these connectors.
   * These live in the package object for convenience and clarity when importing them in calling code.
   */

  implicit def druidDataFrameWriterToDataFrameWriter[T](druidWriter: DruidDataFrameWriter[T]): DataFrameWriter[T] =
    druidWriter.writer

  implicit def druidDataFrameReaderToDataFrameReader(druidReader: DruidDataFrameReader): DataFrameReader =
    druidReader.reader

  implicit def druidDataFrametoDataFrame(druidDf: DruidDataFrame): DataFrame = druidDf.df

  // scalastyle:off number.of.methods (There are more than 50 configurable options for the Writer)
  implicit class DruidDataFrameWriter[T](private[spark] val writer: DataFrameWriter[T]) {
    def druid(): Unit = {
      writer.format(DruidDataSourceV2ShortName).save()
    }

    def dataSource(dataSource: String): DataFrameWriter[T] = {
      writer.option(DruidConfigurationKeys.tableKey, dataSource)
    }

    def deepStorage(deepStorageConfig: DeepStorageConfig): DataFrameWriter[T] = {
      writer.options(deepStorageConfig.toOptions)
    }

    def dimensions(dimensions: Seq[DimensionSchema])(implicit d: DummyImplicit): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.dimensionsKey), MAPPER.writeValueAsString(dimensions))
    }

    def dimensions(dimensions: Seq[String]): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.dimensionsKey), MAPPER.writeValueAsString(dimensions))
    }

    def dimensions(dimensions: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.dimensionsKey), dimensions)
    }

    def excludedDimensions(excludedDimensions: Seq[String]): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.excludedDimensionsKey),
        MAPPER.writeValueAsString(excludedDimensions))
    }

    def excludedDimensions(excludedDimensions: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.excludedDimensionsKey), excludedDimensions)
    }

    def indexSpec(indexSpec: IndexSpec): DataFrameWriter[T] = {
      val bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory
      writer.option(indexPrefix(DruidConfigurationKeys.bitmapTypeKey), MAPPER.writeValueAsString(bitmapSerdeFactory))
      bitmapSerdeFactory match {
        case factory: RoaringBitmapSerdeFactory =>
          writer.option(indexPrefix(DruidConfigurationKeys.compressRunOnSerializationKey),
            factory.getCompressRunOnSerialization)
        case _ =>
      }
      writer.option(
        indexPrefix(DruidConfigurationKeys.dimensionCompressionKey),
        indexSpec.getDimensionCompression.toString
      )
      writer.option(indexPrefix(DruidConfigurationKeys.metricCompressionKey), indexSpec.getMetricCompression.toString)
      writer.option(indexPrefix(DruidConfigurationKeys.longEncodingKey), indexSpec.getLongEncoding.toString)
    }

    def indexSpec(indexSpecStr: String): DataFrameWriter[T] = {
      indexSpec(MAPPER.readValue[IndexSpec](indexSpecStr, new TypeReference[IndexSpec] {}))
    }

    def metadataBaseName(baseName: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataBaseNameKey), baseName)
    }

    def metadataDbcpProperties(properties: Properties): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataDbcpPropertiesKey),
        MAPPER.writeValueAsString(properties))
    }

    def metadataDbcpProperties(properties: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataDbcpPropertiesKey), properties)
    }

    def metadataDbType(dbType: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataDbTypeKey), dbType)
    }

    def metadataHost(host: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataHostKey), host)
    }

    def metadataPassword(password: PasswordProvider): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataPasswordKey), MAPPER.writeValueAsString(password))
    }

    def metadataPassword(password: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataPasswordKey), password)
    }

    def metadataPort(port: Int): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataPortKey), port)
    }

    def metadataUri(uri: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataConnectUriKey), uri)
    }

    def metadataUser(user: String): DataFrameWriter[T] = {
      writer.option(metadataPrefix(DruidConfigurationKeys.metadataUserKey), user)
    }

    def metrics(metrics: Array[AggregatorFactory]): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.metricsKey), MAPPER.writeValueAsString(metrics))
    }

    def metrics(metrics: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.metricsKey), metrics)
    }

    def partitionMap(partitionMap: Map[Int, Map[String, String]]): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.partitionMapKey), MAPPER.writeValueAsString(partitionMap))
    }

    def queryGranularity(queryGranularity: GranularityType): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.queryGranularityKey), queryGranularity.name)
    }

    def queryGranularity(queryGranularity: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.queryGranularityKey), queryGranularity)
    }

    def rationalizeSegments(rationalize: Boolean): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.rationalizeSegmentsKey), rationalize)
    }

    def rowsPerPersist(rowsPerPersist: Long): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.rowsPerPersistKey), rowsPerPersist)
    }

    def rollUp(shouldRollUp: Boolean): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.rollUpSegmentsKey), shouldRollUp)
    }

    def shardSpecType(shardSpecType: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.shardSpecTypeKey), shardSpecType)
    }

    def segmentGranularity(segmentGranularity: GranularityType): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.segmentGranularityKey), segmentGranularity.name)
    }

    def segmentGranularity(segmentGranularity: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.segmentGranularityKey), segmentGranularity)
    }

    def timestampColumn(timestampColumn: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.timestampColumnKey), timestampColumn)
    }

    def timestampFormat(timestampFormat: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.timestampFormatKey), timestampFormat)
    }

    def useCompactSketches(useCompactSketches: Boolean): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.useCompactSketchesKey), useCompactSketches)
    }

    def useDefaultValueForNull(useDefaultValueForNull: Boolean): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.useDefaultValueForNull), useDefaultValueForNull)
    }

    def version(version: String): DataFrameWriter[T] = {
      writer.option(writerPrefix(DruidConfigurationKeys.versionKey), version)
    }

    private def indexPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.indexSpecPrefix, key)
    }

    private def metadataPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.metadataPrefix, key)
    }

    private def writerPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.writerPrefix, key)
    }

    private def prefix( base: String, key: String): String = {
      Configuration.toKey(base, key)
    }
  }
  // scalastyle:on number.of.methods

  implicit class DruidDataFrameReader(private[spark] val reader: DataFrameReader) {
    def druid(): DataFrame = {
      reader.format(DruidDataSourceV2ShortName).load()
    }

    def batchSize(batchSize: Int): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.batchSizeKey), batchSize)
    }

    def brokerHost(host: String): DataFrameReader = {
      reader.option(brokerPrefix(DruidConfigurationKeys.brokerHostKey), host)
    }

    def brokerPort(port: Int): DataFrameReader = {
      reader.option(brokerPrefix(DruidConfigurationKeys.brokerPortKey), port)
    }

    def dataSource(dataSource: String): DataFrameReader = {
      reader.option(DruidConfigurationKeys.tableKey, dataSource)
    }

    def metadataBaseName(baseName: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataBaseNameKey), baseName)
    }

    def metadataDbcpProperties(properties: Properties): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataDbcpPropertiesKey),
        MAPPER.writeValueAsString(properties))
    }

    def metadataDbcpProperties(properties: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataDbcpPropertiesKey), properties)
    }

    def metadataDbType(dbType: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataDbTypeKey), dbType)
    }

    def metadataHost(host: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataHostKey), host)
    }

    def metadataPassword(password: PasswordProvider): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataPasswordKey), MAPPER.writeValueAsString(password))
    }

    def metadataPassword(password: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataPasswordKey), password)
    }

    def metadataPort(port: Int): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataPortKey), port)
    }

    def metadataUri(uri: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataConnectUriKey), uri)
    }

    def metadataUser(user: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataUserKey), user)
    }

    def numRetries(numRetries: Int): DataFrameReader = {
      reader.option(brokerPrefix(DruidConfigurationKeys.numRetriesKey), numRetries)
    }

    def retryWaitSeconds(retryWaitSeconds: Int): DataFrameReader = {
      reader.option(brokerPrefix(DruidConfigurationKeys.retryWaitSecondsKey), retryWaitSeconds)
    }

    def segments(segments: Seq[DataSegment]): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.segmentsKey), MAPPER.writeValueAsString(segments))
    }

    def segments(segments: Seq[String])(implicit d: DummyImplicit): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.segmentsKey), MAPPER.writeValueAsString(segments))
    }

    def segments(segments: String): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.segmentsKey), segments)
    }

    def timeoutMilliseconds(timeoutMilliseconds: Int): DataFrameReader = {
      reader.option(brokerPrefix(DruidConfigurationKeys.timeoutMillisecondsKey), timeoutMilliseconds)
    }

    def useCompactSketches(useCompactSketches: Boolean): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.useCompactSketchesKey), useCompactSketches)
    }

    def useDefaultValueForNull(useDefaultValueForNull: Boolean): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.useDefaultValueForNull), useDefaultValueForNull)
    }

    def vectorize(vectorize: Boolean): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.vectorizeKey), vectorize)
    }

    private def brokerPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.brokerPrefix, key)
    }

    private def metadataPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.metadataPrefix, key)
    }

    private def readerPrefix(key: String): String = {
      prefix(DruidConfigurationKeys.readerPrefix, key)
    }

    private def prefix( base: String, key: String): String = {
      Configuration.toKey(base, key)
    }
  }

  implicit class DruidDataFrame(private[spark] val df: DataFrame) extends Serializable {
    def hashPartitionAndWrite(
                               tsCol: String,
                               tsFormat: String,
                               segmentGranularity: String,
                               rowsPerPartition: Long,
                               partitionColsOpt: Option[Seq[String]],
                               shouldRollUp: Boolean = true,
                               hashFunc: HashPartitionFunction = HashPartitionFunction.MURMUR3_32_ABS
                             ): DataFrameWriter[Row] = {

      val partitioner = new HashBasedNumberedPartitioner(df)
      val partitionedDf = partitioner.partition(
        tsCol, tsFormat, segmentGranularity, rowsPerPartition, partitionColsOpt, shouldRollUp, hashFunc
      )

      partitionedDf
        .write
        .partitionMap(partitioner.getPartitionMap)
        .rationalizeSegments(false)
        .rollUp(shouldRollUp)
        .shardSpecType("hashed")
    }

    def rangePartitionAndWrite(
                                tsCol: String,
                                tsFormat: String,
                                segmentGranularity: String,
                                rowsPerPartition: Long,
                                partitionCol: String,
                                shouldRollUp: Boolean = true
                              ): DataFrameWriter[Row] = {
      val partitioner = new SingleDimensionPartitioner(df)
      val partitionedDf =
        partitioner.partition(tsCol, tsFormat, segmentGranularity, rowsPerPartition, partitionCol, shouldRollUp)

      partitionedDf
        .write
        .partitionMap(partitioner.getPartitionMap)
        .rationalizeSegments(false)
        .rollUp(shouldRollUp)
        .shardSpecType("single")
    }

    def partitionAndWrite(
                           tsCol: String,
                           tsFormat: String,
                           segmentGranularity: String,
                           rowsPerPartition: Long,
                           shouldRollUp: Boolean = true,
                           dimensions: Option[Seq[String]] = None
                         ): DataFrameWriter[Row] = {
      val partitioner = new NumberedPartitioner(df)
      val partitionedDf =
        partitioner.partition(tsCol, tsFormat, segmentGranularity, rowsPerPartition, shouldRollUp, dimensions)

      partitionedDf
        .write
        .partitionMap(partitioner.getPartitionMap)
        .rationalizeSegments(false)
        .rollUp(shouldRollUp)
        .shardSpecType("numbered")
    }
  }
}

