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
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.metadata.DynamicConfigProvider
import org.apache.druid.query.expression.{LikeExprMacro, RegexpExtractExprMacro,
  TimestampCeilExprMacro, TimestampExtractExprMacro, TimestampFloorExprMacro,
  TimestampFormatExprMacro, TimestampParseExprMacro, TimestampShiftExprMacro, TrimExprMacro}
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys}
import org.apache.druid.spark.model.DeepStorageConfig
import org.apache.druid.spark.v2.DruidDataSourceV2ShortName
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import _root_.java.util.Properties
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.language.implicitConversions

package object spark {
  private[spark] val MAPPER: ObjectMapper = new DefaultObjectMapper()

  private[spark] val baseInjectableValues: InjectableValues.Std =
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

  MAPPER.setInjectableValues(baseInjectableValues)

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

  def setInjectableValue(clazz: Class[_], value: AnyRef): Unit = {
    MAPPER.setInjectableValues(baseInjectableValues.addValue(clazz, value))
  }

  implicit def druidDataFrameReaderToDataFrameReader(druidReader: DruidDataFrameReader): DataFrameReader =
    druidReader.reader

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

    def deepStorage(deepStorageConfig: DeepStorageConfig): DataFrameReader = {
      reader.options(deepStorageConfig.toOptions)
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

    def metadataPassword(provider: DynamicConfigProvider[String], confKey: String): DataFrameReader = {
      reader.option(metadataPrefix(DruidConfigurationKeys.metadataPasswordKey),
        provider.getConfig.getOrDefault(confKey, ""))
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

    def useSparkConfForDeepStorage(useSparkConfForDeepStorage: Boolean): DataFrameReader = {
      reader.option(readerPrefix(DruidConfigurationKeys.useSparkConfForDeepStorageKey), useSparkConfForDeepStorage)
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
}
