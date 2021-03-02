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

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling
import org.apache.druid.data.input.impl.{DimensionSchema, DimensionsSpec, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema, TimestampSpec}
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.java.util.common.{DateTimes, IAE}
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.utils.{Configuration, DruidConfigurationKeys,
  DruidDataWriterConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType,
  StringType, StructType}

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
  * A factory to configure and create a DruidDataWiter from the given dataframe SCHEMA and conf
  * CONF. Runs on the driver, whereas the created DruidDataWriter runs on an executor.
  *
  * @param schema The schema of the dataframe being written to Druid.
  * @param conf Configuration properties to use to configure created DruidDataWriters.
  */
class DruidDataWriterFactory(
                              schema: StructType,
                              conf: Configuration
                            ) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long):
  DataWriter[InternalRow] = {
    // The table name isn't name spaced since the key is special in DataSourceOptions, so we add it to the sub-conf here
    val writerConf = conf
      .dive(DruidConfigurationKeys.writerPrefix)
      .merge(
        Configuration.fromKeyValue(DruidConfigurationKeys.tableKey, conf.getString(DruidConfigurationKeys.tableKey))
      )
    val version = writerConf.get(DruidConfigurationKeys.versionKey, DateTimes.nowUtc().toString)

    val partitionIdToDruidPartitionsMap = writerConf
      .get(DruidConfigurationKeys.partitionMapKey)
      .map(serializedMap =>
        MAPPER.readValue[Map[Int, Map[String, String]]](
          serializedMap, new TypeReference[Map[Int, Map[String, String]]] {}
        )
      )

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)

    new DruidDataWriter(
      new DruidDataWriterConfig(
        conf.getString(DruidConfigurationKeys.tableKey),
        partitionId,
        schema,
        MAPPER.writeValueAsString(dataSchema),
        writerConf.get(DruidConfigurationKeys.shardSpecTypeDefaultKey),
        writerConf.getInt(DruidConfigurationKeys.rowsPerPersistDefaultKey),
        conf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey),
        conf,
        version,
        partitionIdToDruidPartitionsMap
      )
    )
  }
}

object DruidDataWriterFactory {
  def convertStructTypeToDruidDimensionSchema(
                                               dimensions: Seq[String],
                                               schema: StructType
                                             ): Seq[DimensionSchema] = {
    schema
      .filter(field => dimensions.contains(field.name))
      .map(field =>
        field.dataType match {
          case LongType | IntegerType => new LongDimensionSchema(field.name)
          case FloatType => new FloatDimensionSchema(field.name)
          case DoubleType => new DoubleDimensionSchema(field.name)
          case StringType | ArrayType(StringType, false) => new StringDimensionSchema(
            field.name,
            MultiValueHandling.SORTED_ARRAY, // TODO: Make this configurable
            true // TODO: Make this configurable
          )
          case _ => throw new IAE(
            "Unsure how to create dimension from column [%s] with data type [%s]",
            field.name,
            field.dataType
          )
        }
      )
  }

  def createDataSchemaFromConfiguration(
                                   conf: Configuration,
                                   schema: StructType
                                 ): DataSchema = {
    val dimensionsArr = conf.get(DruidConfigurationKeys.dimensionsKey).fold(Array.empty[String])(_.split(','))
    val metrics = MAPPER.readValue[Array[AggregatorFactory]](
      conf.get(DruidConfigurationKeys.metricsDefaultKey),
      new TypeReference[Array[AggregatorFactory]] {}
    )

    val excludedDimensions = conf.get(DruidConfigurationKeys.excludedDimensionsKey)
      .fold(Array.empty[String])(_.split(','))
      .toSeq

    val dimensions = if (dimensionsArr.isEmpty) {
      schema.fieldNames.filterNot((metrics.map(_.getName) ++ excludedDimensions).contains(_))
    } else {
      dimensionsArr
    }

    new DataSchema(
      conf.getString(DruidConfigurationKeys.tableKey),
      new TimestampSpec(
        conf.get(DruidConfigurationKeys.timeStampColumnDefaultKey),
        conf.get(DruidConfigurationKeys.timestampFormatDefaultKey),
        null // scalastyle:ignore null
      ),
      new DimensionsSpec(
        DruidDataWriterFactory.convertStructTypeToDruidDimensionSchema(
          dimensions,
          schema
        ).asJava,
        excludedDimensions.asJava,
        null // scalastyle:ignore null
      ),
      metrics,
      new UniformGranularitySpec(
        Granularity.fromString(conf.get(DruidConfigurationKeys.segmentGranularityDefaultKey)),
        Granularity.fromString(conf.get(DruidConfigurationKeys.queryGranularityDefaultKey)),
        conf.getBoolean(DruidConfigurationKeys.rollUpSegmentsDefaultKey),
        null // scalastyle:ignore null
      ),
      null, // scalastyle:ignore null
      null, // scalastyle:ignore null
      MAPPER
    )
  }
}
