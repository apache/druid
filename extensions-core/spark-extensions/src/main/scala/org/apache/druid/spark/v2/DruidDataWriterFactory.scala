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

import java.util.{Map => JMap, Optional}

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling
import org.apache.druid.data.input.impl.{DimensionSchema, DimensionsSpec, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema, TimestampSpec}
import org.apache.druid.java.util.common.granularity.{Granularities, Granularity}
import org.apache.druid.java.util.common.{DateTimes, IAE}
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, DruidDataWriterConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType,
  StringType, StructType}

import scala.collection.JavaConverters.{mapAsScalaMapConverter, seqAsJavaListConverter}

/**
  * A factory to configure and create a DruidDataWiter from the given dataframe SCHEMA and options
  * DATASOURCEOPTIONS. Runs on the driver, whereas the created DruidDataWriter runs on an executor.
  *
  * @param schema The schema of the dataframe being written to Druid.
  * @param dataSourceOptionsMap Options to use to configure created DruidDataWriters.
  */
class DruidDataWriterFactory(
                              schema: StructType,
                              dataSourceOptionsMap: JMap[String, String]
                            ) extends DataWriterFactory[InternalRow] {
  private lazy val dataSourceOptions = new DataSourceOptions(dataSourceOptionsMap)
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long):
  DataWriter[InternalRow] = {
    val version = dataSourceOptions
      .get(DruidDataSourceOptionKeys.versionKey)
      .orElse(DateTimes.nowUtc().toString)

    val partitionIdToDruidPartitionsMap = DruidDataWriterFactory.scalifyOptional(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.partitionMapKey)
    ).map(serializedMap => MAPPER.readValue[Map[Int, Map[String, String]]](
      serializedMap, new TypeReference[Map[Int, Map[String, String]]] {}
    ))

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromOptions(dataSourceOptions, schema)

    new DruidDataWriter(
      new DruidDataWriterConfig(
        dataSourceOptions.tableName().get,
        partitionId,
        schema,
        MAPPER.writeValueAsString(dataSchema),
        dataSourceOptions.get(DruidDataSourceOptionKeys.shardSpecTypeKey).orElse("numbered"),
        dataSourceOptions.getInt(DruidDataSourceOptionKeys.rowsPerPersistKey, 2000000),
        dataSourceOptions.get(DruidDataSourceOptionKeys.deepStorageTypeKey).orElse("local"),
        dataSourceOptions.asMap.asScala.toMap,
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

  def createDataSchemaFromOptions(
                                   dataSourceOptions: DataSourceOptions,
                                   schema: StructType
                                 ): DataSchema = {
    val dimensionsArr = DruidDataWriterFactory.scalifyOptional(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.dimensionsKey)
    )
      .map(_.split(','))
      .getOrElse(Array.empty[String])
    val metrics = MAPPER.readValue[Array[AggregatorFactory]](
      dataSourceOptions.get(DruidDataSourceOptionKeys.metricsKey).orElse("[]"),
      new TypeReference[Array[AggregatorFactory]] {}
    )

    val excludedDimensions = DruidDataWriterFactory.scalifyOptional(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.excludedDimensionsKey)
    )
      .map(_.split(','))
      .getOrElse(Array.empty[String]).toSeq

    val dimensions = if (dimensionsArr.isEmpty) {
      schema.fieldNames.filterNot((metrics.map(_.getName) ++ excludedDimensions).contains(_))
    } else {
      dimensionsArr
    }

    new DataSchema(
      dataSourceOptions.tableName().get(),
      new TimestampSpec(
        dataSourceOptions.get(DruidDataSourceOptionKeys.timestampColumnKey).orElse("ts"),
        dataSourceOptions.get(DruidDataSourceOptionKeys.timestampFormatKey).orElse("auto"),
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
        DruidDataWriterFactory.scalifyOptional(
          dataSourceOptions.get(DruidDataSourceOptionKeys.segmentGranularity)
        )
          .map(Granularity.fromString)
          .getOrElse(Granularities.ALL),
        DruidDataWriterFactory.scalifyOptional(
          dataSourceOptions.get(DruidDataSourceOptionKeys.queryGranularity)
        )
          .map(Granularity.fromString)
          .getOrElse(Granularities.NONE),
        dataSourceOptions.getBoolean(DruidDataSourceOptionKeys.rollUpSegmentsKey, true),
        null // scalastyle:ignore null
      ),
      null, // scalastyle:ignore null
      null, // scalastyle:ignore null
      MAPPER
    )
  }

  // Needed to work around Java Function's type invariance
  def scalifyOptional[T](javaOptional: Optional[T]): Option[T] = {
    if (javaOptional.isPresent) Some(javaOptional.get()) else None
  }
}
