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
import org.apache.druid.data.input.impl.{DimensionSchema, DimensionsSpec, TimestampSpec}
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.java.util.common.DateTimes
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys, DruidDataWriterConfig}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.utils.SchemaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

import java.util.{List => JList}
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

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
        writerConf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey),
        conf,
        version,
        partitionIdToDruidPartitionsMap,
        writerConf.getBoolean(DruidConfigurationKeys.useCompactSketchesDefaultKey),
        writerConf.getBoolean(DruidConfigurationKeys.useDefaultValueForNullDefaultKey)
      )
    )
  }
}

private[v2] object DruidDataWriterFactory extends Logging {
  def createDataSchemaFromConfiguration(
                                   conf: Configuration,
                                   schema: StructType
                                 ): DataSchema = {
    val dimensionsStr = conf.get(DruidConfigurationKeys.dimensionsDefaultKey)
    val metrics = MAPPER.readValue[Array[AggregatorFactory]](
      conf.get(DruidConfigurationKeys.metricsDefaultKey),
      new TypeReference[Array[AggregatorFactory]] {}
    )

    val excludedDimensions = conf.get(DruidConfigurationKeys.excludedDimensionsKey)
      .fold(Array.empty[String])(_.split(','))
      .toSeq

    val tsCol = conf.get(DruidConfigurationKeys.timeStampColumnDefaultKey)

    val dimensions = generateDimensions(dimensionsStr, metrics.map(_.getName), excludedDimensions, tsCol, schema)

    new DataSchema(
      conf.getString(DruidConfigurationKeys.tableKey),
      new TimestampSpec(
        tsCol,
        conf.get(DruidConfigurationKeys.timestampFormatDefaultKey),
        null // scalastyle:ignore null
      ),
      new DimensionsSpec(
        dimensions.asJava,
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

  /**
    * Given a string representation of a list of dimensions (either empty, comma-separated names, or a JSON list of
    * DimensionSchemas), a list of metric column names, a list of excluded dimensions, a timestamp column name, and a
    * Spark schema, return a validated list of DimensionSchemas for a data frame with schema SCHEMA.
    *
    * @param dimensionsStr A string representing a list of Druid dimensions. If the list is empty, dimensions will be
    *                      inferred using METRICS, EXCLUDEDDIMENSIONS, and TSCOL.
    * @param metrics A list of metric column names. Used to infer dimensions if DIMENSIONSSTR does not provide a list.
    * @param excludedDimensions A list of dimensions to exclude from the generated list of dimensions. Only used if
    *                           DIMENSIONSTR does not provide a list.
    * @param tsCol The timestamp column for the source data. Used to infer dimensions if DIMENSIONSSTR does not provide
    *              a list.
    * @param schema The schema of the source data frame we're writing. Used to infer dimensions if DIMENSIONSSTR does
    *               not provide a list and used to validate the dimenions if DIMENSIONSSTR does provide a list.
    * @return The list of Druid DimensionSchemas to use when writing the source data frame as Druid segments.
    */
  def generateDimensions(
                          dimensionsStr: String,
                          metrics: Seq[String],
                          excludedDimensions: Seq[String],
                          tsCol: String,
                          schema: StructType
                        ): Seq[DimensionSchema] = {
    // No dimensions provided, assume all non-excluded non-metric columns are dimensions.
    if (dimensionsStr == DruidConfigurationKeys.dimensionsDefaultKey._2 || dimensionsStr.isEmpty) {
      val nonDimColumns = metrics ++ excludedDimensions :+ tsCol
      val dimensions = schema.fieldNames.filterNot(nonDimColumns.contains(_))

      val excessDimensions = dimensions.filterNot(schema.fieldNames.contains(_))
      if (excessDimensions.nonEmpty) {
        logWarn(s"Dimensions contains columns not in source df! Excess columns: ${excessDimensions.mkString(", ")}")
      }

      SchemaUtils.convertStructTypeToDruidDimensionSchema(dimensions, schema)
    } else {
      // If dimensions were provided as a JSON list, parse them directly. Otherwise, infer types
      val parsedDims = if (dimensionsStr.startsWith("[")) {
        MAPPER.readValue[JList[DimensionSchema]](dimensionsStr, new TypeReference[JList[DimensionSchema]] {}).asScala
      } else {
        val dimensions = dimensionsStr.split(",")

        val excessDimensions = dimensions.filterNot(schema.fieldNames.contains(_))
        if (excessDimensions.nonEmpty) {
          logWarn(s"Dimensions contains columns not in source df! Excess columns: ${excessDimensions.mkString(", ")}")
        }

        SchemaUtils.convertStructTypeToDruidDimensionSchema(dimensions, schema)
      }
      SchemaUtils.validateDimensionSpecAgainstSparkSchema(parsedDims, schema)
      parsedDims
    }
  }
}
