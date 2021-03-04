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
import org.apache.druid.data.input.impl.{DimensionSchema, DimensionsSpec, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema, TimestampSpec}
import org.apache.druid.java.util.common.granularity.Granularity
import org.apache.druid.java.util.common.{DateTimes, IAE}
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.utils.{Configuration, DruidConfigurationKeys, DruidDataWriterConfig,
  Logging}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType,
  StringType, StructType}

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
        conf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey),
        conf,
        version,
        partitionIdToDruidPartitionsMap
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
    if (dimensionsStr == DruidConfigurationKeys.dimensionsDefaultKey._2 || dimensionsStr.isEmpty) {
      val nonDimColumns = metrics ++ excludedDimensions :+ tsCol
      convertStructTypeToDruidDimensionSchema(schema.fieldNames.filterNot(nonDimColumns.contains(_)), schema)
    } else {
      // If dimensions were provided as a JSON list, parse them directly. Otherwise, infer types
      val parsedDims = if (dimensionsStr.startsWith("[")) {
        MAPPER.readValue[JList[DimensionSchema]](dimensionsStr, new TypeReference[JList[DimensionSchema]] {}).asScala
      } else {
        convertStructTypeToDruidDimensionSchema(dimensionsStr.split(","), schema)
      }
      validateDimensionSpecAgainstSparkSchema(parsedDims, schema)
      parsedDims
    }
  }

  /**
    * Given a list of column names DIMENSIONS and a struct type SCHEMA, returns a list of Druid DimensionSchemas
    * constructed from each column named in DIMENSIONS and the properties of the corresponding field in SCHEMA.
    *
    * @param dimensions A list of column names to construct Druid DimensionSchemas for.
    * @param schema The Spark schema to use to determine the types of the Druid DimensionSchemas created.
    * @return A list of DimensionSchemas generated from the type information in SCHEMA for each dimension in DIMENSIONS.
    */
  def convertStructTypeToDruidDimensionSchema(
                                               dimensions: Seq[String],
                                               schema: StructType
                                             ): Seq[DimensionSchema] = {
    val excessDimensions = dimensions.filterNot(schema.fieldNames.contains(_))
    if (excessDimensions.nonEmpty) {
      logWarn(s"Dimensions contains columns not in source df! Excess columns: ${excessDimensions.mkString(", ")}")
    }
    schema
      .filter(field => dimensions.contains(field.name))
      .map(field =>
        field.dataType match {
          case LongType | IntegerType => new LongDimensionSchema(field.name)
          case FloatType => new FloatDimensionSchema(field.name)
          case DoubleType => new DoubleDimensionSchema(field.name)
          case StringType | ArrayType(StringType, _) =>
            new StringDimensionSchema(field.name)
          case _ => throw new IAE(
            "Unsure how to create dimension from column [%s] with data type [%s]",
            field.name,
            field.dataType
          )
        }
      )
  }

  /**
    * Validates that the given list of DimensionSchemas align with the given Spark schema. This validation is done to
    * fail fast if the user-provided dimensions have different data types than the data in the source data frame to be
    * written.
    *
    * @param dimensions The list of DimensionSchemas to validate against SCHEMA.
    * @param schema The source-of-truth Spark schema to ensure compatibility with.
    * @throws IAE If the data types in DIMENSIONS do not align with the data types in SCHEMA.
    */
  def validateDimensionSpecAgainstSparkSchema(dimensions: Seq[DimensionSchema], schema: StructType): Boolean = {
    val incompatibilities = dimensions.flatMap{dim =>
      if (schema.fieldNames.contains(dim.getName)) {
        val sparkType = schema(schema.fieldIndex(dim.getName)).dataType
        sparkType match {
          case LongType | IntegerType =>
            if (dim.getTypeName != DimensionSchema.LONG_TYPE_NAME) {
              Some(s"${dim.getName}: expected type ${DimensionSchema.LONG_TYPE_NAME} but was ${dim.getTypeName}!")
            } else {
              None
            }
          case FloatType =>
            if (dim.getTypeName != DimensionSchema.FLOAT_TYPE_NAME) {
              Some(s"${dim.getName}: expected type ${DimensionSchema.FLOAT_TYPE_NAME} but was ${dim.getTypeName}!")
            } else {
              None
            }
          case DoubleType =>
            if (dim.getTypeName != DimensionSchema.DOUBLE_TYPE_NAME) {
              Some(s"${dim.getName}: expected type ${DimensionSchema.DOUBLE_TYPE_NAME} but was ${dim.getTypeName}!")
            } else {
              None
            }
          case StringType | ArrayType(StringType, _) =>
            if (dim.getTypeName != DimensionSchema.STRING_TYPE_NAME) {
              Some(s"${dim.getName}: expected type ${DimensionSchema.STRING_TYPE_NAME} but was ${dim.getTypeName}!")
            } else {
              None
            }
        }
      } else {
        None
      }
    }
    if (incompatibilities.nonEmpty) {
      throw new IAE(s"Incompatible dimensions spec provided! Offending columns: ${incompatibilities.mkString("; ")}")
    }
    // For now, the return type could just be Unit, but leaving the stubs in place for future improvement
    true
  }
}
