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

import org.apache.druid.data.input.impl.{DimensionSchema, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema}
import org.apache.druid.java.util.common.IAE
import org.apache.druid.java.util.common.granularity.Granularities
import org.apache.druid.spark.SparkFunSuite
import org.apache.druid.spark.utils.{Configuration, DruidConfigurationKeys}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, IntegerType,
  LongType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.asScalaBufferConverter


class DruidDataWriterFactorySuite extends SparkFunSuite with Matchers with DruidDataSourceV2TestUtils {
  private val expectedBaseDimensions = Seq[DimensionSchema](
    new StringDimensionSchema("dim1"),
    new StringDimensionSchema("dim2"),
    new StringDimensionSchema("id1"),
    new StringDimensionSchema("id2")
  )

  private val writerPropsWithDataSource = writerProps +
    (s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.tableKey}" -> dataSource)

  test("convertStructTypeToDruidDimensionSchema should convert dimensions from a well-formed StructType") {
    val updatedSchema = schema
      .add("id3", LongType)
      .add("dim3", FloatType)
      .add("dim4", DoubleType)
      .add("dim5", IntegerType)
    val updatedDimensions = dimensions.asScala ++ Seq("id3", "dim3", "dim4", "dim5")

    val dimensionSchema =
      DruidDataWriterFactory.convertStructTypeToDruidDimensionSchema(updatedDimensions, updatedSchema)

    val expectedDimensions = expectedBaseDimensions ++ Seq[DimensionSchema](
      new LongDimensionSchema("id3"),
      new FloatDimensionSchema("dim3"),
      new DoubleDimensionSchema("dim4"),
      new LongDimensionSchema("dim5")
    )

    dimensionSchema should contain theSameElementsInOrderAs expectedDimensions
  }

  test("convertStructTypeToDruidDimensionSchema should only process schema fields specified in dimensions") {
    val updatedSchema = schema
      .add("id3", LongType)
      .add("dim3", FloatType)
      .add("dim4", DoubleType)
      .add("dim5", IntegerType)
      .add("bin1", BinaryType) // Incompatible types are ok in the schema if they aren't dimensions

    val dimensionSchema =
      DruidDataWriterFactory.convertStructTypeToDruidDimensionSchema(dimensions.asScala, updatedSchema)

    dimensionSchema should contain theSameElementsInOrderAs expectedBaseDimensions
  }

  test("convertStructTypeToDruidDimensionSchema should error when incompatible Spark types are present") {
    val updatedSchema = schema
      .add("bin1", BinaryType)
    val updatedDimensions = dimensions.asScala :+ "bin1"

    an[IAE] should be thrownBy
      DruidDataWriterFactory.convertStructTypeToDruidDimensionSchema(updatedDimensions, updatedSchema)
  }

  test("createDataSchemaFromConfiguration should handle empty dimensions") {
    val updatedWriterProps = writerPropsWithDataSource -
      s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.dimensionsKey}"
    val writerConf = Configuration(updatedWriterProps).dive(DruidConfigurationKeys.writerPrefix)

    val dataSchema =DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)
    dataSchema.getDimensionsSpec.getDimensions.asScala should contain theSameElementsInOrderAs expectedBaseDimensions
  }

  test("createDataSchemaFromConfiguration should handle excluded dimensions") {
    val updatedWriterProps = (writerPropsWithDataSource -
      s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.dimensionsKey}") + (
      s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.excludedDimensionsKey}" -> "dim1"
    )
    val writerConf = Configuration(updatedWriterProps).dive(DruidConfigurationKeys.writerPrefix)

    val expectedDimensions = expectedBaseDimensions.tail

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)
    dataSchema.getDimensionsSpec.getDimensions.asScala should contain theSameElementsInOrderAs expectedDimensions
  }

  test("createDataSchemaFromConfiguration should handle comma-delimited dimensions") {
    val writerConf = Configuration(writerPropsWithDataSource).dive(DruidConfigurationKeys.writerPrefix)

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)
    dataSchema.getDimensionsSpec.getDimensions.asScala should contain theSameElementsInOrderAs expectedBaseDimensions
  }

  test("createDataSchemaFromConfiguration should handle bracketed dimensions") {
    val updatedWriterProps = writerPropsWithDataSource + (
      s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.dimensionsKey}" ->
        s"[${dimensions.asScala.mkString("\"", "\",\"", "\"")}]"
      )

    val writerConf = Configuration(updatedWriterProps).dive(DruidConfigurationKeys.writerPrefix)

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)
    dataSchema.getDimensionsSpec.getDimensions.asScala should contain theSameElementsInOrderAs expectedBaseDimensions
  }

  test("createDataSchemaFromConfiguration should handle DimensionSchemas") {
    val dimSchemaConf =
      """
        |[
        |  {
        |    "name": "testDim",
        |    "multiValueHandling": "SORTED_SET",
        |    "createBitmapIndex": false
        |  },
        |  {
        |    "name": "testFloatDim",
        |    "type": "float"
        |  },
        |  {
        |    "name": "quizDim",
        |    "multiValueHandling": "SORTED_ARRAY",
        |    "createBitmapIndex": true
        |  },
        |  {
        |    "name": "testLongDim",
        |    "type": "long"
        |  },
        |  "examDim"
        |]
        |""".stripMargin.trim

    val testSchema = StructType(Seq[StructField](
      StructField("testDim", StringType),
      StructField("testFloatDim", FloatType),
      StructField("quizDim", ArrayType(StringType)),
      StructField("testLongDim", LongType),
      StructField("examDim", StringType)
    ))

    val updatedWriterProps = writerPropsWithDataSource + (
      s"${DruidConfigurationKeys.writerPrefix}.${DruidConfigurationKeys.dimensionsKey}" -> dimSchemaConf)

    val writerConf = Configuration(updatedWriterProps).dive(DruidConfigurationKeys.writerPrefix)

    val expectedDimensions = Seq[DimensionSchema](
      new StringDimensionSchema("testDim", DimensionSchema.MultiValueHandling.SORTED_SET, false),
      new FloatDimensionSchema("testFloatDim"),
      new StringDimensionSchema("quizDim"),
      new LongDimensionSchema("testLongDim"),
      new StringDimensionSchema("examDim")
    )

    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, testSchema)
    dataSchema.getDimensionsSpec.getDimensions.asScala should contain theSameElementsInOrderAs expectedDimensions
  }

  test("createDataSchemaFromConfiguration should correctly parse timestampSpecs from Configurations") {
    val writerConf = Configuration(writerPropsWithDataSource).dive(DruidConfigurationKeys.writerPrefix)
    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)

    dataSchema.getTimestampSpec.getTimestampColumn should equal("__time")
    dataSchema.getTimestampSpec.getTimestampFormat should equal("auto")
  }

  test("createDataSchemaFromConfiguration should correctly parse granularitySpecs from Configurations") {
    val writerConf = Configuration(writerPropsWithDataSource).dive(DruidConfigurationKeys.writerPrefix)
    val dataSchema = DruidDataWriterFactory.createDataSchemaFromConfiguration(writerConf, schema)

    dataSchema.getGranularitySpec.getQueryGranularity should equal(Granularities.NONE)
    dataSchema.getGranularitySpec.getSegmentGranularity should equal(Granularities.DAY)
  }

  test("validateDimensionSpecAgainstSparkSchema should return true for valid dimension schemata") {
    val dimensions = Seq[DimensionSchema](
      new StringDimensionSchema("testStringDim"),
      new LongDimensionSchema("testLongDim"),
      new StringDimensionSchema("testStringDim2", DimensionSchema.MultiValueHandling.ARRAY, false),
      new FloatDimensionSchema("testFloatDim"),
      new DoubleDimensionSchema("testDoubleDim"),
      new LongDimensionSchema("testLongDim2")
    )

    val schema = StructType(Seq[StructField](
      StructField("testStringDim", ArrayType(StringType)),
      StructField("testLongDim", LongType),
      StructField("testStringDim2", StringType),
      StructField("testFloatDim", FloatType),
      StructField("tesDoubleDim", DoubleType),
      StructField("testLongDim2", IntegerType)
    ))

    DruidDataWriterFactory.validateDimensionSpecAgainstSparkSchema(dimensions, schema) should be(true)
  }

  test("validateDimensionSpecAgainstSparkSchema should throw an IAE for an invalid set of dimensions") {
    val dimensions = Seq[DimensionSchema](
      new StringDimensionSchema("testStringDim")
    )

    val schema = StructType(Seq[StructField](
      StructField("testStringDim", LongType)
    ))

    an[IAE] should be thrownBy DruidDataWriterFactory.validateDimensionSpecAgainstSparkSchema(dimensions, schema)
  }
}
