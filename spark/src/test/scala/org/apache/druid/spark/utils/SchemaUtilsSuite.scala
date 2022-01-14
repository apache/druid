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

package org.apache.druid.spark.utils

import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.data.input.impl.{DimensionSchema, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema}
import org.apache.druid.java.util.common.IAE
import org.apache.druid.spark.registries.ComplexTypeRegistry
import org.apache.druid.spark.v2.DruidDataSourceV2TestUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, IntegerType,
  LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}

class SchemaUtilsSuite extends AnyFunSuite with Matchers with DruidDataSourceV2TestUtils {
  private val expectedBaseDimensions = Seq[DimensionSchema](
    new StringDimensionSchema("dim1"),
    new StringDimensionSchema("dim2"),
    new StringDimensionSchema("id1"),
    new StringDimensionSchema("id2")
  )

  test("convertDruidSchemaToSparkSchema should convert a Druid schema") {
    val columnMap = Map[String, (String, Boolean)](
      "__time" -> ("LONG", false),
      "dim1" -> ("STRING", true),
      "dim2" -> ("STRING", false),
      "id1" -> ("STRING", false),
      "id2" -> ("STRING", false),
      "count" -> ("LONG", false),
      "sum_metric1" -> ("LONG", false),
      "sum_metric2" -> ("LONG", false),
      "sum_metric3" -> ("DOUBLE", false),
      "sum_metric4" -> ("FLOAT", false),
      "uniq_id1" -> ("thetaSketch", false)
    )

    ComplexTypeRegistry.registerByName("thetaSketch", false)
    val actualSchema = SchemaUtils.convertDruidSchemaToSparkSchema(columnMap)
    actualSchema.fields should contain theSameElementsAs schema.fields
  }

  test("convertInputRowToSparkRow should convert an InputRow to an InternalRow") {
    val timestamp = 0L
    val dimensions = List[String]("dim1", "dim2", "dim3", "dim4", "dim5", "dim6", "dim7", "dim8")
    val event = Map[String, Any](
      "dim1" -> 1L,
      "dim2" -> "false",
      "dim3" -> "str",
      "dim4" -> List[String]("val1", "val2").asJava,
      "dim5" -> 4.2,
      "dim6" -> List[Long](12L, 26L).asJava,
      "dim7" -> "12345",
      "dim8" -> "12"
    ).map{case(k, v) =>
      // Gotta do our own auto-boxing in Scala
      k -> v.asInstanceOf[AnyRef]
    }
    val inputRow: MapBasedInputRow =
      new MapBasedInputRow(timestamp, dimensions.asJava, event.asJava)
    val schema = StructType(Seq(
      StructField("__time", TimestampType),
      StructField("dim1", LongType),
      StructField("dim2", StringType),
      StructField("dim3", StringType),
      StructField("dim4", ArrayType(StringType, false)),
      StructField("dim5", DoubleType),
      StructField("dim6", ArrayType(LongType, false)),
      StructField("dim7", LongType),
      StructField("dim8", ArrayType(LongType, false))
    ))
    val res = SchemaUtils.convertInputRowToSparkRow(inputRow, schema, false)
    val expected = InternalRow.fromSeq(
      Seq(
        0,
        1L,
        UTF8String.fromString("false"),
        UTF8String.fromString("str"),
        ArrayData.toArrayData(Array(UTF8String.fromString("val1"), UTF8String.fromString("val2"))),
        4.2,
        ArrayData.toArrayData(Seq(12L, 26L)),
        12345,
        ArrayData.toArrayData(Seq(12L))
      )
    )
    for (i <- 0 until schema.length) {
      res.get(i, schema(i).dataType) should equal(expected.get(i, schema(i).dataType))
    }
  }

  test("convertInputRowToSparkRow should return null for missing dimensions") {
    val timestamp = 0L
    val dimensions = List[String]("dim1", "dim2", "dim3", "dim4", "dim5")
    val event = Map[String, Any](
      "dim1" -> 1L,
      "dim2" -> "false",
      "dim3" -> "str"
    ).map{case(k, v) =>
      // Gotta do our own auto-boxing in Scala
      k -> v.asInstanceOf[AnyRef]
    }
    val inputRow: MapBasedInputRow =
      new MapBasedInputRow(timestamp, dimensions.asJava, event.asJava)
    val schema = StructType(Seq(
      StructField("__time", TimestampType),
      StructField("dim1", LongType),
      StructField("dim2", StringType),
      StructField("dim3", StringType),
      StructField("dim4", ArrayType(StringType, false)),
      StructField("dim5", LongType)
    ))
    val defaultNullHandlingRes = SchemaUtils.convertInputRowToSparkRow(inputRow, schema, true)
    val defaultNullExpected = InternalRow.fromSeq(
      Seq(
        0,
        1L,
        UTF8String.fromString("false"),
        UTF8String.fromString("str"),
        UTF8String.fromString(""),
        0
      )
    )
    for (i <- 0 until schema.length) {
      defaultNullHandlingRes.get(i, schema(i).dataType) should equal(defaultNullExpected.get(i, schema(i).dataType))
    }


    val sqlNullHandlingRes = SchemaUtils.convertInputRowToSparkRow(inputRow, schema, false)
    val sqlNullExpected = InternalRow.fromSeq(
      Seq(
        0,
        1L,
        UTF8String.fromString("false"),
        UTF8String.fromString("str"),
        null, // scalastyle:ignore null
        null // scalastyle:ignore null
      )
    )
    for (i <- 0 until schema.length) {
      sqlNullHandlingRes.get(i, schema(i).dataType) should equal(sqlNullExpected.get(i, schema(i).dataType))
    }
  }


  test("convertStructTypeToDruidDimensionSchema should convert dimensions from a well-formed StructType") {
    val updatedSchema = schema
      .add("id3", LongType)
      .add("dim3", FloatType)
      .add("dim4", DoubleType)
      .add("dim5", IntegerType)
    val updatedDimensions = dimensions.asScala ++ Seq("id3", "dim3", "dim4", "dim5")

    val dimensionSchema =
      SchemaUtils.convertStructTypeToDruidDimensionSchema(updatedDimensions, updatedSchema)

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
      SchemaUtils.convertStructTypeToDruidDimensionSchema(dimensions.asScala, updatedSchema)

    dimensionSchema should contain theSameElementsInOrderAs expectedBaseDimensions
  }

  test("convertStructTypeToDruidDimensionSchema should error when incompatible Spark types are present") {
    val updatedSchema = schema
      .add("bin1", BinaryType)
    val updatedDimensions = dimensions.asScala :+ "bin1"

    an[IAE] should be thrownBy
      SchemaUtils.convertStructTypeToDruidDimensionSchema(updatedDimensions, updatedSchema)
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

    SchemaUtils.validateDimensionSpecAgainstSparkSchema(dimensions, schema) should be(true)
  }

  test("validateDimensionSpecAgainstSparkSchema should throw an IAE for an invalid set of dimensions") {
    val dimensions = Seq[DimensionSchema](
      new StringDimensionSchema("testStringDim")
    )

    val schema = StructType(Seq[StructField](
      StructField("testStringDim", LongType)
    ))

    an[IAE] should be thrownBy SchemaUtils.validateDimensionSpecAgainstSparkSchema(dimensions, schema)
  }
}
