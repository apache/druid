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

import org.apache.druid.data.input.InputRow
import org.apache.druid.data.input.impl.{DimensionSchema, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema}
import org.apache.druid.java.util.common.IAE
import org.apache.druid.spark.registries.ComplexMetricRegistry
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DoubleType, FloatType,
  IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.{Collection => JCollection}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
  * Converters and utilities for working with Spark and Druid schemas.
  */
object SchemaUtils {
  /**
    * Convert a COLUMNMAP representing a Druid datasource's schema as returned by
    * DruidMetadataClient.getClient into a Spark StructType.
    *
    * @param columnMap The Druid schema to convert into a corresponding Spark StructType.
    * @return The StructType equivalent of the Druid schema described by COLUMNMAP.
    */
  def convertDruidSchemaToSparkSchema(columnMap: Map[String, (String, Boolean)]): StructType = {
    StructType.apply(
      columnMap.map { case (name, (colType, hasMultipleValues)) =>
        val sparkType = colType match {
          case "LONG" => LongType
          case "STRING" => StringType
          case "DOUBLE" => DoubleType
          case "FLOAT" => FloatType
          case "TIMESTAMP" => TimestampType
          case complexType: String if ComplexMetricRegistry.getRegisteredMetricNames.contains(complexType) =>
            BinaryType
          // Add other supported types later
          case _ => throw new IAE(s"Unrecognized type $colType!")
        }
        if (hasMultipleValues) {
          StructField(name, new ArrayType(sparkType, false))
        } else {
          StructField(name, sparkType)
        }
      }.toSeq
    )
  }

  /**
    * Convert a Druid INPUTROW into a Spark InternalRow with schema SCHEMA.
    *
    * @param inputRow The Druid InputRow to convert into a Spark Row for loading into a dataframe.
    * @param schema The schema to map INPUTROW into.
    * @param useDefaultNullHandling Whether to use the Druid defaults for null values or actual nulls.
    * @return A Spark InternalRow with schema SCHEMA and values parsed from INPUTROW.
    */
  def convertInputRowToSparkRow(
                                 inputRow: InputRow,
                                 schema: StructType,
                                 useDefaultNullHandling: Boolean
                               ): InternalRow = {
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
          if (useDefaultNullHandling) {
            schema(colName).dataType match {
              case StringType | ArrayType(StringType, _) => UTF8String.EMPTY_UTF8
              case LongType | IntegerType | FloatType | DoubleType | TimestampType => 0
              case _ => null // scalastyle:ignore null
            }
          } else {
            null // scalastyle:ignore null
          }
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
