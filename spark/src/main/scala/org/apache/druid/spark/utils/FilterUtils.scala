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

import org.apache.druid.java.util.common.{ISE, JodaUtils}
import org.apache.druid.query.filter.{AndDimFilter, BoundDimFilter, DimFilter, FalseDimFilter,
  InDimFilter, NotDimFilter, OrDimFilter, RegexDimFilter, SelectorDimFilter}
import org.apache.druid.query.ordering.{StringComparator, StringComparators}
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan,
  GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains,
  StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType,
  LongType, StringType, StructType, TimestampType}

import scala.collection.JavaConverters.{seqAsJavaListConverter, setAsJavaSetConverter}

/**
  * Converters and utilities for working with Spark and Druid Filters.
  */
object FilterUtils {
  /**
    * Map an array of Spark filters FILTERS to a Druid dim filter or None if filters is empty.
    *
    * We return a DimFilter instead of a Filter and force callers to call .toFilter
    * or .toOptimizedFilter to get a filter because callers can't covert back to a DimFilter from a
    * Filter.
    *
    * @param filters The spark filters to map to a Druid filter.
    * @return A Druid filter corresponding to the union of filter conditions enumerated in FILTERS.
    */
  def mapFilters(filters: Array[Filter], schema: StructType): Option[DimFilter] = {
    if (filters.isEmpty) {
      Option.empty[DimFilter]
    } else {
      Some(new AndDimFilter(filters.map(mapFilter(_, schema)).toList.asJava).optimize())
    }
  }

  /**
    * Convert a Spark-style filter FILTER to a Druid-style filter.
    *
    * @param filter The Spark filter to map to a Druid filter.
    * @return The Druid filter corresponding to the filter condition described by FILTER.
    */
  def mapFilter(filter: Filter, schema: StructType): DimFilter = { // scalastyle:ignore method.length
    // scalastyle:off null
    filter match {
      case And(left, right) =>
        new AndDimFilter(List(mapFilter(left, schema), mapFilter(right, schema)).asJava)
      case Or(left, right) =>
        new OrDimFilter(List(mapFilter(left, schema), mapFilter(right, schema)).asJava)
      case Not(condition) =>
        new NotDimFilter(mapFilter(condition, schema))
      case IsNull(field) =>
        new SelectorDimFilter(field, null, null, null)
      case IsNotNull(field) => new NotDimFilter(new SelectorDimFilter(field, null, null, null))
      case In(field, values) =>
        new InDimFilter(field, values.filter(_ != null).map(_.toString).toSet.asJava, null, null)
      case StringContains(field, value) =>
        // Not 100% sure what Spark's expectations are for regex, case insensitive, etc.
        // and not sure the relative efficiency of various Druid dim filters
        // Could also use a SearchQueryDimFilter here
        // new LikeDimFilter(field, s"%$value%", null, null)
        new RegexDimFilter(field, value, null, null)
      case StringStartsWith(field, value) =>
        // Not sure the trade-offs between LikeDimFilter and RegexDimFilter here
        // new LikeDimFilter(field, s"$value%", null, null, null)
        new RegexDimFilter(field, s"^$value", null, null)
      case StringEndsWith(field, value) =>
        // Not sure the trade-offs between LikeDimFilter and RegexDimFilter here
        // new LikeDimFilter(field, s"%$value", null, null, null)
        new RegexDimFilter(field, s"$value$$", null, null)
      case EqualTo(field, value) =>
        if (value == null) {
          FalseDimFilter.instance()
        } else {
          new SelectorDimFilter(field, value.toString, null, null)
        }
      case EqualNullSafe(field, value) =>
        new SelectorDimFilter(field, Option(value).map(_.toString).orNull, null, null)
      case LessThan(field, value) =>
        new BoundDimFilter(
          field,
          null,
          value.toString,
          false,
          true,
          null,
          null,
          getOrderingFromDataType(schema(field).dataType),
          null
        )
      case LessThanOrEqual(field, value) =>
        new BoundDimFilter(
          field,
          null,
          value.toString,
          false,
          false,
          null,
          null,
          getOrderingFromDataType(schema(field).dataType),
          null
        )
      case GreaterThan(field, value) =>
        new BoundDimFilter(
          field,
          value.toString,
          null,
          true,
          false,
          null,
          null,
          getOrderingFromDataType(schema(field).dataType),
          null
        )
      case GreaterThanOrEqual(field, value) =>
        new BoundDimFilter(
          field,
          value.toString,
          null,
          false,
          false,
          null,
          null,
          getOrderingFromDataType(schema(field).dataType),
          null
        )
    }
    // scalastyle:on
  }

  private[utils] def getOrderingFromDataType(dataType: DataType): StringComparator = {
    dataType match {
      case LongType | IntegerType | DoubleType | FloatType => StringComparators.NUMERIC
      case StringType | ArrayType(StringType, _) => StringComparators.LEXICOGRAPHIC
      // Filters on complex types should return false when evaluated in isSupportedFilter, something's gone wrong
      case _ => throw new ISE("This reader doesn't support filtering on complex types! Complex type " +
        "filters should not be pushed down.")
    }
  }

  /**
    * Given a Spark filter and a target DataFrame schema, returns true iff the filter can be pushed down to the Druid
    * InputPartitionReaders. Since segments are pulled from deep storage before being filtered, this is not as useful
    * as it could be but still saves time and resources.
    *
    * @param filter The filter to evaluate for support.
    * @param schema The schema of the DataFrame to be filtered by FILTER.
    * @return True iff FILTER can be pushed down to the InputPartitionReaders.
    */
  private[spark] def isSupportedFilter(
                                        filter: Filter,
                                        schema: StructType,
                                        useSQLCompatibleNulls: Boolean = false
                                      ): Boolean = {
    // If the filter references columns we don't know about, we can't push it down
    if (!filter.references.forall(schema.fieldNames.contains(_))) {
      false
    } else {
      filter match {
        // scalastyle:off null
        case and: And => isSupportedFilter(and.left, schema, useSQLCompatibleNulls) &&
          isSupportedFilter(and.right, schema, useSQLCompatibleNulls)
        case or: Or => isSupportedFilter(or.left, schema, useSQLCompatibleNulls) &&
          isSupportedFilter(or.right, schema, useSQLCompatibleNulls)
        case not: Not => isSupportedFilter(not.child, schema, useSQLCompatibleNulls)
        // If we're using SQL-compatible nulls, we can filter for null.
        // Otherwise, callers should explictly filter for '' or 0 depending on the column type.
        // If we ever support pushing down filters on complex types, we'll need to add handling here.
        case _: IsNull => useSQLCompatibleNulls
        case _: IsNotNull => useSQLCompatibleNulls
        case in: In => checkAllDataTypesSupported(filter, schema) &&
          (useSQLCompatibleNulls || !in.values.contains(null))
        case _: StringContains => checkStringsOnly(filter, schema)
        case _: StringStartsWith => checkStringsOnly(filter, schema)
        case _: StringEndsWith => checkStringsOnly(filter, schema)
        // Hopefully Spark is smart enough to short-circuit for foo = NULL queries but if not, I guess we can
        case equalTo: EqualTo => checkAllDataTypesSupported(filter, schema) &&
          (useSQLCompatibleNulls || equalTo.value != null)
        case equalNullSafe: EqualNullSafe => checkAllDataTypesSupported(filter, schema) &&
          (useSQLCompatibleNulls || equalNullSafe.value != null)
        case _: LessThan => checkAllDataTypesSupported(filter, schema)
        case _: LessThanOrEqual => checkAllDataTypesSupported(filter, schema)
        case _: GreaterThan => checkAllDataTypesSupported(filter, schema)
        case _: GreaterThanOrEqual => checkAllDataTypesSupported(filter, schema)
        case _ => false
        // scalastyle:on
      }
    }
  }

  private def checkAllDataTypesSupported(filter: Filter, schema: StructType): Boolean = {
    filter.references.map{field =>
      schema(schema.fieldIndex(field)).dataType
    }.forall(supportedDataTypesForFiltering.contains(_))
  }

  private def checkStringsOnly(filter: Filter, schema: StructType): Boolean = {
    filter.references.map{field =>
      schema(schema.fieldIndex(field)).dataType == StringType
    }.forall(identity)
  }

  private val supportedDataTypesForFiltering: Seq[DataType] = Seq(
    IntegerType, LongType, FloatType, DoubleType, TimestampType, StringType
  )

  private val emptyBoundSeq = Seq.empty[(Bound, Long)]

  /**
    * Given an array of Spark filters, return upper and lower bounds on the value of the __time column if bounds
    * can be determined.
    *
    * @param filters The array of filters to extract __time bounds from
    * @return A tuple containing an optional lower and an optional upper bound on the __time column.
    */
  def getTimeFilterBounds(filters: Array[Filter]): (Option[Long], Option[Long]) = {
    val timeFilters = filters
      .filter(_.references.contains("__time"))
      .flatMap(FilterUtils.decomposeTimeFilters)
      .partition(_._1 == FilterUtils.LOWER)
    (timeFilters._1.map(_._2).reduceOption(_ max _),
      timeFilters._2.map(_._2).reduceOption(_ min _))
  }

  /**
    * Decompose a Spark Filter into a sequence of bounds on the __time field if possible.
    *
    * @param filter The Spark filter to possibly extract time bounds from.
    * @return A sequnce of tuples containing either UPPER or LOWER bounds on the __time field, in
    *         epoch millis.
    */
  private[spark] def decomposeTimeFilters(filter: Filter): Seq[(Bound, Long)] = { // scalastyle:ignore method.length
    filter match {
      case And(left, right) =>
        val bounds = Seq(left, right)
          .filter(_.references.contains("__time"))
          .flatMap(decomposeTimeFilters)
          .partition(_._1 == LOWER)
        val optBounds = (bounds._1.map(_._2).reduceOption(_ max _ ),
          bounds._2.map(_._2).reduceOption(_ min _ ))
        Seq[Option[(Bound, Long)]](
          optBounds._1.fold(Option.empty[(Bound, Long)])(bound => Some((LOWER, bound))),
          optBounds._2.fold(Option.empty[(Bound, Long)])(bound => Some((UPPER, bound)))
        ).flatten
      case Or(left, right) =>
        val bounds = Seq(left, right)
          .filter(_.references.contains("__time")).flatMap(decomposeTimeFilters)
          .partition(_._1 == LOWER)
        val optBounds = (bounds._1.map(_._2).reduceOption(_ min _ ),
          bounds._2.map(_._2).reduceOption(_ max _ ))
        Seq[Option[(Bound, Long)]](
          optBounds._1.fold(Option.empty[(Bound, Long)])(bound => Some((LOWER, bound))),
          optBounds._2.fold(Option.empty[(Bound, Long)])(bound => Some((UPPER, bound)))
        ).flatten
      case Not(condition) =>
        if (condition.references.contains("__time")) {
          // Our quick and dirty bounds enum doesn't handle nots, so just return an unbounded interval
          Seq[(Bound, Long)](
            (LOWER, JodaUtils.MIN_INSTANT),
            (UPPER, JodaUtils.MAX_INSTANT)
          )
        } else {
          emptyBoundSeq
        }
      case EqualTo(field, value) =>
        if (field == "__time") {
          Seq(
            (LOWER, value.asInstanceOf[Long]),
            (UPPER, value.asInstanceOf[Long])
          )
        } else {
          emptyBoundSeq
        }
      case LessThan(field, value) =>
        if (field == "__time") {
          Seq((UPPER, value.asInstanceOf[Long] - 1))
        } else {
          emptyBoundSeq
        }
      case LessThanOrEqual(field, value) =>
        if (field == "__time") {
          Seq((UPPER, value.asInstanceOf[Long]))
        } else {
          emptyBoundSeq
        }
      case GreaterThan(field, value) =>
        if (field == "__time") {
          Seq((LOWER, value.asInstanceOf[Long] + 1))
        } else {
          emptyBoundSeq
        }
      case GreaterThanOrEqual(field, value) =>
        if (field == "__time") {
          Seq((LOWER, value.asInstanceOf[Long]))
        } else {
          emptyBoundSeq
        }
      case _ => emptyBoundSeq
    }
  }

  private[spark] sealed trait Bound
  case object LOWER extends Bound
  case object UPPER extends Bound

}
