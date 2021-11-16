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

import org.apache.druid.query.filter.{AndDimFilter, BoundDimFilter, DimFilter, FalseDimFilter,
  InDimFilter, NotDimFilter, RegexDimFilter, SelectorDimFilter}
import org.apache.druid.query.ordering.StringComparators
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, GreaterThan, GreaterThanOrEqual,
  In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith,
  StringStartsWith, Filter => SparkFilter}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaSetConverter, seqAsJavaListConverter,
  setAsJavaSetConverter}

class FilterUtilsSuite extends AnyFunSuite with Matchers {
  private val testSchema = StructType(Seq[StructField](
    StructField("count", LongType),
    StructField("name", StringType)
  ))

  test("mapFilters should convert a Spark filter into an equivalent Druid filter") {
    val testSchema = StructType(Seq[StructField](
      StructField("count", LongType)
    ))

    val druidFilter = FilterUtils.mapFilters(Array[SparkFilter](GreaterThan("count", 5)), testSchema).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("count")

    // scalastyle:off null
    val expected = new BoundDimFilter(
      "count",
      "5",
      null,
      true,
      null,
      null,
      null,
      StringComparators.NUMERIC,
      null)
    // scalastyle:on
    expected should equal(druidFilter)
  }

  test("mapFilters should map multiple Spark filters into an equivalent Druid filter") {
    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](GreaterThan("count", 5), LessThanOrEqual("name", "foo")),
      testSchema
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("count", "name")

    // scalastyle:off null
    val expected = new AndDimFilter(
      List[DimFilter](
        new BoundDimFilter(
          "count",
          "5",
          null,
          true,
          null,
          null,
          null,
          StringComparators.NUMERIC,
          null),
        new BoundDimFilter(
          "name",
          null,
          "foo",
          null,
          false,
          null,
          null,
          StringComparators.LEXICOGRAPHIC,
          null)
      ).asJava
    )
    // scalastyle:on
    expected should equal(druidFilter)
  }

  test("mapFilters should map a complex Spark filter into an equivalent Druid filter") {
    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](And(GreaterThan("count", 5), StringStartsWith("name", "abc"))),
      testSchema
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("count", "name")

    // scalastyle:off null

    val expected = new AndDimFilter(
      List[DimFilter](
        new BoundDimFilter(
          "count",
          "5",
          null,
          true,
          null,
          null,
          null,
          StringComparators.NUMERIC,
          null),
        new RegexDimFilter("name", "^abc", null, null)
      ).asJava
    )
    // scalastyle:on
    expected should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark IsNull filter into an equivalent Druid filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](IsNull("name")), testSchema
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("name")

    // scalastyle:off null
    val expected = new SelectorDimFilter("name", null, null, null)
    // scalastyle:on

    expected should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark IsNotNull filter into an equivalent Druid filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](IsNotNull("name")), testSchema
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("name")

    // scalastyle:off null
    val expected = new NotDimFilter(
      new SelectorDimFilter("name", null, null, null)
    )
    // scalastyle:on

    expected should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark In filter with null into an equivalent Druid filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](In("name", Array("a", "b", null))), testSchema // scalastyle:ignore null
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("name")

    // scalastyle:off null
    val expected = new InDimFilter("name", Set[String]("a", "b").asJava, null, null)
    // scalastyle:on

    expected should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark In filter with only null into a short-circuit filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](In("name", Array(null))), testSchema // scalastyle:ignore null
    ).get

    FalseDimFilter.instance() should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark EqualNullSafe null filter into an equivalent Druid filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](EqualNullSafe("name", null)), testSchema // scalastyle:ignore null
    ).get
    druidFilter.getRequiredColumns.asScala should contain theSameElementsAs Seq("name")

    // scalastyle:off null
    val expected = new SelectorDimFilter("name", null, null, null)
    // scalastyle:on null

    expected should equal(druidFilter)
  }

  test("mapFilters should correctly map a Spark EqualTo null filter into a short-circuit filter") {
    NullHandlingUtils.initializeDruidNullHandling(false)

    val druidFilter = FilterUtils.mapFilters(
      Array[SparkFilter](EqualTo("name", null)), testSchema // scalastyle:ignore null
    ).get

    FalseDimFilter.instance() should equal(druidFilter)
  }

  test("isSupportedFilter should correctly identify supported and unsupported filters") {
    FilterUtils.isSupportedFilter(And(EqualTo("count", 1), LessThan("name", "abc")), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(And(EqualTo("count", 1), IsNull("name")), testSchema) shouldBe false

    FilterUtils.isSupportedFilter(Or(EqualTo("count", 1), LessThan("name", "abc")), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(Or(EqualTo("count", 1), IsNull("name")), testSchema) shouldBe false

    FilterUtils.isSupportedFilter(Not(GreaterThan("count", 5)), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(Not(IsNull("count")), testSchema) shouldBe false

    FilterUtils.isSupportedFilter(IsNull("count"), testSchema) shouldBe false
    FilterUtils.isSupportedFilter(IsNotNull("count"), testSchema) shouldBe false

    FilterUtils.isSupportedFilter(In("name", Array[Any]("foo", "bar")), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(In("count", Array[Any](null)), testSchema) shouldBe false // scalastyle:ignore null

    FilterUtils.isSupportedFilter(StringContains("count", "foo"), testSchema) shouldBe false
    FilterUtils.isSupportedFilter(StringContains("name", "foo"), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(StringStartsWith("name", "foo"), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(StringEndsWith("name", "foo"), testSchema) shouldBe true

    FilterUtils.isSupportedFilter(EqualTo("count", 5), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(EqualTo("name", null), testSchema) shouldBe false // scalastyle:ignore null
    FilterUtils.isSupportedFilter(EqualNullSafe("count", 5), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(EqualNullSafe("name", null), testSchema) shouldBe false // scalastyle:ignore null

    FilterUtils.isSupportedFilter(LessThan("name", "foo"), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(LessThanOrEqual("count", 17), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(GreaterThan("name", "bar"), testSchema) shouldBe true
    FilterUtils.isSupportedFilter(GreaterThanOrEqual("count", -8), testSchema) shouldBe true
  }

  test("isSupportedFilter should correctly identify IsNull and IsNotNull filters as supported when using " +
    "SQL-compatible null handling") {
    FilterUtils.isSupportedFilter(And(EqualTo("count", 1), IsNull("name")), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(Or(EqualTo("count", 1), IsNull("name")), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(Not(IsNull("count")), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(IsNull("count"), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(IsNotNull("count"), testSchema, true) shouldBe true
  }

  test("isSupportedFilter should correctly identify In, EqualNullSafe, and EqualTo filters on null as " +
    "supported when using SQL-compatible null handling") {
    // scalastyle:off null
    FilterUtils.isSupportedFilter(In("name", Array[Any]("a", "b", null)), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(In("name", Array[Any](null)), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(EqualNullSafe("name", null), testSchema, true) shouldBe true
    FilterUtils.isSupportedFilter(EqualTo("name", null), testSchema, true) shouldBe true
    // scalastyle:on
  }

  test("getTimeFilterBounds should handle upper and lower bounds") {
    val expected = (Some(2501L), Some(5000L))
    val filters = Array[SparkFilter](LessThanOrEqual("__time", 5000L), GreaterThan("__time", 2500L))

    val actual = FilterUtils.getTimeFilterBounds(filters)
    actual should equal(expected)
  }

  test("getTimeFilterBounds should handle empty or multiple filters for a bound") {
    val expected = (None, Some(2499L))
    val filters = Array[SparkFilter](LessThanOrEqual("__time", 5000L), LessThan("__time", 2500L))

    val actual =  FilterUtils.getTimeFilterBounds(filters)
    actual should equal(expected)
  }
}
