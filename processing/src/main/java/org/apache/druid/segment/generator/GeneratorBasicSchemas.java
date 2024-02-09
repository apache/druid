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

package org.apache.druid.segment.generator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ValueType;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GeneratorBasicSchemas
{
  private static final ImmutableMap.Builder<String, GeneratorSchemaInfo> SCHEMA_INFO_BUILDER = ImmutableMap.builder();

  static {
    // basic schema
    List<GeneratorColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims
        GeneratorColumnSchema.makeSequential("dimSequential", ValueType.STRING, false, 1, null, 0, 1000),
        GeneratorColumnSchema.makeZipf("dimZipf", ValueType.STRING, false, 1, null, 1, 101, 1.0),
        GeneratorColumnSchema.makeDiscreteUniform("dimUniform", ValueType.STRING, false, 1, null, 1, 100000),
        GeneratorColumnSchema.makeSequential("dimSequentialHalfNull", ValueType.STRING, false, 1, 0.5, 0, 1000),
        GeneratorColumnSchema.makeEnumerated(
            "dimMultivalEnumerated",
            ValueType.STRING,
            false,
            4,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        GeneratorColumnSchema.makeEnumerated(
            "dimMultivalEnumerated2",
            ValueType.STRING,
            false,
            3,
            null,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        GeneratorColumnSchema.makeSequential("dimMultivalSequentialWithNulls", ValueType.STRING, false, 8, 0.15, 1, 11),
        GeneratorColumnSchema.makeSequential("dimHyperUnique", ValueType.STRING, false, 1, null, 0, 100000),
        GeneratorColumnSchema.makeSequential("dimNull", ValueType.STRING, false, 1, 1.0, 0, 1),

        // metrics
        GeneratorColumnSchema.makeSequential("metLongSequential", ValueType.LONG, true, 1, null, 0, 10000),
        GeneratorColumnSchema.makeDiscreteUniform("metLongUniform", ValueType.LONG, true, 1, null, 0, 500),
        GeneratorColumnSchema.makeNormal("metFloatNormal", ValueType.FLOAT, true, 1, null, 5000.0, 1.0, true),
        GeneratorColumnSchema.makeZipf("metFloatZipf", ValueType.FLOAT, true, 1, null, 0, 1000, 1.0)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));
    basicSchemaIngestAggs.add(new LongSumAggregatorFactory("sumLongSequential", "metLongSequential"));
    basicSchemaIngestAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "metLongUniform"));
    basicSchemaIngestAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "metFloatNormal"));
    basicSchemaIngestAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "metFloatZipf"));
    basicSchemaIngestAggs.add(new HyperUniquesAggregatorFactory("hyper", "dimHyperUnique"));

    List<AggregatorFactory> basicSchemaIngestAggsExpression = new ArrayList<>();
    basicSchemaIngestAggsExpression.add(new CountAggregatorFactory("rows"));
    basicSchemaIngestAggsExpression.add(new LongSumAggregatorFactory(
        "sumLongSequential",
        null,
        "if(sumLongSequential>0 && dimSequential>100 || dimSequential<10 || metLongSequential>3000,sumLongSequential,0)",
        ExprMacroTable.nil()
    ));
    basicSchemaIngestAggsExpression.add(new LongMaxAggregatorFactory("maxLongUniform", "metLongUniform"));
    basicSchemaIngestAggsExpression.add(new DoubleSumAggregatorFactory(
        "sumFloatNormal",
        null,
        "if(sumFloatNormal>0 && dimSequential>100 || dimSequential<10 || metLongSequential>3000,sumFloatNormal,0)",
        ExprMacroTable.nil()
    ));
    basicSchemaIngestAggsExpression.add(new DoubleMinAggregatorFactory("minFloatZipf", "metFloatZipf"));
    basicSchemaIngestAggsExpression.add(new HyperUniquesAggregatorFactory("hyper", "dimHyperUnique"));

    Interval basicSchemaDataInterval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo basicSchema = new GeneratorSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        true
    );

    GeneratorSchemaInfo basicSchemaExpression = new GeneratorSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggsExpression,
        basicSchemaDataInterval,
        true
    );

    SCHEMA_INFO_BUILDER.put("basic", basicSchema);
    SCHEMA_INFO_BUILDER.put("expression", basicSchemaExpression);
  }

  static { // simple single string column and count agg schema, no rollup
    List<GeneratorColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims
        GeneratorColumnSchema.makeSequential("dimSequential", ValueType.STRING, false, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    GeneratorSchemaInfo basicSchema = new GeneratorSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_INFO_BUILDER.put("simple", basicSchema);
  }

  static {
    // simple single long column and count agg schema, no rollup
    List<GeneratorColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims, ingest as a metric for now with rollup off, until numeric dims at ingestion are supported
        GeneratorColumnSchema.makeSequential("dimSequential", ValueType.LONG, true, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new LongSumAggregatorFactory("dimSequential", "dimSequential"));
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    GeneratorSchemaInfo basicSchema = new GeneratorSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_INFO_BUILDER.put("simpleLong", basicSchema);
  }

  static {
    // simple single float column and count agg schema, no rollup
    List<GeneratorColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims, ingest as a metric for now with rollup off, until numeric dims at ingestion are supported
        GeneratorColumnSchema.makeSequential("dimSequential", ValueType.FLOAT, true, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new DoubleSumAggregatorFactory("dimSequential", "dimSequential"));
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    GeneratorSchemaInfo basicSchema = new GeneratorSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_INFO_BUILDER.put("simpleFloat", basicSchema);
  }

  static {
    // schema with high opportunity for rollup
    List<GeneratorColumnSchema> rolloColumns = ImmutableList.of(
        // dims
        GeneratorColumnSchema.makeEnumerated(
            "dimEnumerated",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        GeneratorColumnSchema.makeEnumerated(
            "dimEnumerated2",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        GeneratorColumnSchema.makeZipf("dimZipf", ValueType.STRING, false, 1, null, 1, 100, 2.0),
        GeneratorColumnSchema.makeDiscreteUniform("dimUniform", ValueType.STRING, false, 1, null, 1, 100),

        // metrics
        GeneratorColumnSchema.makeZipf("metLongZipf", ValueType.LONG, true, 1, null, 0, 10000, 2.0),
        GeneratorColumnSchema.makeDiscreteUniform("metLongUniform", ValueType.LONG, true, 1, null, 0, 500),
        GeneratorColumnSchema.makeNormal("metFloatNormal", ValueType.FLOAT, true, 1, null, 5000.0, 1.0, true),
        GeneratorColumnSchema.makeZipf("metFloatZipf", ValueType.FLOAT, true, 1, null, 0, 1000, 1.5)
    );

    List<AggregatorFactory> rolloSchemaIngestAggs = new ArrayList<>();
    rolloSchemaIngestAggs.add(new CountAggregatorFactory("rows"));
    rolloSchemaIngestAggs.add(new LongSumAggregatorFactory("sumLongSequential", "metLongSequential"));
    rolloSchemaIngestAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "metLongUniform"));
    rolloSchemaIngestAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "metFloatNormal"));
    rolloSchemaIngestAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "metFloatZipf"));
    rolloSchemaIngestAggs.add(new HyperUniquesAggregatorFactory("hyper", "dimHyperUnique"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    GeneratorSchemaInfo rolloSchema = new GeneratorSchemaInfo(
        rolloColumns,
        rolloSchemaIngestAggs,
        basicSchemaDataInterval,
        true
    );
    SCHEMA_INFO_BUILDER.put("rollo", rolloSchema);
  }

  static {
    // simple schema with null valued rows, no aggs on numeric columns
    List<GeneratorColumnSchema> nullsSchemaColumns = ImmutableList.of(
        // string dims with nulls
        GeneratorColumnSchema.makeZipf("stringZipf", ValueType.STRING, false, 1, 0.8, 1, 101, 1.5),
        GeneratorColumnSchema.makeDiscreteUniform("stringUniform", ValueType.STRING, false, 1, 0.3, 1, 100000),
        GeneratorColumnSchema.makeSequential("stringSequential", ValueType.STRING, false, 1, 0.5, 0, 1000),

        // numeric dims with nulls
        GeneratorColumnSchema.makeSequential("longSequential", ValueType.LONG, false, 1, 0.45, 0, 10000),
        GeneratorColumnSchema.makeDiscreteUniform("longUniform", ValueType.LONG, false, 1, 0.25, 0, 500),
        GeneratorColumnSchema.makeZipf("doubleZipf", ValueType.DOUBLE, false, 1, 0.1, 0, 1000, 2.0),
        GeneratorColumnSchema.makeZipf("floatZipf", ValueType.FLOAT, false, 1, 0.1, 0, 1000, 2.0)
    );

    List<AggregatorFactory> simpleNullsSchemaIngestAggs = new ArrayList<>();
    simpleNullsSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval nullsSchemaDataInterval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo nullsSchema = new GeneratorSchemaInfo(
        nullsSchemaColumns,
        simpleNullsSchemaIngestAggs,
        nullsSchemaDataInterval,
        false
    );

    SCHEMA_INFO_BUILDER.put("nulls", nullsSchema);
  }

  static {
    // simple schema with null valued rows, no aggs on numeric columns
    List<GeneratorColumnSchema> nullsSchemaColumns = ImmutableList.of(
        // string dims
        GeneratorColumnSchema.makeZipf("stringZipf", ValueType.STRING, false, 1, null, 1, 101, 1.5),
        GeneratorColumnSchema.makeDiscreteUniform("stringUniform", ValueType.STRING, false, 1, null, 1, 100000),
        GeneratorColumnSchema.makeSequential("stringSequential", ValueType.STRING, false, 1, null, 0, 1000),

        // string dims with nulls
        GeneratorColumnSchema.makeZipf("stringZipfWithNulls", ValueType.STRING, false, 1, 0.8, 1, 101, 1.5),
        GeneratorColumnSchema.makeDiscreteUniform("stringUniformWithNulls", ValueType.STRING, false, 1, 0.3, 1, 100000),
        GeneratorColumnSchema.makeSequential("stringSequentialWithNulls", ValueType.STRING, false, 1, 0.5, 0, 1000),

        // numeric dims
        GeneratorColumnSchema.makeSequential("longSequential", ValueType.LONG, false, 1, null, 0, 10000),
        GeneratorColumnSchema.makeDiscreteUniform("longUniform", ValueType.LONG, false, 1, null, 0, 500),
        GeneratorColumnSchema.makeZipf("doubleZipf", ValueType.DOUBLE, false, 1, null, 0, 1000, 2.0),
        GeneratorColumnSchema.makeZipf("floatZipf", ValueType.FLOAT, false, 1, null, 0, 1000, 2.0),

        // numeric dims with nulls
        GeneratorColumnSchema.makeSequential("longSequentialWithNulls", ValueType.LONG, false, 1, 0.45, 0, 10000),
        GeneratorColumnSchema.makeDiscreteUniform("longUniformWithNulls", ValueType.LONG, false, 1, 0.25, 0, 500),
        GeneratorColumnSchema.makeZipf("doubleZipfWithNulls", ValueType.DOUBLE, false, 1, 0.1, 0, 1000, 2.0),
        GeneratorColumnSchema.makeZipf("floatZipfWithNulls", ValueType.FLOAT, false, 1, 0.1, 0, 1000, 2.0)
    );

    List<AggregatorFactory> simpleNullsSchemaIngestAggs = new ArrayList<>();
    simpleNullsSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval nullsSchemaDataInterval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo nullsSchema = new GeneratorSchemaInfo(
        nullsSchemaColumns,
        simpleNullsSchemaIngestAggs,
        nullsSchemaDataInterval,
        false
    );

    SCHEMA_INFO_BUILDER.put("nulls-and-non-nulls", nullsSchema);
  }

  static {
    // schema for benchmarking expressions
    List<GeneratorColumnSchema> expressionsTestsSchemaColumns = ImmutableList.of(
        // string dims
        GeneratorColumnSchema.makeSequential("string1", ValueType.STRING, false, 1, null, 0, 10000),
        GeneratorColumnSchema.makeLazyZipf("string2", ValueType.STRING, false, 1, null, 1, 100, 1.5),
        GeneratorColumnSchema.makeLazyZipf("string3", ValueType.STRING, false, 1, 0.1, 1, 1_000_000, 2.0),
        GeneratorColumnSchema.makeLazyDiscreteUniform("string4", ValueType.STRING, false, 1, null, 1, 10_000),
        GeneratorColumnSchema.makeLazyDiscreteUniform("string5", ValueType.STRING, false, 1, 0.3, 1, 1_000_000),

        // multi string dims
        GeneratorColumnSchema.makeSequential("multi-string1", ValueType.STRING, false, 8, null, 0, 10000),
        GeneratorColumnSchema.makeLazyZipf("multi-string2", ValueType.STRING, false, 8, null, 1, 100, 1.5),
        GeneratorColumnSchema.makeLazyZipf("multi-string3", ValueType.STRING, false, 16, 0.1, 1, 1_000_000, 2.0),
        GeneratorColumnSchema.makeLazyDiscreteUniform("multi-string4", ValueType.STRING, false, 4, null, 1, 10_000),
        GeneratorColumnSchema.makeLazyDiscreteUniform("multi-string5", ValueType.STRING, false, 8, 0.3, 1, 1_000_000),

        // numeric dims
        GeneratorColumnSchema.makeSequential("long1", ValueType.LONG, false, 1, null, 0, 10000),
        GeneratorColumnSchema.makeLazyZipf("long2", ValueType.LONG, false, 1, null, 1, 101, 1.5),
        GeneratorColumnSchema.makeLazyZipf("long3", ValueType.LONG, false, 1, 0.1, -1_000_000, 1_000_000, 2.0),
        GeneratorColumnSchema.makeLazyDiscreteUniform("long4", ValueType.LONG, false, 1, null, -10_000, 10000),
        GeneratorColumnSchema.makeLazyDiscreteUniform("long5", ValueType.LONG, false, 1, 0.3, -1_000_000, 1_000_000),

        GeneratorColumnSchema.makeLazyZipf("double1", ValueType.DOUBLE, false, 1, null, 1, 101, 1.5),
        GeneratorColumnSchema.makeLazyZipf("double2", ValueType.DOUBLE, false, 1, 0.1, -1_000_000, 1_000_000, 2.0),
        GeneratorColumnSchema.makeContinuousUniform("double3", ValueType.DOUBLE, false, 1, null, -9000.0, 9000.0),
        GeneratorColumnSchema.makeContinuousUniform("double4", ValueType.DOUBLE, false, 1, null, -1_000_000, 1_000_000),
        GeneratorColumnSchema.makeLazyZipf("double5", ValueType.DOUBLE, false, 1, 0.1, 0, 1000, 2.0),

        GeneratorColumnSchema.makeLazyZipf("float1", ValueType.FLOAT, false, 1, null, 1, 101, 1.5),
        GeneratorColumnSchema.makeLazyZipf("float2", ValueType.FLOAT, false, 1, 0.1, -1_000_000, 1_000_000, 2.0),
        GeneratorColumnSchema.makeContinuousUniform("float3", ValueType.FLOAT, false, 1, null, -9000.0, 9000.0),
        GeneratorColumnSchema.makeContinuousUniform("float4", ValueType.FLOAT, false, 1, null, -1_000_000, 1_000_000),
        GeneratorColumnSchema.makeLazyZipf("float5", ValueType.FLOAT, false, 1, 0.1, 0, 1000, 2.0)

    );

    List<AggregatorFactory> aggs = new ArrayList<>();
    aggs.add(new CountAggregatorFactory("rows"));

    Interval interval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo expressionsTestsSchema = new GeneratorSchemaInfo(
        expressionsTestsSchemaColumns,
        aggs,
        interval,
        false
    );

    SCHEMA_INFO_BUILDER.put("expression-testbench", expressionsTestsSchema);
  }

  static {
    List<GeneratorColumnSchema> inTestsSchemaColumns = ImmutableList.of(
        GeneratorColumnSchema.makeSequential("long1", ValueType.LONG, false, 1, null, 0, 40000),
        GeneratorColumnSchema.makeSequential("string1", ValueType.STRING, false, 1, null, 0, 40000)
    );

    List<AggregatorFactory> aggs = new ArrayList<>();

    Interval interval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo expressionsTestsSchema = new GeneratorSchemaInfo(
        inTestsSchemaColumns,
        aggs,
        interval,
        false
    );

    SCHEMA_INFO_BUILDER.put("in-testbench", expressionsTestsSchema);

  }

  static {
    // simple 'wide' schema with null valued rows, high cardinality columns, no aggs on numeric columns
    // essentially 'nulls-and-non-nulls' with a ton of extra zipf columns of each type with a variety of value
    // distributions
    List<GeneratorColumnSchema> nullsSchemaColumns = ImmutableList.of(
        // string dims
        GeneratorColumnSchema.makeLazyZipf("stringZipf", ValueType.STRING, false, 1, null, 1, 101, 1.5),
        GeneratorColumnSchema.makeLazyDiscreteUniform("stringUniform", ValueType.STRING, false, 1, null, 1, 100000),
        GeneratorColumnSchema.makeSequential("stringSequential", ValueType.STRING, false, 1, null, 0, 1000),

        // string dims with nulls
        GeneratorColumnSchema.makeLazyZipf("stringZipfWithNulls", ValueType.STRING, false, 1, 0.8, 1, 101, 1.5),
        GeneratorColumnSchema.makeLazyDiscreteUniform(
            "stringUniformWithNulls",
            ValueType.STRING,
            false,
            1,
            0.3,
            1,
            100000
        ),
        GeneratorColumnSchema.makeSequential("stringSequentialWithNulls", ValueType.STRING, false, 1, 0.5, 0, 1000),

        // additional string columns
        GeneratorColumnSchema.makeLazyZipf("string1", ValueType.STRING, false, 1, 0.1, 1, 100000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("string2", ValueType.STRING, false, 1, 0.3, 1, 1000000, 1.5),
        GeneratorColumnSchema.makeLazyZipf("string3", ValueType.STRING, false, 1, 0.12, 1, 1000, 1.25),
        GeneratorColumnSchema.makeLazyZipf("string4", ValueType.STRING, false, 1, 0.22, 1, 12000, 3.0),
        GeneratorColumnSchema.makeLazyZipf("string5", ValueType.STRING, false, 1, 0.05, 1, 33333, 1.8),
        GeneratorColumnSchema.makeLazyZipf("string6", ValueType.STRING, false, 1, 0.09, 1, 25000000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("string7", ValueType.STRING, false, 1, 0.8, 1, 100, 1.5),
        GeneratorColumnSchema.makeLazyZipf("string8", ValueType.STRING, false, 1, 0.5, 1, 10, 1.2),
        GeneratorColumnSchema.makeLazyZipf("string9", ValueType.STRING, false, 1, 0.05, 1, 1000000, 1.3),
        GeneratorColumnSchema.makeLazyZipf("string10", ValueType.STRING, false, 1, 0.4, 1, 888888, 1.4),
        GeneratorColumnSchema.makeLazyZipf("string11", ValueType.STRING, false, 1, 0.7, 1, 999, 1.8),
        GeneratorColumnSchema.makeLazyZipf("string12", ValueType.STRING, false, 1, 0.2, 1, 123456, 1.0),
        GeneratorColumnSchema.makeLazyZipf("string13", ValueType.STRING, false, 1, 0.33, 1, 52, 1.9),
        GeneratorColumnSchema.makeLazyZipf("string14", ValueType.STRING, false, 1, 0.42, 1, 90001, 1.75),

        // numeric dims
        GeneratorColumnSchema.makeSequential("longSequential", ValueType.LONG, false, 1, null, 0, 10000),
        GeneratorColumnSchema.makeLazyDiscreteUniform("longUniform", ValueType.LONG, false, 1, null, 0, 500),

        GeneratorColumnSchema.makeLazyZipf("longZipf", ValueType.LONG, false, 1, null, 0, 1000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("doubleZipf", ValueType.DOUBLE, false, 1, null, 0, 1000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("floatZipf", ValueType.FLOAT, false, 1, null, 0, 1000, 2.0),

        // numeric dims with nulls
        GeneratorColumnSchema.makeSequential("longSequentialWithNulls", ValueType.LONG, false, 1, 0.45, 0, 10000),
        GeneratorColumnSchema.makeLazyDiscreteUniform("longUniformWithNulls", ValueType.LONG, false, 1, 0.25, 0, 500),

        GeneratorColumnSchema.makeLazyZipf("longZipfWithNulls", ValueType.LONG, false, 1, 0.1, 0, 1000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("doubleZipfWithNulls", ValueType.DOUBLE, false, 1, 0.1, 0, 1000, 2.0),
        GeneratorColumnSchema.makeLazyZipf("floatZipfWithNulls", ValueType.FLOAT, false, 1, 0.1, 0, 1000, 2.0),

        // additional numeric columns
        GeneratorColumnSchema.makeLazyZipf("long1", ValueType.LONG, false, 1, 0.1, 0, 1001, 2.0),
        GeneratorColumnSchema.makeLazyZipf("long2", ValueType.LONG, false, 1, 0.01, 0, 666666, 2.2),
        GeneratorColumnSchema.makeLazyZipf("long3", ValueType.LONG, false, 1, 0.12, 0, 1000000, 2.5),
        GeneratorColumnSchema.makeLazyZipf("long4", ValueType.LONG, false, 1, 0.4, 0, 23, 1.2),
        GeneratorColumnSchema.makeLazyZipf("long5", ValueType.LONG, false, 1, 0.33, 0, 9999, 1.5),
        GeneratorColumnSchema.makeLazyZipf("long6", ValueType.LONG, false, 1, 0.8, 0, 12345, 1.8),
        GeneratorColumnSchema.makeLazyZipf("long7", ValueType.LONG, false, 1, 0.6, 0, 543210, 2.3),
        GeneratorColumnSchema.makeLazyZipf("long8", ValueType.LONG, false, 1, 0.5, 0, 11223344, 2.4),
        GeneratorColumnSchema.makeLazyZipf("long9", ValueType.LONG, false, 1, 0.111, 0, 80, 4.0),
        GeneratorColumnSchema.makeLazyZipf("long10", ValueType.LONG, false, 1, 0.21, 0, 111, 3.3),

        GeneratorColumnSchema.makeLazyZipf("double1", ValueType.DOUBLE, false, 1, 0.1, 0, 333, 2.2),
        GeneratorColumnSchema.makeLazyZipf("double2", ValueType.DOUBLE, false, 1, 0.01, 0, 4021, 2.5),
        GeneratorColumnSchema.makeLazyZipf("double3", ValueType.DOUBLE, false, 1, 0.41, 0, 90210, 4.0),
        GeneratorColumnSchema.makeLazyZipf("double4", ValueType.DOUBLE, false, 1, 0.5, 0, 5555555, 1.2),
        GeneratorColumnSchema.makeLazyZipf("double5", ValueType.DOUBLE, false, 1, 0.23, 0, 80, 1.8),
        GeneratorColumnSchema.makeLazyZipf("double6", ValueType.DOUBLE, false, 1, 0.33, 0, 99999, 3.0),
        GeneratorColumnSchema.makeLazyZipf("double7", ValueType.DOUBLE, false, 1, 0.8, 0, 12345678, 1.4),
        GeneratorColumnSchema.makeLazyZipf("double8", ValueType.DOUBLE, false, 1, 0.4, 0, 8080, 4.2),
        GeneratorColumnSchema.makeLazyZipf("double9", ValueType.DOUBLE, false, 1, 0.111, 0, 9999, 2.3),
        GeneratorColumnSchema.makeLazyZipf("double10", ValueType.DOUBLE, false, 1, 0.2, 0, 19, 0.6),

        GeneratorColumnSchema.makeLazyZipf("float1", ValueType.FLOAT, false, 1, 0.11, 0, 1000000, 1.7),
        GeneratorColumnSchema.makeLazyZipf("float2", ValueType.FLOAT, false, 1, 0.4, 0, 10, 1.5),
        GeneratorColumnSchema.makeLazyZipf("float3", ValueType.FLOAT, false, 1, 0.8, 0, 5000, 2.3),
        GeneratorColumnSchema.makeLazyZipf("float4", ValueType.FLOAT, false, 1, 0.999, 0, 14440, 2.0),
        GeneratorColumnSchema.makeLazyZipf("float5", ValueType.FLOAT, false, 1, 0.001, 0, 1029, 1.5),
        GeneratorColumnSchema.makeLazyZipf("float6", ValueType.FLOAT, false, 1, 0.01, 0, 4445555, 1.8),
        GeneratorColumnSchema.makeLazyZipf("float7", ValueType.FLOAT, false, 1, 0.44, 0, 1000000, 1.1),
        GeneratorColumnSchema.makeLazyZipf("float8", ValueType.FLOAT, false, 1, 0.55, 0, 33, 4.5),
        GeneratorColumnSchema.makeLazyZipf("float9", ValueType.FLOAT, false, 1, 0.12, 0, 5000, 2.2),
        GeneratorColumnSchema.makeLazyZipf("float10", ValueType.FLOAT, false, 1, 0.11, 0, 101, 1.3)
    );

    List<AggregatorFactory> simpleNullsSchemaIngestAggs = new ArrayList<>();
    simpleNullsSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval nullsSchemaDataInterval = Intervals.of("2000-01-01/P1D");

    GeneratorSchemaInfo nullsSchema = new GeneratorSchemaInfo(
        nullsSchemaColumns,
        simpleNullsSchemaIngestAggs,
        nullsSchemaDataInterval,
        false
    );

    SCHEMA_INFO_BUILDER.put("wide", nullsSchema);
  }

  public static final Map<String, GeneratorSchemaInfo> SCHEMA_MAP = SCHEMA_INFO_BUILDER.build();
}
