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

package org.apache.druid.benchmark.datagen;

import com.google.common.collect.ImmutableList;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkSchemas
{
  public static final Map<String, BenchmarkSchemaInfo> SCHEMA_MAP = new LinkedHashMap<>();

  static { // basic schema
    List<BenchmarkColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims
        BenchmarkColumnSchema.makeSequential("dimSequential", ValueType.STRING, false, 1, null, 0, 1000),
        BenchmarkColumnSchema.makeZipf("dimZipf", ValueType.STRING, false, 1, null, 1, 101, 1.0),
        BenchmarkColumnSchema.makeDiscreteUniform("dimUniform", ValueType.STRING, false, 1, null, 1, 100000),
        BenchmarkColumnSchema.makeSequential("dimSequentialHalfNull", ValueType.STRING, false, 1, 0.5, 0, 1000),
        BenchmarkColumnSchema.makeEnumerated(
            "dimMultivalEnumerated",
            ValueType.STRING,
            false,
            4,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        BenchmarkColumnSchema.makeEnumerated(
            "dimMultivalEnumerated2",
            ValueType.STRING,
            false,
            3,
            null,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        BenchmarkColumnSchema.makeSequential("dimMultivalSequentialWithNulls", ValueType.STRING, false, 8, 0.15, 1, 11),
        BenchmarkColumnSchema.makeSequential("dimHyperUnique", ValueType.STRING, false, 1, null, 0, 100000),
        BenchmarkColumnSchema.makeSequential("dimNull", ValueType.STRING, false, 1, 1.0, 0, 1),

        // metrics
        BenchmarkColumnSchema.makeSequential("metLongSequential", ValueType.LONG, true, 1, null, 0, 10000),
        BenchmarkColumnSchema.makeDiscreteUniform("metLongUniform", ValueType.LONG, true, 1, null, 0, 500),
        BenchmarkColumnSchema.makeNormal("metFloatNormal", ValueType.FLOAT, true, 1, null, 5000.0, 1.0, true),
        BenchmarkColumnSchema.makeZipf("metFloatZipf", ValueType.FLOAT, true, 1, null, 0, 1000, 1.0)
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
    basicSchemaIngestAggsExpression.add(new LongSumAggregatorFactory("sumLongSequential", null, "if(sumLongSequential>0 && dimSequential>100 || dimSequential<10 || metLongSequential>3000,sumLongSequential,0)", ExprMacroTable.nil()));
    basicSchemaIngestAggsExpression.add(new LongMaxAggregatorFactory("maxLongUniform", "metLongUniform"));
    basicSchemaIngestAggsExpression.add(new DoubleSumAggregatorFactory("sumFloatNormal", null, "if(sumFloatNormal>0 && dimSequential>100 || dimSequential<10 || metLongSequential>3000,sumFloatNormal,0)", ExprMacroTable.nil()));
    basicSchemaIngestAggsExpression.add(new DoubleMinAggregatorFactory("minFloatZipf", "metFloatZipf"));
    basicSchemaIngestAggsExpression.add(new HyperUniquesAggregatorFactory("hyper", "dimHyperUnique"));

    Interval basicSchemaDataInterval = Intervals.of("2000-01-01/P1D");

    BenchmarkSchemaInfo basicSchema = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        true
    );

    BenchmarkSchemaInfo basicSchemaExpression = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggsExpression,
        basicSchemaDataInterval,
        true
    );

    SCHEMA_MAP.put("basic", basicSchema);
    SCHEMA_MAP.put("expression", basicSchemaExpression);
  }

  static { // simple single string column and count agg schema, no rollup
    List<BenchmarkColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims
        BenchmarkColumnSchema.makeSequential("dimSequential", ValueType.STRING, false, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    BenchmarkSchemaInfo basicSchema = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_MAP.put("simple", basicSchema);
  }

  static { // simple single long column and count agg schema, no rollup
    List<BenchmarkColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims, ingest as a metric for now with rollup off, until numeric dims at ingestion are supported
        BenchmarkColumnSchema.makeSequential("dimSequential", ValueType.LONG, true, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new LongSumAggregatorFactory("dimSequential", "dimSequential"));
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    BenchmarkSchemaInfo basicSchema = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_MAP.put("simpleLong", basicSchema);
  }

  static { // simple single float column and count agg schema, no rollup
    List<BenchmarkColumnSchema> basicSchemaColumns = ImmutableList.of(
        // dims, ingest as a metric for now with rollup off, until numeric dims at ingestion are supported
        BenchmarkColumnSchema.makeSequential("dimSequential", ValueType.FLOAT, true, 1, null, 0, 1000000)
    );

    List<AggregatorFactory> basicSchemaIngestAggs = new ArrayList<>();
    basicSchemaIngestAggs.add(new DoubleSumAggregatorFactory("dimSequential", "dimSequential"));
    basicSchemaIngestAggs.add(new CountAggregatorFactory("rows"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    BenchmarkSchemaInfo basicSchema = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval,
        false
    );
    SCHEMA_MAP.put("simpleFloat", basicSchema);
  }

  static { // schema with high opportunity for rollup
    List<BenchmarkColumnSchema> rolloColumns = ImmutableList.of(
        // dims
        BenchmarkColumnSchema.makeEnumerated(
            "dimEnumerated",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        BenchmarkColumnSchema.makeEnumerated(
            "dimEnumerated2",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null),
            Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        BenchmarkColumnSchema.makeZipf("dimZipf", ValueType.STRING, false, 1, null, 1, 100, 2.0),
        BenchmarkColumnSchema.makeDiscreteUniform("dimUniform", ValueType.STRING, false, 1, null, 1, 100),

        // metrics
        BenchmarkColumnSchema.makeZipf("metLongZipf", ValueType.LONG, true, 1, null, 0, 10000, 2.0),
        BenchmarkColumnSchema.makeDiscreteUniform("metLongUniform", ValueType.LONG, true, 1, null, 0, 500),
        BenchmarkColumnSchema.makeNormal("metFloatNormal", ValueType.FLOAT, true, 1, null, 5000.0, 1.0, true),
        BenchmarkColumnSchema.makeZipf("metFloatZipf", ValueType.FLOAT, true, 1, null, 0, 1000, 1.5)
    );

    List<AggregatorFactory> rolloSchemaIngestAggs = new ArrayList<>();
    rolloSchemaIngestAggs.add(new CountAggregatorFactory("rows"));
    rolloSchemaIngestAggs.add(new LongSumAggregatorFactory("sumLongSequential", "metLongSequential"));
    rolloSchemaIngestAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "metLongUniform"));
    rolloSchemaIngestAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "metFloatNormal"));
    rolloSchemaIngestAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "metFloatZipf"));
    rolloSchemaIngestAggs.add(new HyperUniquesAggregatorFactory("hyper", "dimHyperUnique"));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    BenchmarkSchemaInfo rolloSchema = new BenchmarkSchemaInfo(
        rolloColumns,
        rolloSchemaIngestAggs,
        basicSchemaDataInterval,
        true
    );
    SCHEMA_MAP.put("rollo", rolloSchema);
  }
}
