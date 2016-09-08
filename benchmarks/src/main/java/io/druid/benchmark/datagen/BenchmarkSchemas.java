/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.datagen;

import com.google.common.collect.ImmutableList;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.column.ValueType;
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
        BenchmarkColumnSchema.makeDiscreteUniform("dimUniform", ValueType.STRING, false, 1, null, 1, 1000000),
        BenchmarkColumnSchema.makeSequential("dimSequentialHalfNull", ValueType.STRING, false, 1, 0.5, 0, 1000),
        BenchmarkColumnSchema.makeEnumerated(
            "dimMultivalEnumerated",
            ValueType.STRING,
            false,
            4,
            null,
            Arrays.<Object>asList("Hello", "World", "Foo", "Bar", "Baz"),
            Arrays.<Double>asList(0.2, 0.25, 0.15, 0.10, 0.3)
        ),
        BenchmarkColumnSchema.makeEnumerated(
            "dimMultivalEnumerated2",
            ValueType.STRING,
            false,
            3,
            null,
            Arrays.<Object>asList("Apple", "Orange", "Xylophone", "Corundum", null),
            Arrays.<Double>asList(0.2, 0.25, 0.15, 0.10, 0.3)
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

    Interval basicSchemaDataInterval = new Interval(0, 1000000);

    BenchmarkSchemaInfo basicSchema = new BenchmarkSchemaInfo(
        basicSchemaColumns,
        basicSchemaIngestAggs,
        basicSchemaDataInterval
    );
    SCHEMA_MAP.put("basic", basicSchema);
  }
}
