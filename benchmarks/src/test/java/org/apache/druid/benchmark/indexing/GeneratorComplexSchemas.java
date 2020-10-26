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

package org.apache.druid.benchmark.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GeneratorComplexSchemas
{
  private static final ImmutableMap.Builder<String, GeneratorSchemaInfo> SCHEMA_INFO_BUILDER = ImmutableMap.builder();


  static {
    // schema with high opportunity for rollup
    final GeneratorColumnSchema generatorColumnSchema = GeneratorColumnSchema.makeEnumerated(
        "dimEnumerated",
        ValueType.STRING,
        false,
        1,
        null,
        Arrays.asList("Hello", "World", "Foo", "Bar", "Baz"),
        Arrays.asList(0.2, 0.25, 0.15, 0.10, 0.3)
    );
    List<GeneratorColumnSchema> rolloColumns = ImmutableList.of(
        // dims
        generatorColumnSchema,
        GeneratorColumnSchema.makeZipf("dimZipf", ValueType.STRING, false, 1, null, 1, 100, 2.0),

        // metrics
        GeneratorColumnSchema.makeZipf("theta01", ValueType.LONG, true, 1, null, 0, 10000, 2.0),
        GeneratorColumnSchema.makeDiscreteUniform("quantilies01", ValueType.LONG, true, 1, null, 0, 500)
    );

    List<AggregatorFactory> rolloSchemaIngestAggs = new ArrayList<>();
    rolloSchemaIngestAggs.add(new CountAggregatorFactory("rows"));
    rolloSchemaIngestAggs.add(new SketchMergeAggregatorFactory("theta01", "theta01",
        null, null, null, null
    ));
    rolloSchemaIngestAggs.add(new DoublesSketchAggregatorFactory("quantilies01", "quantilies01", null));

    Interval basicSchemaDataInterval = Intervals.utc(0, 1000000);

    GeneratorSchemaInfo rolloSchema = new GeneratorSchemaInfo(
        rolloColumns,
        rolloSchemaIngestAggs,
        basicSchemaDataInterval,
        true
    );
    SCHEMA_INFO_BUILDER.put("complex", rolloSchema);
  }


  public static final Map<String, GeneratorSchemaInfo> SCHEMA_MAP = SCHEMA_INFO_BUILDER.build();
}
