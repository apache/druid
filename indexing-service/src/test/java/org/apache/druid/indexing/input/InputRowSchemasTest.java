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

package org.apache.druid.indexing.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandlingTest;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class InputRowSchemasTest extends NullHandlingTest
{
  @Test
  public void test_createColumnsFilter_normal()
  {
    final ColumnsFilter columnsFilter = InputRowSchemas.createColumnsFilter(
        new TimestampSpec("ts", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(StringDimensionSchema.create("foo"))
        ),
        new TransformSpec(
            new SelectorDimFilter("bar", "x", null),
            ImmutableList.of(
                new ExpressionTransform("baz", "qux + 3", ExprMacroTable.nil())
            )
        ),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("billy", "bob")
        }
    );

    Assert.assertEquals(
        ColumnsFilter.inclusionBased(
            ImmutableSet.of(
                "ts",
                "foo",
                "bar",
                "qux",
                "bob"
            )
        ),
        columnsFilter
    );
  }

  @Test
  public void test_createColumnsFilter_schemaless()
  {
    final ColumnsFilter columnsFilter = InputRowSchemas.createColumnsFilter(
        new TimestampSpec("ts", "auto", null),
        DimensionsSpec.builder()
                      .setDimensionExclusions(ImmutableList.of("ts", "foo", "bar", "qux", "bob"))
                      .build(),
        new TransformSpec(
            new SelectorDimFilter("bar", "x", null),
            ImmutableList.of(
                new ExpressionTransform("baz", "qux + 3", ExprMacroTable.nil())
            )
        ),
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("billy", "bob")
        }
    );

    Assert.assertEquals(
        ColumnsFilter.exclusionBased(
            ImmutableSet.of(
                "foo"
            )
        ),
        columnsFilter
    );
  }

  @Test
  public void testFromDataSchema()
  {
    TimestampSpec timestampSpec = new TimestampSpec(null, null, null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2"),
            new LongDimensionSchema("d3"),
            new FloatDimensionSchema("d4"),
            new DoubleDimensionSchema("d5")
        )
    );
    DataSchema schema = new DataSchema(
        "dataSourceName",
        new TimestampSpec(null, null, null),
        dimensionsSpec,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("met", "met")
        },
        new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null),
        null
    );

    InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(schema);
    Assert.assertEquals(timestampSpec, inputRowSchema.getTimestampSpec());
    Assert.assertEquals(dimensionsSpec.getDimensions(), inputRowSchema.getDimensionsSpec().getDimensions());
    Assert.assertEquals(dimensionsSpec.getDimensionNames(), inputRowSchema.getDimensionsSpec().getDimensionNames());
    Assert.assertEquals(ImmutableSet.of("count", "met"), inputRowSchema.getMetricNames());
  }

  @Test
  public void testFromDataSchemaWithNoAggregator()
  {
    TimestampSpec timestampSpec = new TimestampSpec(null, null, null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2"),
            new LongDimensionSchema("d3"),
            new FloatDimensionSchema("d4"),
            new DoubleDimensionSchema("d5")
        )
    );
    DataSchema schema = new DataSchema(
        "dataSourceName",
        new TimestampSpec(null, null, null),
        dimensionsSpec,
        new AggregatorFactory[]{},
        new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null),
        null
    );

    InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(schema);
    Assert.assertEquals(timestampSpec, inputRowSchema.getTimestampSpec());
    Assert.assertEquals(dimensionsSpec.getDimensions(), inputRowSchema.getDimensionsSpec().getDimensions());
    Assert.assertEquals(dimensionsSpec.getDimensionNames(), inputRowSchema.getDimensionsSpec().getDimensionNames());
    Assert.assertEquals(ImmutableSet.of(), inputRowSchema.getMetricNames());
  }
}
