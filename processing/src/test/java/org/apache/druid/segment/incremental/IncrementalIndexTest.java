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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexTest(String indexType, String mode, boolean deserializeComplexMetrics,
                              IncrementalIndexSchema schema) throws JsonProcessingException
  {
    indexCreator = closer.closeLater(new IncrementalIndexCreator(indexType, (builder, args) -> builder
        .setIndexSchema(schema)
        .setDeserializeComplexMetrics(deserializeComplexMetrics)
        .setSortFacts("rollup".equals(mode))
        .setMaxRowCount(1_000_000)
        .build())
    );
  }

  @Parameterized.Parameters(name = "{index}: {0}, {1}, deserialize={2}")
  public static Collection<?> constructorFeeder()
  {
    DimensionsSpec dimensions = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("string"),
            new FloatDimensionSchema("float"),
            new LongDimensionSchema("long"),
            new DoubleDimensionSchema("double")
        ), null, null
    );
    AggregatorFactory[] metrics = {
        new FilteredAggregatorFactory(
            new CountAggregatorFactory("cnt"),
            new SelectorDimFilter("billy", "A", null)
        )
    };
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withQueryGranularity(Granularities.MINUTE)
        .withDimensionsSpec(dimensions)
        .withMetrics(metrics)
        .build();

    return IncrementalIndexCreator.indexTypeCartesianProduct(
        ImmutableList.of("rollup", "plain"),
        ImmutableList.of(true, false),
        ImmutableList.of(schema)
    );
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex<?> index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensionsFirstOccurrence() throws IndexSizeExceededException
  {
    IncrementalIndex<?> index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void controlTest() throws IndexSizeExceededException
  {
    IncrementalIndex<?> index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "C", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void testUnparseableNumerics() throws IndexSizeExceededException
  {
    IncrementalIndex<?> index = indexCreator.createIndex();

    IncrementalIndexAddResult result;
    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "19.0",
                "long", "asdj",
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(ParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=19.0, long=asdj, double=21.0}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [asdj] to long]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "aaa",
                "long", 20,
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(ParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=aaa, long=20, double=21.0}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [aaa] to float]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", 19.0,
                "long", 20,
                "double", ""
            )
        )
    );
    Assert.assertEquals(ParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=19.0, long=20, double=}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [] to double]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void sameRow() throws IndexSizeExceededException
  {
    MapBasedInputRow row = new MapBasedInputRow(
        System.currentTimeMillis() - 1,
        Lists.newArrayList("billy", "joe"),
        ImmutableMap.of("billy", "A", "joe", "B")
    );
    IncrementalIndex<?> index = indexCreator.createIndex();
    index.add(row);
    index.add(row);
    index.add(row);

    Assert.assertEquals(1, index.size());
  }
}
