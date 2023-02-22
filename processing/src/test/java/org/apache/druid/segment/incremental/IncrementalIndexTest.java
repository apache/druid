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
import org.apache.druid.java.util.common.parsers.UnparseableColumnsParseException;
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
        )
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
    IncrementalIndex index = indexCreator.createIndex();
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
    IncrementalIndex index = indexCreator.createIndex();
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
    IncrementalIndex index = indexCreator.createIndex();
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
    IncrementalIndex index = indexCreator.createIndex();

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
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=asdj, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=asdj, double=21.0}], exceptions: [could not convert value [asdj] to long]",
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
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=aaa, long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=aaa, long=20, double=21.0}], exceptions: [could not convert value [aaa] to float]",
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
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=20, double=}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=}], exceptions: [could not convert value [] to double]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void testMultiValuedNumericDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();

    IncrementalIndexAddResult result;
    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "19.0",
                "long", Arrays.asList(10L, 5L),
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=[10, 5], double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=[10, 5], double=21.0}], exceptions: [Could not ingest value [10, 5] as long. A long column cannot have multiple values in the same row.]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", Arrays.asList(10.0f, 5.0f),
                "long", 20,
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=[10.0, 5.0], long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=[10.0, 5.0], long=20, double=21.0}], exceptions: [Could not ingest value [10.0, 5.0] as float. A float column cannot have multiple values in the same row.]",
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
                "double", Arrays.asList(10.0D, 5.0D)
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=20, double=[10.0, 5.0]}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=[10.0, 5.0]}], exceptions: [Could not ingest value [10.0, 5.0] as double. A double column cannot have multiple values in the same row.]",
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
    IncrementalIndex index = indexCreator.createIndex();
    index.add(row);
    index.add(row);
    index.add(row);

    Assert.assertEquals(1, index.size());
  }
}
