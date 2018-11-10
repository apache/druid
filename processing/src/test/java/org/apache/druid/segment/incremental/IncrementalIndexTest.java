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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.CloserRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest
{

  interface IndexCreator
  {
    IncrementalIndex createIndex();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final CloserRule closerRule = new CloserRule(false);

  private final IndexCreator indexCreator;
  private final Closer resourceCloser;

  @After
  public void teardown() throws IOException
  {
    resourceCloser.close();
  }

  public IncrementalIndexTest(IndexCreator IndexCreator, Closer resourceCloser)
  {
    this.indexCreator = IndexCreator;
    this.resourceCloser = resourceCloser;
  }

  @Parameterized.Parameters
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

    final List<Object[]> constructors = new ArrayList<>();
    for (final Boolean sortFacts : ImmutableList.of(false, true)) {
      constructors.add(
          new Object[]{
              new IndexCreator()
              {
                @Override
                public IncrementalIndex createIndex()
                {
                  return new IncrementalIndex.Builder()
                      .setIndexSchema(schema)
                      .setDeserializeComplexMetrics(false)
                      .setSortFacts(sortFacts)
                      .setMaxRowCount(1000)
                      .buildOnheap();
                }
              },
              Closer.create()
          }
      );
      final Closer poolCloser = Closer.create();
      final CloseableStupidPool<ByteBuffer> stupidPool = new CloseableStupidPool<>(
          "OffheapIncrementalIndex-bufferPool",
          () -> ByteBuffer.allocate(256 * 1024)
      );
      poolCloser.register(stupidPool);
      constructors.add(
          new Object[]{
              new IndexCreator()
              {
                @Override
                public IncrementalIndex createIndex()
                {
                  return new IncrementalIndex.Builder()
                      .setIndexSchema(schema)
                      .setSortFacts(sortFacts)
                      .setMaxRowCount(1000000)
                      .buildOffheap(stupidPool);
                }
              },
              poolCloser
          }
      );
    }

    return constructors;
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex());
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
    IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex());
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
    IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex());
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
    IncrementalIndex<?> index = closerRule.closeLater(indexCreator.createIndex());

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
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=19.0, long=asdj, double=21.0}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [asdj] to long,]",
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
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=aaa, long=20, double=21.0}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [aaa] to float,]",
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
        "Found unparseable columns in row: [MapBasedInputRow{timestamp=1970-01-01T00:00:00.000Z, event={string=A, float=19.0, long=20, double=}, dimensions=[string, float, long, double]}], exceptions: [could not convert value [] to double,]",
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
    IncrementalIndex index = closerRule.closeLater(indexCreator.createIndex());
    index.add(row);
    index.add(row);
    index.add(row);

    Assert.assertEquals(1, index.size());
  }
}
