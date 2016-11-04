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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.CloserRule;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
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
  public final CloserRule closer = new CloserRule(false);

  private final IndexCreator indexCreator;

  public IncrementalIndexTest(IndexCreator IndexCreator)
  {
    this.indexCreator = IndexCreator;
  }

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    DimensionsSpec dimensions = new DimensionsSpec(
        Arrays.<DimensionSchema>asList(
            new StringDimensionSchema("string"),
            new StringDimensionSchema("float"),
            new StringDimensionSchema("long")
        ), null, null
    );
    AggregatorFactory[] metrics = {
        new FilteredAggregatorFactory(
            new CountAggregatorFactory("cnt"),
            new SelectorDimFilter("billy", "A", null)
        )
    };
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(0)
        .withQueryGranularity(QueryGranularities.MINUTE)
        .withDimensionsSpec(dimensions)
        .withMetrics(metrics)
        .withRollup(true)
        .build();

    final List<Object[]> constructors = Lists.newArrayList();
    for (final Boolean sortFacts : ImmutableList.of(false, true)) {
      constructors.add(
          new Object[]{
              new IndexCreator()
              {
                @Override
                public IncrementalIndex createIndex()
                {
                  return new OnheapIncrementalIndex(schema, false, true, sortFacts, 1000);
                }
              }
          }
      );
      constructors.add(
          new Object[]{
              new IndexCreator()
              {
                @Override
                public IncrementalIndex createIndex()
                {
                  return new OffheapIncrementalIndex(
                      schema, true, true, sortFacts, 1000000, new StupidPool<ByteBuffer>(
                      new Supplier<ByteBuffer>()
                      {
                        @Override
                        public ByteBuffer get()
                        {
                          return ByteBuffer.allocate(256 * 1024);
                        }
                      }
                  )
                  );
                }
              }
          }
      );
    }

    return constructors;
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex index = closer.closeLater(indexCreator.createIndex());
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensionsFirstOccurrence() throws IndexSizeExceededException
  {
    IncrementalIndex index = closer.closeLater(indexCreator.createIndex());
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void controlTest() throws IndexSizeExceededException
  {
    IncrementalIndex index = closer.closeLater(indexCreator.createIndex());
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.<String, Object>of("billy", "C", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void testNullDimensionTransform() throws IndexSizeExceededException
  {
    IncrementalIndex<?> index = closer.closeLater(indexCreator.createIndex());
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("string", "float", "long"),
            ImmutableMap.<String, Object>of(
                "string", Arrays.asList("A", null, ""),
                "float", Arrays.asList(Float.MAX_VALUE, null, ""),
                "long", Arrays.asList(Long.MIN_VALUE, null, "")
            )
        )
    );

    Row row = index.iterator().next();

    Assert.assertEquals(Arrays.asList(new String[]{"", "", "A"}), row.getRaw("string"));
    Assert.assertEquals(Arrays.asList(new String[]{"", "", String.valueOf(Float.MAX_VALUE)}), row.getRaw("float"));
    Assert.assertEquals(Arrays.asList(new String[]{"", "", String.valueOf(Long.MIN_VALUE)}), row.getRaw("long"));
  }

  @Test
  public void sameRow() throws IndexSizeExceededException
  {
    MapBasedInputRow row = new MapBasedInputRow(
        new DateTime().minus(1).getMillis(),
        Lists.newArrayList("billy", "joe"),
        ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
    );
    IncrementalIndex index = closer.closeLater(indexCreator.createIndex());
    index.add(row);
    index.add(row);
    index.add(row);

    Assert.assertEquals(1, index.size());
  }
}
