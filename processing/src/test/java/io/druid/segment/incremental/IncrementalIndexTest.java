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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryRunnerTestHelper;
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

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest
{

  private final StupidPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocate(256 * 1024);
        }
      }
  );

  private final AggregatorFactory[] metrics = new AggregatorFactory[]{
      new FilteredAggregatorFactory(
          new CountAggregatorFactory("cnt"),
          new SelectorDimFilter("billy", "A")
      )
  };

  interface IndexCreator
  {
    IncrementalIndex createIndex();
  }

  @Parameterized.Parameters(name = "offHeap={0}:sortResult={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(false, true), Arrays.asList(false, true));
  }

  @Rule
  public final CloserRule closer = new CloserRule(false);

  private final IndexCreator indexCreator;

  public IncrementalIndexTest(final boolean offHeap, final boolean sortResult)
  {
    indexCreator = new IndexCreator()
    {
      @Override
      public IncrementalIndex createIndex()
      {
        return IncrementalIndices.create(
            offHeap ? pool : null,
            0,
            QueryGranularity.MINUTE,
            metrics,
            true,
            true,
            sortResult,
            1000
        );
      }
    };
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
  public void testDuplicateDimensionsFirstOccurance() throws IndexSizeExceededException
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
