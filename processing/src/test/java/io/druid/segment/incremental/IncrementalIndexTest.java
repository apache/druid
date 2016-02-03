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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.CloserRule;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

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
    return Arrays.asList(
        new Object[][]{
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex()
                  {
                    return new OnheapIncrementalIndex(
                        0, QueryGranularity.MINUTE, new AggregatorFactory[]{new CountAggregatorFactory("cnt")}, 1000
                    );
                  }
                }

            },
            {
                new IndexCreator()
                {
                  @Override
                  public IncrementalIndex createIndex()
                  {
                    return new OffheapIncrementalIndex(
                        0L,
                        QueryGranularity.NONE,
                        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
                        1000000,
                        new StupidPool<ByteBuffer>(
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

        }
    );
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
            ImmutableMap.<String, Object>of("billy", "A", "joe", "B")
        )
    );
  }
}
