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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.UOE;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is mostly a test of the validator
 */
@RunWith(Parameterized.class)
public class IndexIOTest
{
  private static Interval DEFAULT_INTERVAL = Intervals.of("1970-01-01/2000-01-01");
  private static final IndexSpec INDEX_SPEC = IndexMergerTestBase.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressionStrategy.LZ4,
      CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );

  private static <T> List<T> filterByBitset(List<T> list, BitSet bitSet)
  {
    final ArrayList<T> outList = new ArrayList<>(bitSet.cardinality());
    for (int i = 0; i < list.size(); ++i) {
      if (bitSet.get(i)) {
        outList.add(list.get(i));
      }
    }
    return outList;
  }

  @Parameterized.Parameters(name = "{0}, {1}")
  public static Iterable<Object[]> constructionFeeder()
  {

    final Map<String, Object> map = ImmutableMap.<String, Object>of();

    final Map<String, Object> map00 = ImmutableMap.<String, Object>of(
        "dim0", ImmutableList.<String>of("dim00", "dim01")
    );
    final Map<String, Object> map10 = ImmutableMap.<String, Object>of(
        "dim1", "dim10"
    );
    final Map<String, Object> map0null = new HashMap<>();
    map0null.put("dim0", null);

    final Map<String, Object> map1null = new HashMap<>();
    map1null.put("dim1", null);

    final Map<String, Object> mapAll = ImmutableMap.<String, Object>of(
        "dim0", ImmutableList.<String>of("dim00", "dim01"),
        "dim1", "dim10"
    );

    final List<Map<String, Object>> maps = ImmutableList.of(
        map, map00, map10, map0null, map1null, mapAll
    );

    return Iterables.<Object[]>concat(
        // First iterable tests permutations of the maps which are expected to be equal
        Iterables.<Object[]>concat(
            new Iterable<Iterable<Object[]>>()
            {
              @Override
              public Iterator<Iterable<Object[]>> iterator()
              {
                return new Iterator<Iterable<Object[]>>()
                {
                  long nextBitset = 1L;

                  @Override
                  public boolean hasNext()
                  {
                    return nextBitset < (1L << maps.size());
                  }

                  @Override
                  public Iterable<Object[]> next()
                  {
                    final BitSet bitset = BitSet.valueOf(new long[]{nextBitset++});
                    final List<Map<String, Object>> myMaps = filterByBitset(maps, bitset);
                    return Collections2.transform(
                        Collections2.permutations(myMaps), new Function<List<Map<String, Object>>, Object[]>()
                        {
                          @Nullable
                          @Override
                          public Object[] apply(List<Map<String, Object>> input)
                          {
                            return new Object[]{input, input, null};
                          }
                        }
                    );
                  }

                  @Override
                  public void remove()
                  {
                    throw new UOE("Remove not suported");
                  }
                };
              }
            }
        ),
        // Second iterable tests combinations of the maps which may or may not be equal
        Iterables.<Object[]>concat(
            new Iterable<Iterable<Object[]>>()
            {
              @Override
              public Iterator<Iterable<Object[]>> iterator()
              {
                return new Iterator<Iterable<Object[]>>()
                {
                  long nextMap1Bits = 1L;

                  @Override
                  public boolean hasNext()
                  {
                    return nextMap1Bits < (1L << maps.size());
                  }

                  @Override
                  public Iterable<Object[]> next()
                  {
                    final BitSet bitset1 = BitSet.valueOf(new long[]{nextMap1Bits++});
                    final List<Map<String, Object>> maplist1 = filterByBitset(maps, bitset1);
                    return new Iterable<Object[]>()
                    {
                      @Override
                      public Iterator<Object[]> iterator()
                      {
                        return new Iterator<Object[]>()
                        {
                          long nextMap2Bits = 1L;

                          @Override
                          public boolean hasNext()
                          {
                            return nextMap2Bits < (1L << maps.size());
                          }

                          @Override
                          public Object[] next()
                          {
                            final List<Map<String, Object>> maplist2 = filterByBitset(
                                maps,
                                BitSet.valueOf(
                                    new long[]{nextMap2Bits++}
                                )
                            );
                            return new Object[]{
                                maplist1,
                                maplist2,
                                filterNullValues(maplist1).equals(filterNullValues(maplist2)) ?
                                    null : SegmentValidationException.class
                            };
                          }

                          @Override
                          public void remove()
                          {
                            throw new UOE("remove not supported");
                          }
                        };
                      }
                    };
                  }

                  @Override
                  public void remove()
                  {
                    throw new UOE("Remove not supported");
                  }
                };
              }
            }
        )
    );
  }

  public static List<Map> filterNullValues(List<Map<String, Object>> mapList)
  {
    return Lists.transform(
        mapList, new Function<Map, Map>()
        {
          @Nullable
          @Override
          public Map apply(@Nullable Map input)
          {
            return Maps.filterValues(input, Predicates.notNull());
          }
        }
    );
  }

  private final Collection<Map<String, Object>> events1;
  private final Collection<Map<String, Object>> events2;
  private final Class<? extends Exception> exception;

  public IndexIOTest(
      Collection<Map<String, Object>> events1,
      Collection<Map<String, Object>> events2,
      Class<? extends Exception> exception
  )
  {
    this.events1 = events1;
    this.events2 = events2;
    this.exception = exception;
  }

  final IncrementalIndex<Aggregator> incrementalIndex1 = new IncrementalIndex.Builder()
      .setIndexSchema(
          new IncrementalIndexSchema.Builder()
              .withMinTimestamp(DEFAULT_INTERVAL.getStart().getMillis())
              .withMetrics(new CountAggregatorFactory("count"))
              .withDimensionsSpec(
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(Arrays.asList("dim0", "dim1")),
                      null,
                      null
                  )
              )
              .build()
      )
      .setMaxRowCount(1000000)
      .buildOnheap();

  final IncrementalIndex<Aggregator> incrementalIndex2 = new IncrementalIndex.Builder()
      .setIndexSchema(
          new IncrementalIndexSchema.Builder()
              .withMinTimestamp(DEFAULT_INTERVAL.getStart().getMillis())
              .withMetrics(new CountAggregatorFactory("count"))
              .withDimensionsSpec(
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(Arrays.asList("dim0", "dim1")),
                      null,
                      null
                  )
              )
              .build()
      )
      .setMaxRowCount(1000000)
      .buildOnheap();

  IndexableAdapter adapter1;
  IndexableAdapter adapter2;

  @Before
  public void setUp() throws IndexSizeExceededException
  {
    long timestamp = 0L;
    for (Map<String, Object> event : events1) {
      incrementalIndex1.add(new MapBasedInputRow(timestamp++, Lists.newArrayList(event.keySet()), event));
    }

    timestamp = 0L;
    for (Map<String, Object> event : events2) {
      incrementalIndex2.add(new MapBasedInputRow(timestamp++, Lists.newArrayList(event.keySet()), event));
    }

    adapter2 = new IncrementalIndexAdapter(
        DEFAULT_INTERVAL,
        incrementalIndex2,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );

    adapter1 = new IncrementalIndexAdapter(
        DEFAULT_INTERVAL,
        incrementalIndex1,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );
  }

  @Test
  public void testRowValidatorEquals() throws Exception
  {
    Exception ex = null;
    try {
      TestHelper.getTestIndexIO(OffHeapMemorySegmentWriteOutMediumFactory.instance()).validateTwoSegments(adapter1, adapter2);
    }
    catch (Exception e) {
      ex = e;
    }
    if (exception != null) {
      Assert.assertNotNull("Exception was not thrown", ex);
      if (!exception.isAssignableFrom(ex.getClass())) {
        throw ex;
      }
    } else {
      if (ex != null) {
        throw ex;
      }
    }
  }
}
