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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This is mostly a test of the validator
 */
@RunWith(Parameterized.class)
public class IndexIOTest extends InitializedNullHandlingTest
{
  private static Interval DEFAULT_INTERVAL = Intervals.of("1970-01-01/2000-01-01");
  private static final IndexSpec INDEX_SPEC = IndexMergerTestBase.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressionStrategy.LZ4,
      CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );

  static {
    NullHandling.initializeForTests();
  }

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
    final Map<String, Object> map = ImmutableMap.of();

    final Map<String, Object> map00 = ImmutableMap.of(
        "dim0", ImmutableList.of("dim00", "dim01")
    );
    final Map<String, Object> map10 = ImmutableMap.of(
        "dim1", "dim10"
    );
    final Map<String, Object> map0null = new HashMap<>();
    map0null.put("dim0", null);

    final Map<String, Object> map1null = new HashMap<>();
    map1null.put("dim1", null);

    final Map<String, Object> mapAll = ImmutableMap.of(
        "dim0", ImmutableList.of("dim00", "dim01"),
        "dim1", "dim10"
    );

    final List<Map<String, Object>> maps = ImmutableList.of(map, map00, map10, map0null, map1null, mapAll);

    return Iterables.concat(
        // First iterable tests permutations of the maps which are expected to be equal
        Iterables.concat(
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
        Iterables.concat(
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
    return Lists.transform(mapList, (Function<Map, Map>) input -> Maps.filterValues(input, Objects::nonNull));
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

  final IncrementalIndex<Aggregator> incrementalIndex1 = new OnheapIncrementalIndex.Builder()
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
      .build();

  final IncrementalIndex<Aggregator> incrementalIndex2 = new OnheapIncrementalIndex.Builder()
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
      .build();

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
      TestHelper.getTestIndexIO().validateTwoSegments(adapter1, adapter2);
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

  @Test
  public void testLoadSegmentDamagedFileWithLazy()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final IndexIO indexIO = new IndexIO(mapper, () -> 0);
    String path = this.getClass().getClassLoader().getResource("v9SegmentPersistDir/segmentWithDamagedFile/").getPath();

    ForkSegmentLoadDropHandler segmentLoadDropHandler = new ForkSegmentLoadDropHandler();
    ForkSegment segment = new ForkSegment(true);
    Assert.assertTrue(segment.getSegmentExist());
    File inDir = new File(path);
    Exception e = null;

    try {
      QueryableIndex queryableIndex = indexIO.loadIndex(inDir, true, () -> segmentLoadDropHandler.removeSegment(segment));
      Assert.assertNotNull(queryableIndex);
      queryableIndex.getDimensionHandlers();
      List<String> columnNames = queryableIndex.getColumnNames();
      for (String columnName : columnNames) {
        queryableIndex.getColumnHolder(columnName).toString();
      }
    }
    catch (Exception ex) {
      // Do nothing. Can ignore exceptions here.
      e = ex;
    }
    Assert.assertNotNull(e);
    Assert.assertFalse(segment.getSegmentExist());

  }

  private static class ForkSegmentLoadDropHandler
  {
    public void addSegment()
    {
    }
    public void removeSegment(ForkSegment segment)
    {
      segment.setSegmentExist(false);
    }
  }

  private static class ForkSegment
  {
    private Boolean segmentExist;

    ForkSegment(Boolean segmentExist)
    {
      this.segmentExist = segmentExist;
    }

    void setSegmentExist(Boolean value)
    {
      this.segmentExist = value;
    }

    Boolean getSegmentExist()
    {
      return this.segmentExist;
    }
  }
}
