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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.SelectorFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexStorageAdapterTest
{
  interface IndexCreator
  {
    public IncrementalIndex createIndex();
  }

  private final IndexCreator indexCreator;

  public IncrementalIndexStorageAdapterTest(
      IndexCreator IndexCreator
  )
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
                        0, QueryGranularities.MINUTE, new AggregatorFactory[]{new CountAggregatorFactory("cnt")}, 1000
                    );
                  }
                }
            }
        }
    );
  }

  @Test
  public void testSanity() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy"),
            ImmutableMap.<String, Object>of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("sally"),
            ImmutableMap.<String, Object>of("sally", "bo")
        )
    );

    GroupByQueryEngine engine = makeGroupByQueryEngine();

    final Sequence<Row> rows = engine.process(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(QueryGranularities.ALL)
                    .setInterval(new Interval(0, new DateTime().getMillis()))
                    .addDimension("billy")
                    .addDimension("sally")
                    .addAggregator(new LongSumAggregatorFactory("cnt", "cnt"))
                    .build(),
        new IncrementalIndexStorageAdapter(index)
    );

    final ArrayList<Row> results = Sequences.toList(rows, Lists.<Row>newArrayList());

    Assert.assertEquals(2, results.size());

    MapBasedRow row = (MapBasedRow) results.get(0);
    Assert.assertEquals(ImmutableMap.of("sally", "bo", "cnt", 1L), row.getEvent());

    row = (MapBasedRow) results.get(1);
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1L), row.getEvent());
  }

  @Test
  public void testObjectColumnSelectorOnVaryingColumnSchema() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            new DateTime("2014-09-01T00:00:00"),
            Lists.newArrayList("billy"),
            ImmutableMap.<String, Object>of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime("2014-09-01T01:00:00"),
            Lists.newArrayList("billy", "sally"),
            ImmutableMap.<String, Object>of(
                "billy", "hip",
                "sally", "hop"
            )
        )
    );

    GroupByQueryEngine engine = makeGroupByQueryEngine();

    final Sequence<Row> rows = engine.process(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(QueryGranularities.ALL)
                    .setInterval(new Interval(0, new DateTime().getMillis()))
                    .addDimension("billy")
                    .addDimension("sally")
                    .addAggregator(
                        new LongSumAggregatorFactory("cnt", "cnt")
                    )
                    .addAggregator(
                        new JavaScriptAggregatorFactory(
                            "fieldLength",
                            Arrays.asList("sally", "billy"),
                            "function(current, s, b) { return current + (s == null ? 0 : s.length) + (b == null ? 0 : b.length); }",
                            "function() { return 0; }",
                            "function(a,b) { return a + b; }",
                            JavaScriptConfig.getDefault()
                        )
                    )
                    .build(),
        new IncrementalIndexStorageAdapter(index)
    );

    final ArrayList<Row> results = Sequences.toList(rows, Lists.<Row>newArrayList());

    Assert.assertEquals(2, results.size());

    MapBasedRow row = (MapBasedRow) results.get(0);
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1L, "fieldLength", 2.0), row.getEvent());

    row = (MapBasedRow) results.get(1);
    Assert.assertEquals(ImmutableMap.of("billy", "hip", "sally", "hop", "cnt", 1L, "fieldLength", 6.0), row.getEvent());
  }

  private static GroupByQueryEngine makeGroupByQueryEngine()
  {
    return new GroupByQueryEngine(
        Suppliers.<GroupByQueryConfig>ofInstance(
            new GroupByQueryConfig()
            {
              @Override
              public int getMaxIntermediateRows()
              {
                return 5;
              }
            }
        ),
        new StupidPool(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(50000);
              }
            }
        )
    );
  }

  @Test
  public void testResetSanity() throws IOException
  {

    IncrementalIndex index = indexCreator.createIndex();
    DateTime t = DateTime.now();
    Interval interval = new Interval(t.minusMinutes(1), t.plusMinutes(1));

    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Lists.newArrayList("billy"),
            ImmutableMap.<String, Object>of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Lists.newArrayList("sally"),
            ImmutableMap.<String, Object>of("sally", "bo")
        )
    );

    IncrementalIndexStorageAdapter adapter = new IncrementalIndexStorageAdapter(index);

    for (boolean descending : Arrays.asList(false, true)) {
      Sequence<Cursor> cursorSequence = adapter.makeCursors(
          new SelectorFilter("sally", "bo"),
          interval,
          QueryGranularities.NONE,
          descending
      );

      Cursor cursor = Sequences.toList(Sequences.limit(cursorSequence, 1), Lists.<Cursor>newArrayList()).get(0);
      DimensionSelector dimSelector;

      dimSelector = cursor.makeDimensionSelector(new DefaultDimensionSpec("sally", "sally"));
      Assert.assertEquals("bo", dimSelector.lookupName(dimSelector.getRow().get(0)));

      index.add(
          new MapBasedInputRow(
              t.minus(1).getMillis(),
              Lists.newArrayList("sally"),
              ImmutableMap.<String, Object>of("sally", "ah")
          )
      );

      // Cursor reset should not be affected by out of order values
      cursor.reset();

      dimSelector = cursor.makeDimensionSelector(new DefaultDimensionSpec("sally", "sally"));
      Assert.assertEquals("bo", dimSelector.lookupName(dimSelector.getRow().get(0)));
    }
  }

  @Test
  public void testSingleValueTopN() throws IOException
  {
    IncrementalIndex index = indexCreator.createIndex();
    DateTime t = DateTime.now();
    index.add(
        new MapBasedInputRow(
            t.minus(1).getMillis(),
            Lists.newArrayList("sally"),
            ImmutableMap.<String, Object>of("sally", "bo")
        )
    );

    TopNQueryEngine engine = new TopNQueryEngine(
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(50000);
              }
            }
        )
    );

    final Iterable<Result<TopNResultValue>> results = Sequences.toList(
        engine.query(
            new TopNQueryBuilder().dataSource("test")
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(Lists.newArrayList(new Interval(0, new DateTime().getMillis())))
                                  .dimension("sally")
                                  .metric("cnt")
                                  .threshold(10)
                                  .aggregators(
                                      Lists.<AggregatorFactory>newArrayList(
                                          new LongSumAggregatorFactory(
                                              "cnt",
                                              "cnt"
                                          )
                                      )
                                  )
                                  .build(),
            new IncrementalIndexStorageAdapter(index)
        ),
        Lists.<Result<TopNResultValue>>newLinkedList()
    );

    Assert.assertEquals(1, Iterables.size(results));
    Assert.assertEquals(1, results.iterator().next().getValue().getValue().size());
  }

  @Test
  public void testFilterByNull() throws Exception
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy"),
            ImmutableMap.<String, Object>of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("sally"),
            ImmutableMap.<String, Object>of("sally", "bo")
        )
    );

    GroupByQueryEngine engine = makeGroupByQueryEngine();

    final Sequence<Row> rows = engine.process(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(QueryGranularities.ALL)
                    .setInterval(new Interval(0, new DateTime().getMillis()))
                    .addDimension("billy")
                    .addDimension("sally")
                    .addAggregator(new LongSumAggregatorFactory("cnt", "cnt"))
                    .setDimFilter(DimFilters.dimEquals("sally", (String) null))
                    .build(),
        new IncrementalIndexStorageAdapter(index)
    );

    final ArrayList<Row> results = Sequences.toList(rows, Lists.<Row>newArrayList());

    Assert.assertEquals(1, results.size());

    MapBasedRow row = (MapBasedRow) results.get(0);
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1L), row.getEvent());
  }

  @Test
  public void testCursoringAndIndexUpdationInterleaving() throws Exception
  {
    final IncrementalIndex index = indexCreator.createIndex();
    final long timestamp = System.currentTimeMillis();

    for (int i = 0; i < 2; i++) {
      index.add(
          new MapBasedInputRow(
              timestamp,
              Lists.newArrayList("billy"),
              ImmutableMap.<String, Object>of("billy", "v1" + i)
          )
      );
    }

    final StorageAdapter sa = new IncrementalIndexStorageAdapter(index);

    Sequence<Cursor> cursors = sa.makeCursors(
        null, new Interval(timestamp - 60_000, timestamp + 60_000), QueryGranularities.ALL, false
    );

    Sequences.toList(
        Sequences.map(
            cursors,
            new Function<Cursor, Object>()
            {
              @Nullable
              @Override
              public Object apply(Cursor cursor)
              {
                DimensionSelector dimSelector = cursor.makeDimensionSelector(
                    new DefaultDimensionSpec(
                        "billy",
                        "billy"
                    )
                );
                int cardinality = dimSelector.getValueCardinality();

                //index gets more rows at this point, while other thread is iterating over the cursor
                try {
                  for (int i = 0; i < 1; i++) {
                    index.add(
                        new MapBasedInputRow(
                            timestamp,
                            Lists.newArrayList("billy"),
                            ImmutableMap.<String, Object>of("billy", "v2" + i)
                        )
                    );
                  }
                }
                catch (Exception ex) {
                  throw new RuntimeException(ex);
                }

                // and then, cursoring continues in the other thread
                while (!cursor.isDone()) {
                  IndexedInts row = dimSelector.getRow();
                  for (int i : row) {
                    Assert.assertTrue(i < cardinality);
                  }
                  cursor.advance();
                }

                return null;
              }
            }
        ),
        new ArrayList<>()
    );
  }
}
