/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.filter.SelectorFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
            {   new IndexCreator()
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
                        0,
                        QueryGranularity.MINUTE,
                        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
                        TestQueryRunners.pool,
                        true,
                        100 * 1024 * 1024
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
                    .setGranularity(QueryGranularity.ALL)
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
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1l), row.getEvent());

    row = (MapBasedRow) results.get(1);
    Assert.assertEquals(ImmutableMap.of("sally", "bo", "cnt", 1l), row.getEvent());
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
                    .setGranularity(QueryGranularity.ALL)
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
                            "function(a,b) { return a + b; }"
                        )
                    )
                    .build(),
        new IncrementalIndexStorageAdapter(index)
    );

    final ArrayList<Row> results = Sequences.toList(rows, Lists.<Row>newArrayList());

    Assert.assertEquals(2, results.size());

    MapBasedRow row = (MapBasedRow) results.get(0);
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1l, "fieldLength", 2.0), row.getEvent());

    row = (MapBasedRow) results.get(1);
    Assert.assertEquals(ImmutableMap.of("billy", "hip", "sally", "hop", "cnt", 1l, "fieldLength", 6.0), row.getEvent());
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
  public void testResetSanity() throws IOException{

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
    Sequence<Cursor> cursorSequence = adapter.makeCursors(new SelectorFilter("sally", "bo"),
                                                          interval,
                                                          QueryGranularity.NONE);

    Cursor cursor = Sequences.toList(Sequences.limit(cursorSequence, 1), Lists.<Cursor>newArrayList()).get(0);
    DimensionSelector dimSelector;

    dimSelector = cursor.makeDimensionSelector("sally");
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

    dimSelector = cursor.makeDimensionSelector("sally");
    Assert.assertEquals("bo", dimSelector.lookupName(dimSelector.getRow().get(0)));
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
                                  .granularity(QueryGranularity.ALL)
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
                    .setGranularity(QueryGranularity.ALL)
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
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1l), row.getEvent());
  }
}
