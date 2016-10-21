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

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexBuilder;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TestHelper;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseFilterTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<InputRow> rows;

  protected final IndexBuilder indexBuilder;
  protected final Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher;
  protected StorageAdapter adapter;
  protected Closeable closeable;
  protected boolean optimize;
  protected final String testName;

  // JUnit creates a new test instance for every test method call.
  // For filter tests, the test setup creates a segment.
  // Creating a new segment for every test method call is pretty slow, so cache the StorageAdapters.
  // Each thread gets its own map.
  protected static ThreadLocal<Map<String, Map<String, Pair<StorageAdapter, Closeable>>>> adapterCache =
      new ThreadLocal<Map<String, Map<String, Pair<StorageAdapter, Closeable>>>>()
  {
    @Override
    protected Map<String, Map<String, Pair<StorageAdapter, Closeable>>> initialValue()
    {
      return new HashMap<>();
    }
  };

  public BaseFilterTest(
      String testName,
      List<InputRow> rows,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    this.testName = testName;
    this.rows = rows;
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
    this.optimize = optimize;
  }

  @Before
  public void setUp() throws Exception
  {
    String className = getClass().getName();
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);
    if (adaptersForClass == null) {
      adaptersForClass = new HashMap<>();
      adapterCache.get().put(className, adaptersForClass);
    }

    Pair<StorageAdapter, Closeable> pair = adaptersForClass.get(testName);
    if (pair == null) {
      pair = finisher.apply(
          indexBuilder.tmpDir(temporaryFolder.newFolder()).add(rows)
      );
      adaptersForClass.put(testName, pair);
    }

    this.adapter = pair.lhs;
    this.closeable = pair.rhs;

  }

  public static void tearDown(String className) throws Exception
  {
    Map<String, Pair<StorageAdapter, Closeable>> adaptersForClass = adapterCache.get().get(className);

    if (adaptersForClass != null) {
      for (Map.Entry<String, Pair<StorageAdapter, Closeable>> entry : adaptersForClass.entrySet()) {
        Closeable closeable = entry.getValue().rhs;
        closeable.close();
      }
      adapterCache.get().put(className, null);
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    final List<Object[]> constructors = Lists.newArrayList();

    final Map<String, BitmapSerdeFactory> bitmapSerdeFactories = ImmutableMap.<String, BitmapSerdeFactory>of(
        "concise", new ConciseBitmapSerdeFactory(),
        "roaring", new RoaringBitmapSerdeFactory(true)
    );

    final Map<String, IndexMerger> indexMergers = ImmutableMap.<String, IndexMerger>of(
        "IndexMerger", TestHelper.getTestIndexMerger(),
        "IndexMergerV9", TestHelper.getTestIndexMergerV9()
    );

    final Map<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finishers = ImmutableMap.of(
        "incremental", new Function<IndexBuilder, Pair<StorageAdapter, Closeable>>()
        {
          @Override
          public Pair<StorageAdapter, Closeable> apply(IndexBuilder input)
          {
            final IncrementalIndex index = input.buildIncrementalIndex();
            return Pair.<StorageAdapter, Closeable>of(
                new IncrementalIndexStorageAdapter(index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmapped", new Function<IndexBuilder, Pair<StorageAdapter, Closeable>>()
        {
          @Override
          public Pair<StorageAdapter, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedIndex();
            return Pair.<StorageAdapter, Closeable>of(
                new QueryableIndexStorageAdapter(index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmappedMerged", new Function<IndexBuilder, Pair<StorageAdapter, Closeable>>()
        {
          @Override
          public Pair<StorageAdapter, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedMergedIndex();
            return Pair.<StorageAdapter, Closeable>of(
                new QueryableIndexStorageAdapter(index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        }
    );

    for (Map.Entry<String, BitmapSerdeFactory> bitmapSerdeFactoryEntry : bitmapSerdeFactories.entrySet()) {
      for (Map.Entry<String, IndexMerger> indexMergerEntry : indexMergers.entrySet()) {
        for (Map.Entry<String, Function<IndexBuilder, Pair<StorageAdapter, Closeable>>> finisherEntry : finishers.entrySet()) {
          for (boolean optimize : ImmutableList.of(false, true)) {
            final String testName = String.format(
                "bitmaps[%s], indexMerger[%s], finisher[%s], optimize[%s]",
                bitmapSerdeFactoryEntry.getKey(),
                indexMergerEntry.getKey(),
                finisherEntry.getKey(),
                optimize
            );
            final IndexBuilder indexBuilder = IndexBuilder.create()
                                                          .indexSpec(new IndexSpec(
                                                              bitmapSerdeFactoryEntry.getValue(),
                                                              null,
                                                              null,
                                                              null
                                                          ))
                                                          .indexMerger(indexMergerEntry.getValue());

            constructors.add(new Object[]{testName, indexBuilder, finisherEntry.getValue(), optimize});
          }
        }
      }
    }

    return constructors;
  }

  protected DimFilter maybeOptimize(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }
    return optimize ? dimFilter.optimize() : dimFilter;
  }

  protected Sequence<Cursor> makeCursorSequence(final Filter filter)
  {
    final Sequence<Cursor> cursors = adapter.makeCursors(
        filter,
        new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT),
        QueryGranularities.ALL,
        false
    );

    return cursors;
  }

  /**
   * Selects elements from "selectColumn" from rows matching a filter. selectColumn must be a single valued dimension.
   */
  protected List<String> selectColumnValuesMatchingFilter(final DimFilter filter, final String selectColumn)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(Filters.toFilter(maybeOptimize(filter)));
    Sequence<List<String>> seq = Sequences.map(
        cursors,
        new Function<Cursor, List<String>>()
        {
          @Override
          public List<String> apply(Cursor input)
          {
            final DimensionSelector selector = input.makeDimensionSelector(
                new DefaultDimensionSpec(selectColumn, selectColumn)
            );

            final List<String> values = Lists.newArrayList();

            while (!input.isDone()) {
              IndexedInts row = selector.getRow();
              Preconditions.checkState(row.size() == 1);
              values.add(selector.lookupName(row.get(0)));
              input.advance();
            }

            return values;
          }
        }
    );
    return Sequences.toList(seq, new ArrayList<List<String>>()).get(0);
  }

  protected long selectCountUsingFilteredAggregator(final DimFilter filter)
  {
    final Sequence<Cursor> cursors = makeCursorSequence(Filters.toFilter(maybeOptimize(filter)));
    Sequence<Aggregator> aggSeq = Sequences.map(
        cursors,
        new Function<Cursor, Aggregator>()
        {
          @Override
          public Aggregator apply(Cursor input)
          {
            Aggregator agg = new FilteredAggregatorFactory(
                new CountAggregatorFactory("count"),
                maybeOptimize(filter)
            ).factorize(input);

            for (; !input.isDone(); input.advance()) {
              agg.aggregate();
            }

            return agg;
          }
        }
    );
    return Sequences.toList(aggSeq, new ArrayList<Aggregator>()).get(0).getLong();
  }
}
