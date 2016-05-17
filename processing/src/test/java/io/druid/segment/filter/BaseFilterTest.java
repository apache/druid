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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularities;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
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

  public BaseFilterTest(
      List<InputRow> rows,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    this.rows = rows;
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
    this.optimize = optimize;
  }

  @Before
  public void setUp() throws Exception
  {
    final Pair<StorageAdapter, Closeable> pair = finisher.apply(
        indexBuilder.tmpDir(temporaryFolder.newFolder()).add(rows)
    );
    this.adapter = pair.lhs;
    this.closeable = pair.rhs;
  }

  @After
  public void tearDown() throws Exception
  {
    closeable.close();
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
        "roaring", new RoaringBitmapSerdeFactory()
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

  /**
   * Selects elements from "selectColumn" from rows matching a filter. selectColumn must be a single valued dimension.
   */
  protected List<String> selectColumnValuesMatchingFilter(final DimFilter filter, final String selectColumn)
  {
    final Cursor cursor = makeCursor(Filters.toFilter(maybeOptimize(filter)));
    final List<String> values = Lists.newArrayList();
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(selectColumn, selectColumn)
    );

    for (; !cursor.isDone(); cursor.advance()) {
      final IndexedInts row = selector.getRow();
      Preconditions.checkState(row.size() == 1);
      values.add(selector.lookupName(row.get(0)));
    }

    return values;
  }

  protected long selectCountUsingFilteredAggregator(final DimFilter filter)
  {
    final Cursor cursor = makeCursor(null);
    final Aggregator agg = new FilteredAggregatorFactory(
        new CountAggregatorFactory("count"),
        maybeOptimize(filter)
    ).factorize(cursor);

    for (; !cursor.isDone(); cursor.advance()) {
      agg.aggregate();
    }

    return agg.getLong();
  }

  private DimFilter maybeOptimize(final DimFilter dimFilter)
  {
    if (dimFilter == null) {
      return null;
    }
    return optimize ? dimFilter.optimize() : dimFilter;
  }

  private Cursor makeCursor(final Filter filter)
  {
    final Sequence<Cursor> cursors = adapter.makeCursors(
        filter,
        new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT),
        QueryGranularities.ALL,
        false
    );
    return Iterables.getOnlyElement(Sequences.toList(cursors, Lists.<Cursor>newArrayList()));
  }
}
