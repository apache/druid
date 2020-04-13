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

package org.apache.druid.query.search;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.util.List;

/**
 */
public class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
{
  public static final SearchColumnSelectorStrategyFactory SEARCH_COLUMN_SELECTOR_STRATEGY_FACTORY
      = new SearchColumnSelectorStrategyFactory();

  private final Segment segment;
  private final SearchStrategySelector strategySelector;

  public SearchQueryRunner(Segment segment, SearchStrategySelector strategySelector)
  {
    this.segment = segment;
    this.strategySelector = strategySelector;
  }

  private static class SearchColumnSelectorStrategyFactory
      implements ColumnSelectorStrategyFactory<SearchColumnSelectorStrategy>
  {
    @Override
    public SearchColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities,
        ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          return new StringSearchColumnSelectorStrategy();
        case LONG:
          return new LongSearchColumnSelectorStrategy();
        case FLOAT:
          return new FloatSearchColumnSelectorStrategy();
        case DOUBLE:
          return new DoubleSearchColumnSelectorStrategy();
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  public interface SearchColumnSelectorStrategy<ValueSelectorType> extends ColumnSelectorStrategy
  {
    /**
     * Read the current row from dimSelector and update the search result set.
     * <p>
     * For each row value:
     * 1. Check if searchQuerySpec accept()s the value
     * 2. If so, add the value to the result set and increment the counter for that value
     * 3. If the size of the result set reaches the limit after adding a value, return early.
     *
     * @param outputName      Output name for this dimension in the search query being served
     * @param dimSelector     Dimension value selector
     * @param searchQuerySpec Spec for the search query
     * @param set             The result set of the search query
     * @param limit           The limit of the search query
     */
    void updateSearchResultSet(
        String outputName,
        ValueSelectorType dimSelector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    );
  }

  public static class StringSearchColumnSelectorStrategy implements SearchColumnSelectorStrategy<DimensionSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        DimensionSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        final Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (!DimensionSelector.isNilSelector(selector)) {
        final IndexedInts row = selector.getRow();
        for (int i = 0, rowSize = row.size(); i < rowSize; ++i) {
          final String dimVal = selector.lookupName(row.get(i));
          if (searchQuerySpec.accept(dimVal)) {
            set.addTo(new SearchHit(outputName, dimVal), 1);
            if (set.size() >= limit) {
              return;
            }
          }
        }
      }
    }
  }

  public static class LongSearchColumnSelectorStrategy
      implements SearchColumnSelectorStrategy<BaseLongColumnValueSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        BaseLongColumnValueSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = selector.isNull() ? null : String.valueOf(selector.getLong());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  public static class FloatSearchColumnSelectorStrategy
      implements SearchColumnSelectorStrategy<BaseFloatColumnValueSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        BaseFloatColumnValueSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = selector.isNull() ? null : String.valueOf(selector.getFloat());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  public static class DoubleSearchColumnSelectorStrategy
      implements SearchColumnSelectorStrategy<BaseDoubleColumnValueSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        BaseDoubleColumnValueSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = selector.isNull() ? null : String.valueOf(selector.getDouble());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final QueryPlus<Result<SearchResultValue>> queryPlus,
      ResponseContext responseContext
  )
  {
    Query<Result<SearchResultValue>> input = queryPlus.getQuery();
    if (!(input instanceof SearchQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
    }

    final SearchQuery query = (SearchQuery) input;
    final List<SearchQueryExecutor> plan = strategySelector.strategize(query).getExecutionPlan(query, segment);
    final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
    retVal.defaultReturnValue(0);

    int remain = query.getLimit();
    for (final SearchQueryExecutor executor : plan) {
      retVal.putAll(executor.execute(remain));
      remain -= retVal.size();
    }

    return makeReturnResult(segment, query.getLimit(), retVal);
  }

  private static Sequence<Result<SearchResultValue>> makeReturnResult(
      Segment segment,
      int limit,
      Object2IntRBTreeMap<SearchHit> retVal
  )
  {
    Iterable<SearchHit> source = Iterables.transform(
        retVal.object2IntEntrySet(), new Function<Object2IntMap.Entry<SearchHit>, SearchHit>()
        {
          @Override
          public SearchHit apply(Object2IntMap.Entry<SearchHit> input)
          {
            SearchHit hit = input.getKey();
            return new SearchHit(hit.getDimension(), hit.getValue(), input.getIntValue());
          }
        }
    );

    return Sequences.simple(
        ImmutableList.of(
            new Result<>(
                segment.getDataInterval().getStart(),
                new SearchResultValue(
                    Lists.newArrayList(new FunctionalIterable<>(source).limit(limit))
                )
            )
        )
    );
  }
}
