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

package io.druid.query.search;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryExecutor;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;

import java.util.List;
import java.util.Map;

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
        ColumnCapabilities capabilities, ColumnValueSelector selector
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

  public interface SearchColumnSelectorStrategy<ValueSelectorType extends ColumnValueSelector>
      extends ColumnSelectorStrategy
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
      if (selector != null && !(selector instanceof NullDimensionSelector)) {
        final IndexedInts vals = selector.getRow();
        for (int i = 0; i < vals.size(); ++i) {
          final String dimVal = selector.lookupName(vals.get(i));
          if (searchQuerySpec.accept(dimVal)) {
            set.addTo(new SearchHit(outputName, Strings.nullToEmpty(dimVal)), 1);
            if (set.size() >= limit) {
              return;
            }
          }
        }
      }
    }
  }

  public static class LongSearchColumnSelectorStrategy implements SearchColumnSelectorStrategy<LongColumnSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        LongColumnSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = String.valueOf(selector.get());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  public static class FloatSearchColumnSelectorStrategy implements SearchColumnSelectorStrategy<FloatColumnSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        FloatColumnSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = String.valueOf(selector.get());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  public static class DoubleSearchColumnSelectorStrategy implements SearchColumnSelectorStrategy<DoubleColumnSelector>
  {
    @Override
    public void updateSearchResultSet(
        String outputName,
        DoubleColumnSelector selector,
        SearchQuerySpec searchQuerySpec,
        int limit,
        Object2IntRBTreeMap<SearchHit> set
    )
    {
      if (selector != null) {
        final String dimVal = String.valueOf(selector.get());
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
        }
      }
    }
  }

  @Override
  public Sequence<Result<SearchResultValue>> run(
      final QueryPlus<Result<SearchResultValue>> queryPlus,
      Map<String, Object> responseContext
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
