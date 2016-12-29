package io.druid.query.search.search;

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.search.SearchQueryRunner;
import io.druid.query.search.SearchQueryRunner.SearchColumnSelectorStrategy;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

public class CursorBasedStrategy extends SearchStrategy
{
  public static final String NAME = "cursorBased";

  public CursorBasedStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();
    final List<DimensionSpec> dimensionSpecs = getDimsToSearch(adapter.getAvailableDimensions(), query.getDimensions());
    return ImmutableList.<SearchQueryExecutor>of(new CursorBasedExecutor(query,
                                                                         segment,
                                                                         filter,
                                                                         interval,
                                                                         dimensionSpecs));
  }

  public static class CursorBasedExecutor extends SearchQueryExecutor {

    protected Filter filter;
    protected Interval interval;

    public CursorBasedExecutor(SearchQuery query,
                               Segment segment,
                               Filter filter,
                               Interval interval, List<DimensionSpec> dimensionSpecs)
    {
      super(query, segment, dimensionSpecs);

      this.filter = filter;
      this.interval = interval;
    }

    @Override
    public Object2IntRBTreeMap<SearchHit> execute(final int limit)
    {
      final StorageAdapter adapter = segment.asStorageAdapter();

      final Sequence<Cursor> cursors = adapter.makeCursors(
          filter,
          interval,
          VirtualColumns.EMPTY,
          query.getGranularity(),
          query.isDescending()
      );

      final Object2IntRBTreeMap<SearchHit> retVal = new Object2IntRBTreeMap<>(query.getSort().getComparator());
      retVal.defaultReturnValue(0);

      cursors.accumulate(
          retVal,
          new Accumulator<Object2IntRBTreeMap<SearchHit>, Cursor>()
          {
            @Override
            public Object2IntRBTreeMap<SearchHit> accumulate(Object2IntRBTreeMap<SearchHit> set, Cursor cursor)
            {
              if (set.size() >= limit) {
                return set;
              }

              List<ColumnSelectorPlus<SearchColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                  DimensionHandlerUtils.createColumnSelectorPluses(
                      SearchQueryRunner.SEARCH_COLUMN_SELECTOR_STRATEGY_FACTORY,
                      dimsToSearch,
                      cursor
                  )
              );

              while (!cursor.isDone()) {
                for (ColumnSelectorPlus<SearchColumnSelectorStrategy> selectorPlus : selectorPlusList) {
                  selectorPlus.getColumnSelectorStrategy().updateSearchResultSet(
                      selectorPlus.getOutputName(),
                      selectorPlus.getSelector(),
                      searchQuerySpec,
                      limit,
                      set
                  );

                  if (set.size() >= limit) {
                    return set;
                  }
                }

                cursor.advance();
              }

              return set;
            }
          }
      );

      return retVal;
    }
  }
}
