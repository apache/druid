package io.druid.query.search.search;

import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.search.SearchResultValue;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Map;
import java.util.TreeMap;

public class CursorBasedStrategy extends SearchStrategy
{
  public static final String NAME = "cursorBased";

  private static final EmittingLogger log = new EmittingLogger(CursorBasedStrategy.class);

  public CursorBasedStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public SearchQueryExecutor getExecutionPlan(SearchQuery query, Segment segment)
  {
    log.debug("Cursor-based execution strategy is selected");
    return new CursorBasedExecutor(query, segment, filter, interval);
  }

  public static class CursorBasedExecutor extends SearchQueryExecutor {

    protected Filter filter;
    protected Interval interval;

    public CursorBasedExecutor(SearchQuery query, Segment segment, Filter filter, Interval interval)
    {
      super(query, segment);

      this.filter = filter;
      this.interval = interval;
    }

    @Override
    public Sequence<Result<SearchResultValue>> execute()
    {
      final StorageAdapter adapter = segment.asStorageAdapter();

      final Sequence<Cursor> cursors = adapter.makeCursors(
          filter,
          interval,
          VirtualColumns.EMPTY,
          query.getGranularity(),
          query.isDescending()
      );

      final TreeMap<SearchHit, MutableInt> retVal = cursors.accumulate(
          Maps.<SearchHit, SearchHit, MutableInt>newTreeMap(query.getSort().getComparator()),
          new Accumulator<TreeMap<SearchHit, MutableInt>, Cursor>()
          {
            @Override
            public TreeMap<SearchHit, MutableInt> accumulate(TreeMap<SearchHit, MutableInt> set, Cursor cursor)
            {
              if (set.size() >= limit) {
                return set;
              }

              Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
              for (DimensionSpec dim : dimsToSearch) {
                dimSelectors.put(
                    dim.getOutputName(),
                    cursor.makeDimensionSelector(dim)
                );
              }

              while (!cursor.isDone()) {
                for (Map.Entry<String, DimensionSelector> entry : dimSelectors.entrySet()) {
                  final DimensionSelector selector = entry.getValue();

                  if (selector != null) {
                    final IndexedInts vals = selector.getRow();
                    for (int i = 0; i < vals.size(); ++i) {
                      final String dimVal = selector.lookupName(vals.get(i));
                      if (searchQuerySpec.accept(dimVal)) {
                        MutableInt counter = new MutableInt(1);
                        MutableInt prev = set.put(new SearchHit(entry.getKey(), dimVal), counter);
                        if (prev != null) {
                          counter.add(prev.intValue());
                        }
                        if (set.size() >= limit) {
                          return set;
                        }
                      }
                    }
                  }
                }

                cursor.advance();
              }

              return set;
            }
          }
      );

      return makeReturnResult(segment, limit, retVal);
    }
  }
}
