package io.druid.query.search.search;

import com.metamx.emitter.EmittingLogger;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.IAE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.CursorBasedStrategy.CursorBasedExecutor;
import io.druid.query.search.search.IndexOnlyStrategy.IndexOnlyExecutor;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.List;

public class AutoStrategy extends SearchStrategy
{
  private static final EmittingLogger log = new EmittingLogger(AutoStrategy.class);

  @Override
  public SearchQueryExecutor getExecutionPlan(SearchQuery query, Segment segment)
  {
    final QueryableIndex index = segment.asQueryableIndex();

    if (index != null) {
      final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));
      final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
      if (intervals.size() != 1) {
        throw new IAE("Should only have one interval, got[%s]", intervals);
      }
      final Interval interval = intervals.get(0);
      final ImmutableBitmap timeFilteredBitmap = IndexOnlyExecutor.makeTimeFilteredBitmap(index,
                                                                                          segment,
                                                                                          filter,
                                                                                          interval);
      final Iterable<DimensionSpec> dimsToSearch = SearchQueryExecutor.getDimsToSearch(index.getAvailableDimensions(),
                                                                                       query.getDimensions());

      // Choose a search query execution strategy depending on the query.
      // Index-only strategy is selected when
      // 1) there is no filter,
      // 2) the total cardinality is very low, or
      // 3) the filter has a very high selectivity.
      if (filter == null ||
          index.getDecisionHelper().hasLowCardinality(index, dimsToSearch) ||
          index.getDecisionHelper().hasHighSelectivity(index, timeFilteredBitmap)) {
        log.info("Index-only execution strategy is selected");
        return new IndexOnlyExecutor(query, segment);
      } else {
        log.info("Cursor-based execution strategy is selected");
        return new CursorBasedExecutor(query, segment);
      }

    } else {
      log.info("Index doesn't exist. Fall back to cursor-based execution strategy");
      return new CursorBasedExecutor(query, segment);
    }
  }
}
