package io.druid.query.search.search;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.CursorBasedStrategy.CursorBasedExecutor;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.TreeMap;

public class IndexOnlyStrategy extends SearchStrategy
{
  public static final String NAME = "indexOnly";

  private static final EmittingLogger log = new EmittingLogger(IndexOnlyStrategy.class);

  public IndexOnlyStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public SearchQueryExecutor getExecutionPlan(SearchQuery query, Segment segment)
  {
    final QueryableIndex index = segment.asQueryableIndex();

    if (index == null) {
      log.debug("Index doesn't exist. Fall back to cursor-based execution strategy");
      return new CursorBasedExecutor(query, segment, filter, interval);
    } else {
      log.debug("Index-only execution strategy is selected");
      final ImmutableBitmap timeFilteredBitmap = makeTimeFilteredBitmap(index, segment, filter, interval);
      return new IndexOnlyExecutor(query, segment, timeFilteredBitmap);
    }
  }

  static ImmutableBitmap makeTimeFilteredBitmap(final QueryableIndex index,
                                                final Segment segment,
                                                final Filter filter,
                                                final Interval interval)
  {
    final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();
    final ImmutableBitmap baseFilter =
        filter == null ? null : filter.getBitmapIndex(new ColumnSelectorBitmapIndexSelector(bitmapFactory, index));

    final ImmutableBitmap timeFilteredBitmap;
    if (!interval.contains(segment.getDataInterval())) {
      MutableBitmap timeBitmap = bitmapFactory.makeEmptyMutableBitmap();
      final Column timeColumn = index.getColumn(Column.TIME_COLUMN_NAME);
      try (final GenericColumn timeValues = timeColumn.getGenericColumn()) {

        int startIndex = Math.max(0, getStartIndexOfTime(timeValues, interval.getStartMillis(), true));
        int endIndex = Math.min(
            timeValues.length() - 1,
            getStartIndexOfTime(timeValues, interval.getEndMillis(), false)
        );

        for (int i = startIndex; i <= endIndex; i++) {
          timeBitmap.add(i);
        }

        final ImmutableBitmap finalTimeBitmap = bitmapFactory.makeImmutableBitmap(timeBitmap);
        timeFilteredBitmap =
            (baseFilter == null) ? finalTimeBitmap : finalTimeBitmap.intersection(baseFilter);
      }
    } else {
      timeFilteredBitmap = baseFilter;
    }

    return timeFilteredBitmap;
  }

  private static int getStartIndexOfTime(GenericColumn timeValues, long time, boolean inclusive)
  {
    int low = 0;
    int high = timeValues.length() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = timeValues.getLongSingleValueRow(mid);

      if (midVal < time) {
        low = mid + 1;
      } else if (midVal > time) {
        high = mid - 1;
      } else { // key found
        int i;
        // rewind the index of the same time values
        for (i = mid - 1; i >= 0; i--) {
          long prev = timeValues.getLongSingleValueRow(i);
          if (time != prev) {
            break;
          }
        }
        return inclusive ? i + 1 : i;
      }
    }
    // key not found.
    // return insert index
    return inclusive ? low : low - 1;
  }

  public static class IndexOnlyExecutor extends SearchQueryExecutor {

    private final ImmutableBitmap timeFilteredBitmap;

    public IndexOnlyExecutor(SearchQuery query, Segment segment, ImmutableBitmap timeFilteredBitmap)
    {
      super(query, segment);
      this.timeFilteredBitmap = timeFilteredBitmap;
    }

    @Override
    public Sequence<Result<SearchResultValue>> execute()
    {
      final QueryableIndex index = segment.asQueryableIndex();
      Preconditions.checkArgument(index != null, "Index should not be null");

      final BitmapFactory bitmapFactory = index.getBitmapFactoryForDimensions();

      final TreeMap<SearchHit, MutableInt> retVal = Maps.newTreeMap(query.getSort().getComparator());
      for (DimensionSpec dimension : dimsToSearch) {
        final Column column = index.getColumn(dimension.getDimension());
        if (column == null) {
          continue;
        }

        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        ExtractionFn extractionFn = dimension.getExtractionFn();
        if (extractionFn == null) {
          extractionFn = IdentityExtractionFn.getInstance();
        }
        if (bitmapIndex != null) {
          for (int i = 0; i < bitmapIndex.getCardinality(); ++i) {
            String dimVal = Strings.nullToEmpty(extractionFn.apply(bitmapIndex.getValue(i)));
            if (!searchQuerySpec.accept(dimVal)) {
              continue;
            }
            ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
            if (timeFilteredBitmap != null) {
              bitmap = bitmapFactory.intersection(Arrays.asList(timeFilteredBitmap, bitmap));
            }
            if (bitmap.size() > 0) {
              MutableInt counter = new MutableInt(bitmap.size());
              MutableInt prev = retVal.put(new SearchHit(dimension.getOutputName(), dimVal), counter);
              if (prev != null) {
                counter.add(prev.intValue());
              }
              if (retVal.size() >= limit) {
                return makeReturnResult(segment, limit, retVal);
              }
            }
          }
        }
      }

      return makeReturnResult(segment, limit, retVal);
    }
  }
}
