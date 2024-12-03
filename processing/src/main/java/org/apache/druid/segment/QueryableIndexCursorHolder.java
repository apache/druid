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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterBundle;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.historical.HistoricalCursor;
import org.apache.druid.segment.vector.BitmapVectorOffset;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.utils.CloseableUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryableIndexCursorHolder implements CursorHolder
{
  private static final Logger log = new Logger(QueryableIndexCursorHolder.class);
  private final QueryableIndex index;
  private final Interval interval;
  private final VirtualColumns virtualColumns;

  @Nullable
  private final List<AggregatorFactory> aggregatorFactories;
  @Nullable
  private final Filter filter;
  @Nullable
  private final QueryMetrics<? extends Query<?>> metrics;
  private final List<OrderBy> ordering;
  private final QueryContext queryContext;
  private final int vectorSize;
  private final Supplier<CursorResources> resourcesSupplier;

  public QueryableIndexCursorHolder(
      QueryableIndex index,
      CursorBuildSpec cursorBuildSpec,
      TimeBoundaryInspector timeBoundaryInspector
  )
  {
    this.index = index;
    this.interval = cursorBuildSpec.getInterval();
    this.virtualColumns = cursorBuildSpec.getVirtualColumns();
    this.aggregatorFactories = cursorBuildSpec.getAggregators();
    this.filter = cursorBuildSpec.getFilter();

    final List<OrderBy> indexOrdering = index.getOrdering();
    if (Cursors.preferDescendingTimeOrdering(cursorBuildSpec)
        && Cursors.getTimeOrdering(indexOrdering) == Order.ASCENDING) {
      this.ordering = Cursors.descendingTimeOrder();
    } else {
      this.ordering = indexOrdering;
    }

    this.queryContext = cursorBuildSpec.getQueryContext();
    this.vectorSize = cursorBuildSpec.getQueryContext().getVectorSize();
    this.metrics = cursorBuildSpec.getQueryMetrics();
    this.resourcesSupplier = Suppliers.memoize(
        () -> new CursorResources(
            index,
            timeBoundaryInspector,
            virtualColumns,
            Cursors.getTimeOrdering(ordering),
            interval,
            filter,
            cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.CURSOR_AUTO_ARRANGE_FILTERS, true),
            metrics
        )
    );
  }

  @Override
  public boolean canVectorize()
  {
    final ColumnInspector inspector = virtualColumns.wrapInspector(index);
    if (!virtualColumns.isEmpty()) {
      if (!queryContext.getVectorizeVirtualColumns().shouldVectorize(virtualColumns.canVectorize(inspector))) {
        return false;
      }
    }
    if (aggregatorFactories != null) {
      for (AggregatorFactory factory : aggregatorFactories) {
        if (!factory.canVectorize(inspector)) {
          return false;
        }
      }
    }

    final CursorResources resources = resourcesSupplier.get();
    final FilterBundle filterBundle = resources.filterBundle;
    if (filterBundle != null) {
      if (!filterBundle.canVectorizeMatcher()) {
        return false;
      }
    }

    // vector cursors can't iterate backwards yet
    return Cursors.getTimeOrdering(ordering) != Order.DESCENDING;
  }

  @Override
  public Cursor asCursor()
  {
    if (metrics != null) {
      metrics.vectorized(false);
    }
    final Offset baseOffset;

    final CursorResources resources = resourcesSupplier.get();
    final FilterBundle filterBundle = resources.filterBundle;
    final int numRows = resources.numRows;
    final long minDataTimestamp = resources.timeBoundaryInspector.getMinTime().getMillis();
    final long maxDataTimestamp = resources.timeBoundaryInspector.getMaxTime().getMillis();
    final ColumnCache columnCache = resources.columnCache;
    final Order timeOrder = resources.timeOrder;

    // if filterBundle is null, the filter itself is also null. otherwise check to see if the filter
    // can use an index
    if (filterBundle == null || filterBundle.getIndex() == null) {
      baseOffset =
          timeOrder == Order.DESCENDING ? new SimpleDescendingOffset(numRows) : new SimpleAscendingOffset(numRows);
    } else {
      baseOffset =
          BitmapOffset.of(filterBundle.getIndex().getBitmap(), timeOrder == Order.DESCENDING, index.getNumRows());
    }

    final long timeStart = Math.max(interval.getStartMillis(), minDataTimestamp);
    final long timeEnd = interval.getEndMillis();

    if (timeOrder == Order.ASCENDING) {
      for (; baseOffset.withinBounds(); baseOffset.increment()) {
        if (resources.getTimestampsColumn().getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
          break;
        }
      }
    } else if (timeOrder == Order.DESCENDING) {
      for (; baseOffset.withinBounds(); baseOffset.increment()) {
        if (resources.getTimestampsColumn().getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
          break;
        }
      }
    }

    final Offset offset;

    if (timeOrder == Order.ASCENDING) {
      offset = new AscendingTimestampCheckingOffset(
          baseOffset,
          resources.getTimestampsColumn(),
          timeEnd,
          maxDataTimestamp < timeEnd
      );
    } else if (timeOrder == Order.DESCENDING) {
      offset = new DescendingTimestampCheckingOffset(
          baseOffset,
          resources.getTimestampsColumn(),
          timeStart,
          minDataTimestamp >= timeStart
      );
    } else {
      // Time filter is moved into filterBundle in the non-time-ordered case.
      offset = baseOffset;
    }

    final Offset baseCursorOffset = offset.clone();
    final ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactoryForOffset(
        columnCache,
        baseCursorOffset
    );
    // filterBundle will only be null if the filter itself is null, otherwise check to see if the filter
    // needs to use a value matcher
    if (filterBundle != null && filterBundle.getMatcherBundle() != null) {
      final ValueMatcher matcher = filterBundle.getMatcherBundle()
                                               .valueMatcher(
                                                   columnSelectorFactory,
                                                   baseCursorOffset,
                                                   timeOrder == Order.DESCENDING
                                               );
      final FilteredOffset filteredOffset = new FilteredOffset(baseCursorOffset, matcher);
      return new QueryableIndexCursor(filteredOffset, columnSelectorFactory);
    } else {
      return new QueryableIndexCursor(baseCursorOffset, columnSelectorFactory);
    }
  }

  /**
   * Compute filter to use for cursor creation. For non-time-ordered segments, this includes the query interval
   * as a filter.
   */
  @Nullable
  @Override
  public VectorCursor asVectorCursor()
  {
    final CursorResources resources = resourcesSupplier.get();
    final FilterBundle filterBundle = resources.filterBundle;
    final long minDataTimestamp = resources.timeBoundaryInspector.getMinTime().getMillis();
    final long maxDataTimestamp = resources.timeBoundaryInspector.getMaxTime().getMillis();
    final ColumnCache columnCache = resources.columnCache;
    final Order timeOrder = resources.timeOrder;

    // sanity check
    if (!canVectorize()) {
      close();
      throw new IllegalStateException("canVectorize()");
    }
    if (metrics != null) {
      metrics.vectorized(true);
    }

    // startOffset, endOffset must match the "interval" if timeOrdered. Otherwise, the "interval" filtering is embedded
    // within the filterBundle.
    final int startOffset;
    final int endOffset;

    if (timeOrder != Order.NONE && interval.getStartMillis() > minDataTimestamp) {
      startOffset = timeSearch(resources.getTimestampsColumn(), interval.getStartMillis(), 0, index.getNumRows());
    } else {
      startOffset = 0;
    }

    if (timeOrder != Order.NONE && interval.getEndMillis() <= maxDataTimestamp) {
      endOffset = timeSearch(resources.getTimestampsColumn(), interval.getEndMillis(), startOffset, index.getNumRows());
    } else {
      endOffset = index.getNumRows();
    }

    // filterBundle will only be null if the filter itself is null, otherwise check to see if the filter can use
    // an index
    final VectorOffset baseOffset =
        filterBundle == null || filterBundle.getIndex() == null
        ? new NoFilterVectorOffset(vectorSize, startOffset, endOffset)
        : new BitmapVectorOffset(vectorSize, filterBundle.getIndex().getBitmap(), startOffset, endOffset);

    // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
    final VectorColumnSelectorFactory baseColumnSelectorFactory = makeVectorColumnSelectorFactoryForOffset(
        columnCache,
        baseOffset
    );

    // filterBundle will only be null if the filter itself is null, otherwise check to see if the filter needs to use
    // a value matcher
    if (filterBundle != null && filterBundle.getMatcherBundle() != null) {
      final VectorValueMatcher vectorValueMatcher = filterBundle.getMatcherBundle()
                                                                .vectorMatcher(baseColumnSelectorFactory, baseOffset);
      final VectorOffset filteredOffset = FilteredVectorOffset.create(
          baseOffset,
          vectorValueMatcher
      );

      // Now create the cursor and column selector that will be returned to the caller.
      final VectorColumnSelectorFactory filteredColumnSelectorFactory = makeVectorColumnSelectorFactoryForOffset(
          columnCache,
          filteredOffset
      );
      return new QueryableIndexVectorCursor(filteredColumnSelectorFactory, filteredOffset, vectorSize);
    } else {
      return new QueryableIndexVectorCursor(baseColumnSelectorFactory, baseOffset, vectorSize);
    }
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(resourcesSupplier.get());
  }


  protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
      ColumnCache columnCache,
      Offset baseOffset
  )
  {
    return new QueryableIndexColumnSelectorFactory(
        virtualColumns,
        Cursors.getTimeOrdering(ordering),
        baseOffset.getBaseReadableOffset(),
        columnCache
    );
  }

  protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
      ColumnCache columnCache,
      VectorOffset baseOffset
  )
  {
    return new QueryableIndexVectorColumnSelectorFactory(
        index,
        baseOffset,
        columnCache,
        virtualColumns
    );
  }

  /**
   * Search the time column using binary search. Benchmarks on various other approaches (linear search, binary
   * search that switches to linear at various closeness thresholds) indicated that a pure binary search worked best.
   *
   * @param timeColumn the column
   * @param timestamp  the timestamp to search for
   * @param startIndex first index to search, inclusive
   * @param endIndex   last index to search, exclusive
   * @return first index that has a timestamp equal to, or greater, than "timestamp"
   */
  @VisibleForTesting
  static int timeSearch(
      final NumericColumn timeColumn,
      final long timestamp,
      final int startIndex,
      final int endIndex
  )
  {
    final long prevTimestamp = timestamp - 1;

    // Binary search for prevTimestamp.
    int minIndex = startIndex;
    int maxIndex = endIndex - 1;

    while (minIndex <= maxIndex) {
      final int currIndex = (minIndex + maxIndex) >>> 1;
      final long currValue = timeColumn.getLongSingleValueRow(currIndex);

      if (currValue < prevTimestamp) {
        minIndex = currIndex + 1;
      } else if (currValue > prevTimestamp) {
        maxIndex = currIndex - 1;
      } else {
        // The value at currIndex is prevTimestamp.
        minIndex = currIndex;
        break;
      }
    }

    // Do linear search for the actual timestamp, then return.
    for (; minIndex < endIndex; minIndex++) {
      final long currValue = timeColumn.getLongSingleValueRow(minIndex);
      if (currValue >= timestamp) {
        return minIndex;
      }
    }

    // Not found.
    return endIndex;
  }

  private static class QueryableIndexVectorCursor implements VectorCursor
  {
    private final int vectorSize;
    private final VectorOffset offset;
    private final VectorColumnSelectorFactory columnSelectorFactory;

    public QueryableIndexVectorCursor(
        final VectorColumnSelectorFactory vectorColumnSelectorFactory,
        final VectorOffset offset,
        final int vectorSize
    )
    {
      this.columnSelectorFactory = vectorColumnSelectorFactory;
      this.vectorSize = vectorSize;
      this.offset = offset;
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }

    @Override
    public VectorColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public void advance()
    {
      offset.advance();
      BaseQuery.checkInterrupted();
    }

    @Override
    public boolean isDone()
    {
      return offset.isDone();
    }

    @Override
    public void reset()
    {
      offset.reset();
    }
  }

  private static class QueryableIndexCursor implements HistoricalCursor
  {
    private final Offset cursorOffset;
    private final ColumnSelectorFactory columnSelectorFactory;

    QueryableIndexCursor(Offset cursorOffset, ColumnSelectorFactory columnSelectorFactory)
    {
      this.cursorOffset = cursorOffset;
      this.columnSelectorFactory = columnSelectorFactory;
    }

    @Override
    public Offset getOffset()
    {
      return cursorOffset;
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return columnSelectorFactory;
    }

    @Override
    public void advance()
    {
      cursorOffset.increment();
      // Must call BaseQuery.checkInterrupted() after cursorOffset.increment(), not before, because
      // FilteredOffset.increment() is a potentially long, not an "instant" operation (unlike to all other subclasses
      // of Offset) and it returns early on interruption, leaving itself in an illegal state.  We should not let
      // aggregators, etc. access this illegal state and throw a QueryInterruptedException by calling
      // BaseQuery.checkInterrupted().
      BaseQuery.checkInterrupted();
    }

    @Override
    public void advanceUninterruptibly()
    {
      cursorOffset.increment();
    }

    @Override
    public boolean isDone()
    {
      return !cursorOffset.withinBounds();
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void reset()
    {
      cursorOffset.reset();
    }
  }

  public abstract static class TimestampCheckingOffset extends Offset
  {
    final Offset baseOffset;
    final NumericColumn timestamps;
    final long timeLimit;
    final boolean allWithinThreshold;

    TimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
      // checks if all the values are within the Threshold specified, skips timestamp lookups and checks if all values are within threshold.
      this.allWithinThreshold = allWithinThreshold;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public boolean withinBounds()
    {
      if (!baseOffset.withinBounds()) {
        return false;
      }
      if (allWithinThreshold) {
        return true;
      }
      return timeInRange(timestamps.getLongSingleValueRow(baseOffset.getOffset()));
    }

    @Override
    public void reset()
    {
      baseOffset.reset();
    }

    @Override
    public ReadableOffset getBaseReadableOffset()
    {
      return baseOffset.getBaseReadableOffset();
    }

    protected abstract boolean timeInRange(long current);

    @Override
    public void increment()
    {
      baseOffset.increment();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      throw new IllegalStateException("clone");
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("baseOffset", baseOffset);
      inspector.visit("timestamps", timestamps);
      inspector.visit("allWithinThreshold", allWithinThreshold);
    }
  }

  public static class AscendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    AscendingTimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current < timeLimit;
    }

    @Override
    public String toString()
    {
      return (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "<" + timeLimit + "::" + baseOffset;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      return new AscendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  public static class DescendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    DescendingTimestampCheckingOffset(
        Offset baseOffset,
        NumericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      super(baseOffset, timestamps, timeLimit, allWithinThreshold);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current >= timeLimit;
    }

    @Override
    public String toString()
    {
      return timeLimit + ">=" +
             (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "::" + baseOffset;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public Offset clone()
    {
      return new DescendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  private static final class CursorResources implements Closeable
  {
    private final Closer closer;
    private final TimeBoundaryInspector timeBoundaryInspector;
    private final int numRows;
    @Nullable
    private final FilterBundle filterBundle;
    private final Order timeOrder;
    private final ColumnCache columnCache;
    @MonotonicNonNull
    private NumericColumn timestamps;

    private CursorResources(
        QueryableIndex index,
        TimeBoundaryInspector timeBoundaryInspector,
        VirtualColumns virtualColumns,
        Order timeOrder,
        Interval interval,
        @Nullable Filter filter,
        boolean cursorAutoArrangeFilters,
        @Nullable QueryMetrics<? extends Query<?>> metrics
    )
    {
      this.closer = Closer.create();
      this.columnCache = new ColumnCache(index, closer);
      this.timeBoundaryInspector = timeBoundaryInspector;
      final ColumnSelectorColumnIndexSelector bitmapIndexSelector = new ColumnSelectorColumnIndexSelector(
          index.getBitmapFactoryForDimensions(),
          virtualColumns,
          columnCache
      );
      try {
        this.numRows = index.getNumRows();
        this.filterBundle = makeFilterBundle(
            computeFilterWithIntervalIfNeeded(
                timeBoundaryInspector,
                timeOrder,
                interval,
                filter
            ),
            cursorAutoArrangeFilters,
            bitmapIndexSelector,
            numRows,
            metrics
        );
        this.timeOrder = timeOrder;
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, closer);
      }
    }

    public NumericColumn getTimestampsColumn()
    {
      if (timestamps == null) {
        timestamps = (NumericColumn) columnCache.getColumn(ColumnHolder.TIME_COLUMN_NAME);
      }
      return timestamps;
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
    }
  }

  /**
   * Create a {@link FilterBundle} for a cursor hold instance.
   * <p>
   * The provided filter must include the query-level interface if needed. To compute this properly, use
   * {@link #computeFilterWithIntervalIfNeeded}.
   */
  @Nullable
  private static FilterBundle makeFilterBundle(
      @Nullable final Filter filter,
      boolean cursorAutoArrangeFilters,
      final ColumnSelectorColumnIndexSelector bitmapIndexSelector,
      final int numRows,
      @Nullable final QueryMetrics<?> metrics
  )
  {
    final BitmapFactory bitmapFactory = bitmapIndexSelector.getBitmapFactory();
    final BitmapResultFactory<?> bitmapResultFactory;
    if (metrics != null) {
      bitmapResultFactory = metrics.makeBitmapResultFactory(bitmapFactory);
      metrics.reportSegmentRows(numRows);
    } else {
      bitmapResultFactory = new DefaultBitmapResultFactory(bitmapFactory);
    }
    if (filter == null) {
      return null;
    }
    final long bitmapConstructionStartNs = System.nanoTime();
    final FilterBundle filterBundle = new FilterBundle.Builder(
        filter,
        bitmapIndexSelector,
        cursorAutoArrangeFilters
    ).build(
        bitmapResultFactory,
        numRows,
        numRows,
        false
    );
    if (metrics != null) {
      final long buildTime = System.nanoTime() - bitmapConstructionStartNs;
      metrics.reportBitmapConstructionTime(buildTime);
      final FilterBundle.BundleInfo info = filterBundle.getInfo();
      metrics.filterBundle(info);
      log.debug("Filter partitioning (%sms):%s", TimeUnit.NANOSECONDS.toMillis(buildTime), info);
      if (filterBundle.getIndex() != null) {
        metrics.reportPreFilteredRows(filterBundle.getIndex().getBitmap().size());
      } else {
        metrics.reportPreFilteredRows(0);
      }
    } else if (log.isDebugEnabled()) {
      final FilterBundle.BundleInfo info = filterBundle.getInfo();
      final long buildTime = System.nanoTime() - bitmapConstructionStartNs;
      log.debug("Filter partitioning (%sms):%s", TimeUnit.NANOSECONDS.toMillis(buildTime), info);
    }
    return filterBundle;
  }

  /**
   * Returns the query-level {@link Filter} plus, if needed, a {@link RangeFilter} for
   * {@link ColumnHolder#TIME_COLUMN_NAME}. The time filter is added if time order is {@link Order#NONE} and
   * the provided {@link Interval} is not contained entirely within [minDataTimestamp, maxDataTimestamp].
   */
  @Nullable
  private static Filter computeFilterWithIntervalIfNeeded(
      final TimeBoundaryInspector timeBoundaryInspector,
      final Order timeOrder,
      final Interval interval,
      @Nullable final Filter filter
  )
  {
    if (timeOrder == Order.NONE
        && (timeBoundaryInspector.getMinTime().getMillis() < interval.getStartMillis()
            || timeBoundaryInspector.getMaxTime().getMillis() >= interval.getEndMillis())) {
      final RangeFilter timeFilter = new RangeFilter(
          ColumnHolder.TIME_COLUMN_NAME,
          ColumnType.LONG,
          timeBoundaryInspector.getMinTime().getMillis() < interval.getStartMillis() ? interval.getStartMillis() : null,
          timeBoundaryInspector.getMaxTime().getMillis() >= interval.getEndMillis() ? interval.getEndMillis() : null,
          false,
          true,
          null
      );

      if (filter == null) {
        return timeFilter;
      } else {
        return new AndFilter(ImmutableList.of(filter, timeFilter));
      }
    } else {
      return filter;
    }
  }
}
