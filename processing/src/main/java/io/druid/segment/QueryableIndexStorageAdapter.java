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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.io.Closer;
import io.druid.query.BaseQuery;
import io.druid.query.BitmapResultFactory;
import io.druid.query.DefaultBitmapResultFactory;
import io.druid.query.QueryMetrics;
import io.druid.query.filter.Filter;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.column.BaseColumn;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.Offset;
import io.druid.segment.data.ReadableOffset;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.historical.HistoricalCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private final QueryableIndex index;

  public QueryableIndexStorageAdapter(QueryableIndex index)
  {
    this.index = index;
  }

  @Override
  public String getSegmentIdentifier()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Interval getInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    HashSet<String> columnNames = Sets.newHashSet(index.getColumnNames());
    return Sets.difference(columnNames, Sets.newHashSet(index.getAvailableDimensions()));
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension == null) {
      return 0;
    }

    Column column = index.getColumn(dimension);
    if (column == null) {
      return 0;
    }
    if (!column.getCapabilities().isDictionaryEncoded()) {
      return Integer.MAX_VALUE;
    }
    return column.getDictionaryEncoding().getCardinality();
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public DateTime getMinTime()
  {
    try (final GenericColumn column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn()) {
      return DateTimes.utc(column.getLongSingleValueRow(0));
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    try (final GenericColumn column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn()) {
      return DateTimes.utc(column.getLongSingleValueRow(column.length() - 1));
    }
  }

  @Override
  @Nullable
  public Comparable getMinValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(0) : null;
    }
    return null;
  }

  @Override
  @Nullable
  public Comparable getMaxValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(bitmap.getCardinality() - 1) : null;
    }
    return null;
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getColumnCapabilites(index, column);
  }

  @Override
  public String getColumnTypeName(String columnName)
  {
    final Column column = index.getColumn(columnName);
    try (final ComplexColumn complexColumn = column.getComplexColumn()) {
      return complexColumn != null ? complexColumn.getTypeName() : column.getCapabilities().getType().toString();
    }
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    // For immutable indexes, maxIngestedEventTime is maxTime.
    return getMaxTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {

    DateTime minTime = getMinTime();
    long minDataTimestamp = minTime.getMillis();
    DateTime maxTime = getMaxTime();
    long maxDataTimestamp = maxTime.getMillis();
    final Interval dataInterval = new Interval(minTime, gran.bucketEnd(maxTime));

    if (!interval.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    final Interval actualInterval = interval.overlap(dataInterval);

    final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
        index.getBitmapFactoryForDimensions(),
        virtualColumns,
        index
    );

    final int totalRows = index.getNumRows();

    /*
     * Filters can be applied in two stages:
     * pre-filtering: Use bitmap indexes to prune the set of rows to be scanned.
     * post-filtering: Iterate through rows and apply the filter to the row values
     *
     * The pre-filter and post-filter step have an implicit AND relationship. (i.e., final rows are those that
     * were not pruned AND those that matched the filter during row scanning)
     *
     * An AND filter can have its subfilters partitioned across the two steps. The subfilters that can be
     * processed entirely with bitmap indexes (subfilter returns true for supportsBitmapIndex())
     * will be moved to the pre-filtering stage.
     *
     * Any subfilters that cannot be processed entirely with bitmap indexes will be moved to the post-filtering stage.
     */
    final Offset offset;
    final List<Filter> preFilters;
    final List<Filter> postFilters = new ArrayList<>();
    int preFilteredRows = totalRows;
    if (filter == null) {
      preFilters = Collections.emptyList();
      offset = descending ? new SimpleDescendingOffset(totalRows) : new SimpleAscendingOffset(totalRows);
    } else {
      preFilters = new ArrayList<>();

      if (filter instanceof AndFilter) {
        // If we get an AndFilter, we can split the subfilters across both filtering stages
        for (Filter subfilter : ((AndFilter) filter).getFilters()) {
          if (subfilter.supportsBitmapIndex(selector)) {
            preFilters.add(subfilter);
          } else {
            postFilters.add(subfilter);
          }
        }
      } else {
        // If we get an OrFilter or a single filter, handle the filter in one stage
        if (filter.supportsBitmapIndex(selector)) {
          preFilters.add(filter);
        } else {
          postFilters.add(filter);
        }
      }

      if (preFilters.size() == 0) {
        offset = descending ? new SimpleDescendingOffset(totalRows) : new SimpleAscendingOffset(totalRows);
      } else {
        if (queryMetrics != null) {
          BitmapResultFactory<?> bitmapResultFactory =
              queryMetrics.makeBitmapResultFactory(selector.getBitmapFactory());
          long bitmapConstructionStartNs = System.nanoTime();
          // Use AndFilter.getBitmapResult to intersect the preFilters to get its short-circuiting behavior.
          ImmutableBitmap bitmapIndex = AndFilter.getBitmapIndex(selector, bitmapResultFactory, preFilters);
          preFilteredRows = bitmapIndex.size();
          offset = BitmapOffset.of(bitmapIndex, descending, totalRows);
          queryMetrics.reportBitmapConstructionTime(System.nanoTime() - bitmapConstructionStartNs);
        } else {
          BitmapResultFactory<?> bitmapResultFactory = new DefaultBitmapResultFactory(selector.getBitmapFactory());
          offset = BitmapOffset.of(
              AndFilter.getBitmapIndex(selector, bitmapResultFactory, preFilters),
              descending,
              totalRows
          );
        }
      }
    }

    final Filter postFilter;
    if (postFilters.size() == 0) {
      postFilter = null;
    } else if (postFilters.size() == 1) {
      postFilter = postFilters.get(0);
    } else {
      postFilter = new AndFilter(postFilters);
    }

    if (queryMetrics != null) {
      queryMetrics.preFilters(preFilters);
      queryMetrics.postFilters(postFilters);
      queryMetrics.reportSegmentRows(totalRows);
      queryMetrics.reportPreFilteredRows(preFilteredRows);
    }

    return Sequences.filter(
        new CursorSequenceBuilder(
            this,
            actualInterval,
            virtualColumns,
            gran,
            offset,
            minDataTimestamp,
            maxDataTimestamp,
            descending,
            postFilter,
            selector
        ).build(),
        Objects::nonNull
    );
  }

  @Nullable
  static ColumnCapabilities getColumnCapabilites(ColumnSelector index, String columnName)
  {
    Column columnObj = index.getColumn(columnName);
    if (columnObj == null) {
      return null;
    }
    return columnObj.getCapabilities();
  }

  private static class CursorSequenceBuilder
  {
    private final QueryableIndex index;
    private final Interval interval;
    private final VirtualColumns virtualColumns;
    private final Granularity gran;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;
    @Nullable
    private final Filter postFilter;
    private final ColumnSelectorBitmapIndexSelector bitmapIndexSelector;

    public CursorSequenceBuilder(
        QueryableIndexStorageAdapter storageAdapter,
        Interval interval,
        VirtualColumns virtualColumns,
        Granularity gran,
        Offset offset,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        @Nullable Filter postFilter,
        ColumnSelectorBitmapIndexSelector bitmapIndexSelector
    )
    {
      this.index = storageAdapter.index;
      this.interval = interval;
      this.virtualColumns = virtualColumns;
      this.gran = gran;
      this.offset = offset;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.postFilter = postFilter;
      this.bitmapIndexSelector = bitmapIndexSelector;
    }

    public Sequence<Cursor> build()
    {
      final Offset baseOffset = offset.clone();

      // Column caches shared amongst all cursors in this sequence.
      final Map<String, BaseColumn> columnCache = new HashMap<>();

      final GenericColumn timestamps = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();

      final Closer closer = Closer.create();
      closer.register(timestamps);

      Iterable<Interval> iterable = gran.getIterable(interval);
      if (descending) {
        iterable = Lists.reverse(ImmutableList.copyOf(iterable));
      }

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Interval, Cursor>()
              {
                @Override
                public Cursor apply(final Interval inputInterval)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), inputInterval.getStartMillis());
                  final long timeEnd = Math.min(
                      interval.getEndMillis(),
                      gran.increment(inputInterval.getStart()).getMillis()
                  );

                  if (descending) {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
                        break;
                      }
                    }
                  }

                  final Offset offset = descending ?
                                        new DescendingTimestampCheckingOffset(
                                            baseOffset,
                                            timestamps,
                                            timeStart,
                                            minDataTimestamp >= timeStart
                                        ) :
                                        new AscendingTimestampCheckingOffset(
                                            baseOffset,
                                            timestamps,
                                            timeEnd,
                                            maxDataTimestamp < timeEnd
                                        );


                  final Offset baseCursorOffset = offset.clone();
                  final ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
                      index,
                      virtualColumns,
                      descending,
                      closer,
                      baseCursorOffset.getBaseReadableOffset(),
                      columnCache
                  );
                  final DateTime myBucket = gran.toDateTime(inputInterval.getStartMillis());

                  if (postFilter == null) {
                    return new QueryableIndexCursor(baseCursorOffset, columnSelectorFactory, myBucket);
                  } else {
                    FilteredOffset filteredOffset = new FilteredOffset(
                        baseCursorOffset,
                        columnSelectorFactory,
                        descending,
                        postFilter,
                        bitmapIndexSelector
                    );
                    return new QueryableIndexCursor(filteredOffset, columnSelectorFactory, myBucket);
                  }

                }
              }
          ),
          closer
      );
    }
  }

  private static class QueryableIndexCursor implements HistoricalCursor
  {
    private final Offset cursorOffset;
    private final ColumnSelectorFactory columnSelectorFactory;
    private final DateTime bucketStart;

    QueryableIndexCursor(Offset cursorOffset, ColumnSelectorFactory columnSelectorFactory, DateTime bucketStart)
    {
      this.cursorOffset = cursorOffset;
      this.columnSelectorFactory = columnSelectorFactory;
      this.bucketStart = bucketStart;
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
    public DateTime getTime()
    {
      return bucketStart;
    }

    @Override
    public void advance()
    {
      cursorOffset.increment();
      // Must call BaseQuery.checkInterrupted() after cursorOffset.increment(), not before, because
      // FilteredOffset.increment() is a potentially long, not an "instant" operation (unlike to all other subclasses
      // of Offset) and it returns early on interruption, leaving itself in an illegal state. We should not let
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
    public void advanceTo(int offset)
    {
      int count = 0;
      while (count < offset && !isDone()) {
        advance();
        count++;
      }
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
    final GenericColumn timestamps;
    final long timeLimit;
    final boolean allWithinThreshold;

    TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit,
        boolean allWithinThreshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
      // checks if all the values are within the Threshold specified, skips timestamp lookups and checks if all values
      // are within threshold.
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
        GenericColumn timestamps,
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
        GenericColumn timestamps,
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

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
