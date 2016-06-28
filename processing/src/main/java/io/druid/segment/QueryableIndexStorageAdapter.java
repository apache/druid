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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.RowOffsetMatcherFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.BooleanValueMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

  private final QueryableIndex index;

  public QueryableIndexStorageAdapter(
      QueryableIndex index
  )
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
    return Sets.difference(Sets.newHashSet(index.getColumnNames()), Sets.newHashSet(index.getAvailableDimensions()));
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
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(0));
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(column.length() - 1));
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public Comparable getMinValue(String dimension)
  {
    DimensionColumnReader reader = index.getDimensionReaders().get(dimension);
    if (reader == null) {
      return null;
    }

    return reader.getMinValue();
  }

  @Override
  public Comparable getMaxValue(String dimension)
  {
    DimensionColumnReader reader = index.getDimensionReaders().get(dimension);
    if (reader == null) {
      return null;
    }

    return reader.getMaxValue();
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    Column columnObj = index.getColumn(column);
    if (columnObj == null) {
      return null;
    }
    return columnObj.getCapabilities();
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return index.getDimensionHandlers();
  }

  @Override
  public String getColumnTypeName(String columnName)
  {
    final Column column = index.getColumn(columnName);
    final ComplexColumn complexColumn = column.getComplexColumn();
    return complexColumn != null ? complexColumn.getTypeName() : column.getCapabilities().getType().toString();
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    // For immutable indexes, maxIngestedEventTime is maxTime.
    return getMaxTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran, boolean descending)
  {
    Interval actualInterval = interval;

    long minDataTimestamp = getMinTime().getMillis();
    long maxDataTimestamp = getMaxTime().getMillis();
    final Interval dataInterval = new Interval(
        minDataTimestamp,
        gran.next(gran.truncate(maxDataTimestamp))
    );

    if (!actualInterval.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    if (actualInterval.getStart().isBefore(dataInterval.getStart())) {
      actualInterval = actualInterval.withStart(dataInterval.getStart());
    }
    if (actualInterval.getEnd().isAfter(dataInterval.getEnd())) {
      actualInterval = actualInterval.withEnd(dataInterval.getEnd());
    }

    final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
        index.getBitmapFactoryForDimensions(),
        index,
        index.getDimensionReaders()
    );


    /**
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
    final List<Filter> postFilters = new ArrayList<>();
    if (filter == null) {
      offset = new NoFilterOffset(0, index.getNumRows(), descending);
    } else {
      final List<Filter> preFilters = new ArrayList<>();

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
        offset = new NoFilterOffset(0, index.getNumRows(), descending);
      } else {
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (Filter prefilter : preFilters) {
          bitmaps.add(prefilter.getBitmapIndex(selector));
        }
        offset = new BitmapOffset(
            selector.getBitmapFactory(),
            selector.getBitmapFactory().intersection(bitmaps),
            descending
        );
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

    return Sequences.filter(
        new CursorSequenceBuilder(
            index,
            actualInterval,
            gran,
            offset,
            minDataTimestamp,
            maxDataTimestamp,
            descending,
            postFilter,
            selector
        ).build(),
        Predicates.<Cursor>notNull()
    );
  }

  private interface CursorAdvancer
  {
    public void advance();

    public void advanceTo(int offset);

    public boolean isDone();

    public void reset();
  }

  private static class CursorSequenceBuilder
  {
    private final QueryableIndex index;
    private final Interval interval;
    private final QueryGranularity gran;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;
    private final Filter postFilter;
    private final ColumnSelectorBitmapIndexSelector bitmapIndexSelector;

    public CursorSequenceBuilder(
        QueryableIndex index,
        Interval interval,
        QueryGranularity gran,
        Offset offset,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        Filter postFilter,
        ColumnSelectorBitmapIndexSelector bitmapIndexSelector
    )
    {
      this.index = index;
      this.interval = interval;
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

      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, Object> objectColumnCache = Maps.newHashMap();
      final Map<String, Closeable> dimensionColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();

      Iterable<Long> iterable = gran.iterable(interval.getStartMillis(), interval.getEndMillis());
      if (descending) {
        iterable = Lists.reverse(ImmutableList.copyOf(iterable));
      }

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Long, Cursor>()
              {
                @Override
                public Cursor apply(final Long input)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), input);
                  final long timeEnd = Math.min(interval.getEndMillis(), gran.next(input));

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


                  final Offset initOffset = offset.clone();
                  final DateTime myBucket = gran.toDateTime(input);
                  final CursorOffsetHolder cursorOffsetHolder = new CursorOffsetHolder();

                  abstract class QueryableIndexBaseCursor implements Cursor
                  {
                    Offset cursorOffset;

                    @Override
                    public DimensionSelector makeDimensionSelector(
                        DimensionSpec dimensionSpec
                    )
                    {
                      final String dimension = dimensionSpec.getDimension();
                      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

                      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                        return new SingleScanTimeDimSelector(
                            makeLongColumnSelector(dimension),
                            extractionFn,
                            descending
                        );
                      }

                      final Column columnDesc = index.getColumn(dimension);
                      if (columnDesc == null) {
                        return NULL_DIMENSION_SELECTOR;
                      }

                      String dimName = dimensionSpec.getDimension();
                      DimensionHandler handler = index.getDimensionHandlers().get(dimName);
                      if (handler == null) {
                        return NULL_DIMENSION_SELECTOR;
                      }

                      DimensionColumnReader reader = index.getDimensionReaders().get(dimension);
                      DimensionSelector selector = reader.makeDimensionSelector(
                          dimensionSpec,
                          cursorOffsetHolder,
                          dimensionColumnCache
                      );

                      return dimensionSpec.decorate(selector);
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder != null && (holder.getCapabilities().getType() == ValueType.FLOAT
                                               || holder.getCapabilities().getType() == ValueType.LONG)) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new FloatColumnSelector()
                        {
                          @Override
                          public float get()
                          {
                            return 0.0f;
                          }
                        };
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new FloatColumnSelector()
                      {
                        @Override
                        public float get()
                        {
                          return metricVals.getFloatSingleValueRow(cursorOffsetHolder.get().getOffset());
                        }
                      };
                    }

                    @Override
                    public LongColumnSelector makeLongColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder != null && (holder.getCapabilities().getType() == ValueType.LONG
                                               || holder.getCapabilities().getType() == ValueType.FLOAT)) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new LongColumnSelector()
                        {
                          @Override
                          public long get()
                          {
                            return 0L;
                          }
                        };
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new LongColumnSelector()
                      {
                        @Override
                        public long get()
                        {
                          return metricVals.getLongSingleValueRow(cursorOffsetHolder.get().getOffset());
                        }
                      };
                    }


                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String column)
                    {
                      Object cachedColumnVals = objectColumnCache.get(column);

                      if (cachedColumnVals == null) {
                        Column holder = index.getColumn(column);

                        if (holder != null) {
                          final ColumnCapabilities capabilities = holder.getCapabilities();

                          if (capabilities.isDictionaryEncoded()) {
                            cachedColumnVals = holder.getDictionaryEncoding();
                          } else if (capabilities.getType() == ValueType.COMPLEX) {
                            cachedColumnVals = holder.getComplexColumn();
                          } else {
                            cachedColumnVals = holder.getGenericColumn();
                          }
                        }

                        if (cachedColumnVals != null) {
                          objectColumnCache.put(column, cachedColumnVals);
                        }
                      }

                      if (cachedColumnVals == null) {
                        return null;
                      }

                      if (cachedColumnVals instanceof GenericColumn) {
                        final GenericColumn columnVals = (GenericColumn) cachedColumnVals;
                        final ValueType type = columnVals.getType();

                        if (columnVals.hasMultipleValues()) {
                          throw new UnsupportedOperationException(
                              "makeObjectColumnSelector does not support multi-value GenericColumns"
                          );
                        }

                        if (type == ValueType.FLOAT) {
                          return new ObjectColumnSelector<Float>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Float.TYPE;
                            }

                            @Override
                            public Float get()
                            {
                              return columnVals.getFloatSingleValueRow(cursorOffsetHolder.get().getOffset());
                            }
                          };
                        }
                        if (type == ValueType.LONG) {
                          return new ObjectColumnSelector<Long>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Long.TYPE;
                            }

                            @Override
                            public Long get()
                            {
                              return columnVals.getLongSingleValueRow(cursorOffsetHolder.get().getOffset());
                            }
                          };
                        }
                        if (type == ValueType.STRING) {
                          return new ObjectColumnSelector<String>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return String.class;
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.getStringSingleValueRow(cursorOffsetHolder.get().getOffset());
                            }
                          };
                        }
                      }

                      if (cachedColumnVals instanceof DictionaryEncodedColumn) {
                        final DictionaryEncodedColumn columnVals = (DictionaryEncodedColumn) cachedColumnVals;
                        if (columnVals.hasMultipleValues()) {
                          return new ObjectColumnSelector<Object>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Object.class;
                            }

                            @Override
                            public Object get()
                            {
                              final IndexedInts multiValueRow = columnVals.getMultiValueRow(cursorOffsetHolder.get().getOffset());
                              if (multiValueRow.size() == 0) {
                                return null;
                              } else if (multiValueRow.size() == 1) {
                                return columnVals.lookupName(multiValueRow.get(0));
                              } else {
                                final String[] strings = new String[multiValueRow.size()];
                                for (int i = 0; i < multiValueRow.size(); i++) {
                                  strings[i] = columnVals.lookupName(multiValueRow.get(i));
                                }
                                return strings;
                              }
                            }
                          };
                        } else {
                          return new ObjectColumnSelector<String>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return String.class;
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.lookupName(columnVals.getSingleValueRow(cursorOffsetHolder.get().getOffset()));
                            }
                          };
                        }
                      }

                      final ComplexColumn columnVals = (ComplexColumn) cachedColumnVals;
                      return new ObjectColumnSelector()
                      {
                        @Override
                        public Class classOfObject()
                        {
                          return columnVals.getClazz();
                        }

                        @Override
                        public Object get()
                        {
                          return columnVals.getRowValue(cursorOffsetHolder.get().getOffset());
                        }
                      };
                    }
                  }

                  if (postFilter == null) {
                    return new QueryableIndexBaseCursor()
                    {
                      {
                        reset();
                      }

                      @Override
                      public DateTime getTime()
                      {
                        return myBucket;
                      }

                      @Override
                      public void advance()
                      {
                        if (Thread.interrupted()) {
                          throw new QueryInterruptedException(new InterruptedException());
                        }
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
                      public void reset()
                      {
                        cursorOffset = initOffset.clone();
                        cursorOffsetHolder.set(cursorOffset);
                      }
                    };
                  } else {
                    return new QueryableIndexBaseCursor()
                    {
                      CursorOffsetHolderValueMatcherFactory valueMatcherFactory = new CursorOffsetHolderValueMatcherFactory(
                          this
                      );
                      RowOffsetMatcherFactory rowOffsetMatcherFactory = new CursorOffsetHolderRowOffsetMatcherFactory(
                          cursorOffsetHolder,
                          descending
                      );

                      final ValueMatcher filterMatcher;
                      {
                        if (postFilter instanceof BooleanFilter) {
                          filterMatcher = ((BooleanFilter) postFilter).makeMatcher(
                              bitmapIndexSelector,
                              valueMatcherFactory,
                              rowOffsetMatcherFactory
                          );
                        } else {
                          if (postFilter.supportsBitmapIndex(bitmapIndexSelector)) {
                            filterMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(postFilter.getBitmapIndex(
                                bitmapIndexSelector));
                          } else {
                            filterMatcher = postFilter.makeMatcher(valueMatcherFactory);
                          }
                        }
                      }

                      {
                        reset();
                      }

                      @Override
                      public DateTime getTime()
                      {
                        return myBucket;
                      }

                      @Override
                      public void advance()
                      {
                        if (Thread.interrupted()) {
                          throw new QueryInterruptedException(new InterruptedException());
                        }
                        cursorOffset.increment();

                        while (!isDone()) {
                          if (Thread.interrupted()) {
                            throw new QueryInterruptedException(new InterruptedException());
                          }
                          if (filterMatcher.matches()) {
                            return;
                          } else {
                            cursorOffset.increment();
                          }
                        }
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
                      public void reset()
                      {
                        cursorOffset = initOffset.clone();
                        cursorOffsetHolder.set(cursorOffset);
                        if (!isDone()) {
                          if (filterMatcher.matches()) {
                            return;
                          } else {
                            advance();
                          }
                        }
                      }
                    };
                  }
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              CloseQuietly.close(timestamps);
              for (GenericColumn column : genericColumnCache.values()) {
                CloseQuietly.close(column);
              }
              for (ComplexColumn complexColumn : complexColumnCache.values()) {
                CloseQuietly.close(complexColumn);
              }
              for (Object column : objectColumnCache.values()) {
                if (column instanceof Closeable) {
                  CloseQuietly.close((Closeable) column);
                }
              }
              for (Closeable column : dimensionColumnCache.values()) {
                CloseQuietly.close(column);
              }
            }
          }
      );
    }
  }

  public static class CursorOffsetHolder
  {
    Offset currOffset = null;

    public Offset get()
    {
      return currOffset;
    }

    public void set(Offset currOffset)
    {
      this.currOffset = currOffset;
    }
  }

  private static boolean isComparableNullOrEmpty(final Comparable value)
  {
    if (value instanceof String) {
      return Strings.isNullOrEmpty((String) value);
    }
    return value == null;
  }

  private static class CursorOffsetHolderValueMatcherFactory implements ValueMatcherFactory
  {
    private final ColumnSelectorFactory cursor;

    public CursorOffsetHolderValueMatcherFactory(
        ColumnSelectorFactory cursor
    )
    {
      this.cursor = cursor;
    }

    // Currently unused, except by unit tests, since filters always support bitmap indexes currently.
    // This will change when non-String dimensions are added.
    @Override
    public ValueMatcher makeValueMatcher(String dimension, final Comparable value)
    {
      final DimensionSelector selector = cursor.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      // if matching against null, rows with size 0 should also match
      final boolean matchNull = isComparableNullOrEmpty(value);

      final int id = selector.lookupId((String) value);
      if (id < 0) {
        return new BooleanValueMatcher(false);
      } else {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            IndexedInts row = selector.getRow();
            if (row.size() == 0) {
              return matchNull;
            }
            for (int i = 0; i < row.size(); i++) {
              if (row.get(i) == id) {
                return true;
              }
            }
            return false;
          }
        };
      }
    }

    // Currently unused, except by unit tests, since filters always support bitmap indexes currently.
    // This will change when non-String dimensions are added.
    @Override
    public ValueMatcher makeValueMatcher(String dimension, final Predicate predicate)
    {
      final DimensionSelector selector = cursor.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      return new ValueMatcher()
      {
        final boolean matchNull = predicate.apply(null);

        @Override
        public boolean matches()
        {
          IndexedInts row = selector.getRow();
          if (row.size() == 0) {
            return matchNull;
          }
          for (int i = 0; i < row.size(); i++) {
            if (predicate.apply(selector.lookupName(row.get(i)))) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }

  private static class CursorOffsetHolderRowOffsetMatcherFactory implements RowOffsetMatcherFactory
  {
    private final CursorOffsetHolder holder;
    private final boolean descending;

    public CursorOffsetHolderRowOffsetMatcherFactory(CursorOffsetHolder holder, boolean descending)
    {
      this.holder = holder;
      this.descending = descending;
    }

    // Use an iterator-based implementation, ImmutableBitmap.get(index) works differently for Concise and Roaring.
    // ImmutableConciseSet.get(index) is also inefficient, it performs a linear scan on each call
    @Override
    public ValueMatcher makeRowOffsetMatcher(final ImmutableBitmap rowBitmap) {
      final IntIterator iter = descending ?
                               BitmapOffset.getReverseBitmapOffsetIterator(rowBitmap) :
                               rowBitmap.iterator();

      if(!iter.hasNext()) {
        return new BooleanValueMatcher(false);
      }

      if (descending) {
        return new ValueMatcher()
        {
          int iterOffset = Integer.MAX_VALUE;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.get().getOffset();
            while (iterOffset > currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }
        };
      } else {
        return new ValueMatcher()
        {
          int iterOffset = -1;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.get().getOffset();
            while (iterOffset < currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }
        };
      }
    }
  }


  private abstract static class TimestampCheckingOffset implements Offset
  {
    protected final Offset baseOffset;
    protected final GenericColumn timestamps;
    protected final long timeLimit;
    protected final boolean allWithinThreshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
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

    protected abstract boolean timeInRange(long current);

    @Override
    public void increment()
    {
      baseOffset.increment();
    }

    @Override
    public Offset clone()
    {
      throw new IllegalStateException("clone");
    }
  }

  private static class AscendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public AscendingTimestampCheckingOffset(
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

    @Override
    public Offset clone()
    {
      return new AscendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  private static class DescendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public DescendingTimestampCheckingOffset(
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

    @Override
    public Offset clone()
    {
      return new DescendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit, allWithinThreshold);
    }
  }

  private static class NoFilterOffset implements Offset
  {
    private final int rowCount;
    private final boolean descending;
    private volatile int currentOffset;

    NoFilterOffset(int currentOffset, int rowCount, boolean descending)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
      this.descending = descending;
    }

    @Override
    public void increment()
    {
      currentOffset++;
    }

    @Override
    public boolean withinBounds()
    {
      return currentOffset < rowCount;
    }

    @Override
    public Offset clone()
    {
      return new NoFilterOffset(currentOffset, rowCount, descending);
    }

    @Override
    public int getOffset()
    {
      return descending ? rowCount - currentOffset - 1 : currentOffset;
    }

    @Override
    public String toString()
    {
      return currentOffset + "/" + rowCount + (descending ? "(DSC)" : "");
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
