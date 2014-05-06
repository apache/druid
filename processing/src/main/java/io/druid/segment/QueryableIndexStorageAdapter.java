/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.metamx.common.collect.MoreIterators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.FunctionalIterator;
import io.druid.granularity.QueryGranularity;
import io.druid.query.filter.Filter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.data.SingleIndexedInts;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
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

    Column column = index.getColumn(dimension.toLowerCase());
    if (column == null) {
      return 0;
    }
    if (!column.getCapabilities().isDictionaryEncoded()) {
      throw new UnsupportedOperationException("Only know cardinality of dictionary encoded columns.");
    }
    return column.getDictionaryEncoding().getCardinality();
  }

  @Override
  public DateTime getMinTime()
  {
    GenericColumn column = null;
    try {
      column = index.getTimeColumn().getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(0));
    }
    finally {
      Closeables.closeQuietly(column);
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    GenericColumn column = null;
    try {
      column = index.getTimeColumn().getGenericColumn();
      return new DateTime(column.getLongSingleValueRow(column.length() - 1));
    }
    finally {
      Closeables.closeQuietly(column);
    }
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public Iterable<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
  {
    Interval actualInterval = interval;

    final Interval dataInterval = new Interval(getMinTime().getMillis(), gran.next(getMaxTime().getMillis()));

    if (!actualInterval.overlaps(dataInterval)) {
      return ImmutableList.of();
    }

    if (actualInterval.getStart().isBefore(dataInterval.getStart())) {
      actualInterval = actualInterval.withStart(dataInterval.getStart());
    }
    if (actualInterval.getEnd().isAfter(dataInterval.getEnd())) {
      actualInterval = actualInterval.withEnd(dataInterval.getEnd());
    }

    final Iterable<Cursor> iterable;
    if (filter == null) {
      iterable = new NoFilterCursorIterable(index, actualInterval, gran);
    } else {
      Offset offset = new ConciseOffset(filter.goConcise(new ColumnSelectorBitmapIndexSelector(index)));

      iterable = new CursorIterable(index, actualInterval, gran, offset);
    }

    return FunctionalIterable.create(iterable).keep(Functions.<Cursor>identity());
  }

  private static class CursorIterable implements Iterable<Cursor>
  {
    private final ColumnSelector index;
    private final Interval interval;
    private final QueryGranularity gran;
    private final Offset offset;

    public CursorIterable(
        ColumnSelector index,
        Interval interval,
        QueryGranularity gran,
        Offset offset
    )
    {
      this.index = index;
      this.interval = interval;
      this.gran = gran;
      this.offset = offset;
    }

    @Override
    public Iterator<Cursor> iterator()
    {
      final Offset baseOffset = offset.clone();

      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, Object> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getTimeColumn().getGenericColumn();

      final FunctionalIterator<Cursor> retVal = FunctionalIterator
          .create(gran.iterable(interval.getStartMillis(), interval.getEndMillis()).iterator())
          .transform(
              new Function<Long, Cursor>()
              {

                @Override
                public Cursor apply(final Long input)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), input);
                  while (baseOffset.withinBounds()
                         && timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeStart) {
                    baseOffset.increment();
                  }

                  final Offset offset = new TimestampCheckingOffset(
                      baseOffset, timestamps, Math.min(interval.getEndMillis(), gran.next(input))
                  );

                  return new Cursor()
                  {
                    private final Offset initOffset = offset.clone();
                    private final DateTime myBucket = gran.toDateTime(input);
                    private Offset cursorOffset = offset;

                    @Override
                    public DateTime getTime()
                    {
                      return myBucket;
                    }

                    @Override
                    public void advance()
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
                    public void reset()
                    {
                      cursorOffset = initOffset.clone();
                    }

                    @Override
                    public TimestampColumnSelector makeTimestampColumnSelector()
                    {
                      return new TimestampColumnSelector()
                      {
                        @Override
                        public long getTimestamp()
                        {
                          return timestamps.getLongSingleValueRow(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public DimensionSelector makeDimensionSelector(String dimension)
                    {
                      final String dimensionName = dimension.toLowerCase();
                      final Column columnDesc = index.getColumn(dimensionName);
                      if (columnDesc == null) {
                        return null;
                      }

                      final DictionaryEncodedColumn column = columnDesc.getDictionaryEncoding();

                      if (column == null) {
                        return null;
                      } else if (columnDesc.getCapabilities().hasMultipleValues()) {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            return column.getMultiValueRow(cursorOffset.getOffset());
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return column.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            final String retVal = column.lookupName(id);
                            return retVal == null ? "" : retVal;
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            return column.lookupId(name);
                          }
                        };
                      } else {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            return new SingleIndexedInts(column.getSingleValueRow(cursorOffset.getOffset()));
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return column.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            return column.lookupName(id);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            return column.lookupId(name);
                          }
                        };
                      }
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      final String metricName = columnName.toLowerCase();
                      GenericColumn cachedMetricVals = genericColumnCache.get(metricName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(metricName);
                        if (holder != null && holder.getCapabilities().getType() == ValueType.FLOAT) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(metricName, cachedMetricVals);
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
                          return metricVals.getFloatSingleValueRow(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String column)
                    {
                      final String columnName = column.toLowerCase();

                      Object cachedColumnVals = objectColumnCache.get(columnName);

                      if (cachedColumnVals == null) {
                        Column holder = index.getColumn(columnName);

                        if (holder != null) {
                          final ColumnCapabilities capabilities = holder.getCapabilities();

                          if (capabilities.hasMultipleValues()) {
                            throw new UnsupportedOperationException(
                                "makeObjectColumnSelector does not support multivalued columns"
                            );
                          }

                          if (capabilities.isDictionaryEncoded()) {
                            cachedColumnVals = holder.getDictionaryEncoding();
                          } else if (capabilities.getType() == ValueType.COMPLEX) {
                            cachedColumnVals = holder.getComplexColumn();
                          } else {
                            cachedColumnVals = holder.getGenericColumn();
                          }
                        }

                        if (cachedColumnVals != null) {
                          objectColumnCache.put(columnName, cachedColumnVals);
                        }
                      }

                      if (cachedColumnVals == null) {
                        return null;
                      }

                      if (cachedColumnVals instanceof GenericColumn) {
                        final GenericColumn columnVals = (GenericColumn) cachedColumnVals;
                        final ValueType type = columnVals.getType();

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
                              return columnVals.getFloatSingleValueRow(cursorOffset.getOffset());
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
                              return columnVals.getLongSingleValueRow(cursorOffset.getOffset());
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
                              return columnVals.getStringSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        }
                      }

                      if (cachedColumnVals instanceof DictionaryEncodedColumn) {
                        final DictionaryEncodedColumn columnVals = (DictionaryEncodedColumn) cachedColumnVals;
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
                            return columnVals.lookupName(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                          }
                        };
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
                          return columnVals.getRowValue(cursorOffset.getOffset());
                        }
                      };
                    }
                  };
                }
              }
          );

      // This after call is not perfect, if there is an exception during processing, it will never get called,
      // but it's better than nothing and doing this properly all the time requires a lot more fixerating
      return MoreIterators.after(
          retVal,
          new Runnable()
          {
            @Override
            public void run()
            {
              Closeables.closeQuietly(timestamps);
              for (GenericColumn column : genericColumnCache.values()) {
                Closeables.closeQuietly(column);
              }
              for (ComplexColumn complexColumn : complexColumnCache.values()) {
                Closeables.closeQuietly(complexColumn);
              }
              for (Object column : objectColumnCache.values()) {
                if(column instanceof Closeable) {
                  Closeables.closeQuietly((Closeable) column);
                }
              }
            }
          }
      );
    }
  }

  private static class TimestampCheckingOffset implements Offset
  {
    private final Offset baseOffset;
    private final GenericColumn timestamps;
    private final long threshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long threshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.threshold = threshold;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public Offset clone()
    {
      return new TimestampCheckingOffset(baseOffset.clone(), timestamps, threshold);
    }

    @Override
    public boolean withinBounds()
    {
      return baseOffset.withinBounds() && timestamps.getLongSingleValueRow(baseOffset.getOffset()) < threshold;
    }

    @Override
    public void increment()
    {
      baseOffset.increment();
    }
  }

  private static class NoFilterCursorIterable implements Iterable<Cursor>
  {
    private final ColumnSelector index;
    private final Interval interval;
    private final QueryGranularity gran;

    public NoFilterCursorIterable(
        ColumnSelector index,
        Interval interval,
        QueryGranularity gran
    )
    {
      this.index = index;
      this.interval = interval;
      this.gran = gran;
    }

    /**
     * This produces iterators of Cursor objects that must be fully processed (until isDone() returns true) before the
     * next Cursor is processed.  It is *not* safe to pass these cursors off to another thread for parallel processing
     *
     * @return
     */
    @Override
    public Iterator<Cursor> iterator()
    {
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, Object> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getTimeColumn().getGenericColumn();

      final FunctionalIterator<Cursor> retVal = FunctionalIterator
          .create(gran.iterable(interval.getStartMillis(), interval.getEndMillis()).iterator())
          .transform(
              new Function<Long, Cursor>()
              {
                private int currRow = 0;

                @Override
                public Cursor apply(final Long input)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), input);
                  while (currRow < timestamps.length() && timestamps.getLongSingleValueRow(currRow) < timeStart) {
                    ++currRow;
                  }

                  return new Cursor()
                  {
                    private final DateTime myBucket = gran.toDateTime(input);
                    private final long nextBucket = Math.min(gran.next(myBucket.getMillis()), interval.getEndMillis());
                    private final int initRow = currRow;

                    @Override
                    public DateTime getTime()
                    {
                      return myBucket;
                    }

                    @Override
                    public void advance()
                    {
                      ++currRow;
                    }

                    @Override
                    public void advanceTo(int offset)
                    {
                      currRow += offset;
                    }

                    @Override
                    public boolean isDone()
                    {
                      return currRow >= timestamps.length() || timestamps.getLongSingleValueRow(currRow) >= nextBucket;
                    }

                    @Override
                    public void reset()
                    {
                      currRow = initRow;
                    }

                    @Override
                    public TimestampColumnSelector makeTimestampColumnSelector()
                    {
                      return new TimestampColumnSelector()
                      {
                        @Override
                        public long getTimestamp()
                        {
                          return timestamps.getLongSingleValueRow(currRow);
                        }
                      };
                    }

                    @Override
                    public DimensionSelector makeDimensionSelector(String dimension)
                    {
                      final String dimensionName = dimension.toLowerCase();
                      final Column column = index.getColumn(dimensionName);
                      if (column == null) {
                        return null;
                      }

                      final DictionaryEncodedColumn dict = column.getDictionaryEncoding();

                      if (dict == null) {
                        return null;
                      } else if (column.getCapabilities().hasMultipleValues()) {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            return dict.getMultiValueRow(currRow);
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return dict.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            final String retVal = dict.lookupName(id);
                            return retVal == null ? "" : retVal;
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            return dict.lookupId(name);
                          }
                        };
                      } else {
                        return new DimensionSelector()
                        {
                          @Override
                          public IndexedInts getRow()
                          {
                            return new SingleIndexedInts(dict.getSingleValueRow(currRow));
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return dict.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            return dict.lookupName(id);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            return dict.lookupId(name);
                          }
                        };
                      }
                    }

                    @Override
                    public FloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      final String metricName = columnName.toLowerCase();
                      GenericColumn cachedMetricVals = genericColumnCache.get(metricName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(metricName);
                        if (holder != null && holder.getCapabilities().getType() == ValueType.FLOAT) {
                          cachedMetricVals = holder.getGenericColumn();
                          genericColumnCache.put(metricName, cachedMetricVals);
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
                          return metricVals.getFloatSingleValueRow(currRow);
                        }
                      };
                    }

                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String column)
                    {
                      final String columnName = column.toLowerCase();

                      Object cachedColumnVals = objectColumnCache.get(columnName);

                      if (cachedColumnVals == null) {
                        Column holder = index.getColumn(columnName);

                        if (holder != null) {
                          if (holder.getCapabilities().hasMultipleValues()) {
                            throw new UnsupportedOperationException(
                                "makeObjectColumnSelector does not support multivalued columns"
                            );
                          }
                          final ValueType type = holder.getCapabilities().getType();

                          if (holder.getCapabilities().isDictionaryEncoded()) {
                            cachedColumnVals = holder.getDictionaryEncoding();
                          } else if (type == ValueType.COMPLEX) {
                            cachedColumnVals = holder.getComplexColumn();
                          } else {
                            cachedColumnVals = holder.getGenericColumn();
                          }
                        }

                        if (cachedColumnVals != null) {
                          objectColumnCache.put(columnName, cachedColumnVals);
                        }
                      }

                      if (cachedColumnVals == null) {
                        return null;
                      }

                      if (cachedColumnVals instanceof GenericColumn) {
                        final GenericColumn columnVals = (GenericColumn) cachedColumnVals;
                        final ValueType type = columnVals.getType();

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
                              return columnVals.getFloatSingleValueRow(currRow);
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
                              return columnVals.getLongSingleValueRow(currRow);
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
                              return columnVals.getStringSingleValueRow(currRow);
                            }
                          };
                        }
                      }

                      if (cachedColumnVals instanceof DictionaryEncodedColumn) {
                        final DictionaryEncodedColumn columnVals = (DictionaryEncodedColumn) cachedColumnVals;
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
                            return columnVals.lookupName(columnVals.getSingleValueRow(currRow));
                          }
                        };
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
                          return columnVals.getRowValue(currRow);
                        }
                      };
                    }
                  };
                }
              }
          );

      return MoreIterators.after(
          retVal,
          new Runnable()
          {
            @Override
            public void run()
            {
              Closeables.closeQuietly(timestamps);
              for (GenericColumn column : genericColumnCache.values()) {
                Closeables.closeQuietly(column);
              }
              for (ComplexColumn complexColumn : complexColumnCache.values()) {
                Closeables.closeQuietly(complexColumn);
              }
              for (Object column : objectColumnCache.values()) {
                if (column instanceof Closeable) {
                  Closeables.closeQuietly((Closeable) column);
                }
              }
            }
          }
      );
    }
  }

  private static class NullDimensionSelector implements DimensionSelector
  {
    @Override
    public IndexedInts getRow()
    {
      return new SingleIndexedInts(0);
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    public String lookupName(int id)
    {
      return "";
    }

    @Override
    public int lookupId(String name)
    {
      return 0;
    }
  }
}
