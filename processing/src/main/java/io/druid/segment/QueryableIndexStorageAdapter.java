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
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryInterruptedException;
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
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
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
      CloseQuietly.close(column);
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
      CloseQuietly.close(column);
    }
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public Sequence<Cursor> makeCursors(Filter filter, Interval interval, QueryGranularity gran)
  {
    Interval actualInterval = interval;

    final Interval dataInterval = new Interval(
        getMinTime().getMillis(),
        gran.next(gran.truncate(getMaxTime().getMillis()))
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

    final Offset offset;
    if (filter == null) {
      offset = new NoFilterOffset(0, index.getNumRows());
    } else {
      offset = new ConciseOffset(filter.goConcise(new ColumnSelectorBitmapIndexSelector(index)));
    }

    return Sequences.filter(
        new CursorSequenceBuilder(index, actualInterval, gran, offset).build(),
        Predicates.<Cursor>notNull()
    );
  }

  private static class CursorSequenceBuilder
  {
    private final ColumnSelector index;
    private final Interval interval;
    private final QueryGranularity gran;
    private final Offset offset;

    public CursorSequenceBuilder(
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

    public Sequence<Cursor> build()
    {
      final Offset baseOffset = offset.clone();

      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, ComplexColumn> complexColumnCache = Maps.newHashMap();
      final Map<String, Object> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getTimeColumn().getGenericColumn();

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(gran.iterable(interval.getStartMillis(), interval.getEndMillis())),
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
                      if (Thread.interrupted()) {
                        throw new QueryInterruptedException();
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

                      DictionaryEncodedColumn cachedColumn = dictionaryColumnCache.get(dimensionName);
                      final Column columnDesc = index.getColumn(dimensionName);

                      if (cachedColumn == null && columnDesc != null) {
                        cachedColumn = columnDesc.getDictionaryEncoding();
                        dictionaryColumnCache.put(dimensionName, cachedColumn);
                      }

                      final DictionaryEncodedColumn column = cachedColumn;

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
                            // using an anonymous class is faster than creating a class that stores a copy of the value
                            return new IndexedInts()
                            {
                              @Override
                              public int size()
                              {
                                return 1;
                              }

                              @Override
                              public int get(int index)
                              {
                                return column.getSingleValueRow(cursorOffset.getOffset());
                              }

                              @Override
                              public Iterator<Integer> iterator()
                              {
                                return Iterators.singletonIterator(column.getSingleValueRow(cursorOffset.getOffset()));
                              }
                            };
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

                        if (columnVals.hasMultipleValues()) {
                          throw new UnsupportedOperationException(
                              "makeObjectColumnSelector does not support multivalued GenericColumns"
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
                              final IndexedInts multiValueRow = columnVals.getMultiValueRow(cursorOffset.getOffset());
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
                              return columnVals.lookupName(columnVals.getSingleValueRow(cursorOffset.getOffset()));
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
                          return columnVals.getRowValue(cursorOffset.getOffset());
                        }
                      };
                    }
                  };
                }
              }
          ),
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              CloseQuietly.close(timestamps);
              for (DictionaryEncodedColumn column : dictionaryColumnCache.values()) {
                CloseQuietly.close(column);
              }
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
    private final boolean allWithinThreshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long threshold
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.threshold = threshold;
      // checks if all the values are within the Threshold specified, skips timestamp lookups and checks if all values are within threshold.
      this.allWithinThreshold = timestamps.getLongSingleValueRow(timestamps.length() - 1) < threshold;
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
      return baseOffset.withinBounds() && (allWithinThreshold
                                           || timestamps.getLongSingleValueRow(baseOffset.getOffset()) < threshold);
    }

    @Override
    public void increment()
    {
      baseOffset.increment();
    }
  }

  private static class NoFilterOffset implements Offset
  {
    private final int rowCount;
    private volatile int currentOffset;

    NoFilterOffset(int currentOffset, int rowCount)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
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
      return new NoFilterOffset(currentOffset, rowCount);
    }

    @Override
    public int getOffset()
    {
      return currentOffset;
    }
  }
}
