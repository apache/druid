package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.collect.MoreIterators;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.druid.BaseStorageAdapter;
import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.brita.InvertedIndexSelector;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.index.v1.processing.DimensionSelector;
import com.metamx.druid.index.v1.processing.Offset;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedFloats;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.IndexedLongs;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.FloatMetricSelector;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class MMappedIndexStorageAdapter extends BaseStorageAdapter
{
  private final MMappedIndex index;

  public MMappedIndexStorageAdapter(
      MMappedIndex index
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
  public int getDimensionCardinality(String dimension)
  {
    final Indexed<String> dimValueLookup = index.getDimValueLookup(dimension);
    if (dimValueLookup == null) {
      return 0;
    }
    return dimValueLookup.size();
  }

  @Override
  public DateTime getMinTime()
  {
    final IndexedLongs timestamps = index.getReadOnlyTimestamps();
    final DateTime retVal = new DateTime(timestamps.get(0));
    Closeables.closeQuietly(timestamps);
    return retVal;
  }

  @Override
  public DateTime getMaxTime()
  {
    final IndexedLongs timestamps = index.getReadOnlyTimestamps();
    final DateTime retVal = new DateTime(timestamps.get(timestamps.size() - 1));
    Closeables.closeQuietly(timestamps);
    return retVal;
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
    if (!actualInterval.overlaps(index.dataInterval)) {
      return ImmutableList.of();
    }

    if (actualInterval.getStart().isBefore(index.dataInterval.getStart())) {
      actualInterval = actualInterval.withStart(index.dataInterval.getStart());
    }
    if (actualInterval.getEnd().isAfter(index.dataInterval.getEnd())) {
      actualInterval = actualInterval.withEnd(index.dataInterval.getEnd());
    }

    final Iterable<Cursor> iterable;
    if (filter == null) {
      iterable = new NoFilterCursorIterable(index, actualInterval, gran);
    } else {
      Offset offset = new ConciseOffset(filter.goConcise(new MMappedInvertedIndexSelector(index)));

      iterable = new CursorIterable(index, actualInterval, gran, offset);
    }

    return FunctionalIterable.create(iterable).keep(Functions.<Cursor>identity());
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return index.getDimValueLookup(dimension);
  }

  @Override
  public ImmutableConciseSet getInvertedIndex(String dimension, String dimVal)
  {
    return index.getInvertedIndex(dimension, dimVal);
  }

  @Override
  public Offset getFilterOffset(Filter filter)
  {
    return new ConciseOffset(
        filter.goConcise(
            new MMappedInvertedIndexSelector(index)
        )
    );
  }

  private static class CursorIterable implements Iterable<Cursor>
  {
    private final MMappedIndex index;
    private final Interval interval;
    private final QueryGranularity gran;
    private final Offset offset;

    public CursorIterable(
        MMappedIndex index,
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

      final Map<String, Object> metricHolderCache = Maps.newHashMap();
      final IndexedLongs timestamps = index.getReadOnlyTimestamps();

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
                         && timestamps.get(baseOffset.getOffset()) < timeStart) {
                    baseOffset.increment();
                  }

                  final Offset offset = new TimestampCheckingOffset(
                      baseOffset, timestamps, Math.min(interval.getEndMillis(), gran.next(timeStart))
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
                    public DimensionSelector makeDimensionSelector(final String dimensionName)
                    {
                      final Indexed<? extends IndexedInts> rowVals = index.getDimColumn(dimensionName);
                      final Indexed<String> dimValueLookup = index.getDimValueLookup(dimensionName);

                      if (rowVals == null) {
                        return null;
                      }

                      return new DimensionSelector()
                      {
                        @Override
                        public IndexedInts getRow()
                        {
                          return rowVals.get(cursorOffset.getOffset());
                        }

                        @Override
                        public int getValueCardinality()
                        {
                          return dimValueLookup.size();
                        }

                        @Override
                        public String lookupName(int id)
                        {
                          final String retVal = dimValueLookup.get(id);
                          return retVal == null ? "" : retVal;
                        }

                        @Override
                        public int lookupId(String name)
                        {
                          return ("".equals(name)) ? dimValueLookup.indexOf(null) : dimValueLookup.indexOf(name);
                        }
                      };
                    }

                    @Override
                    public FloatMetricSelector makeFloatMetricSelector(String metricName)
                    {
                      IndexedFloats cachedMetricVals = (IndexedFloats) metricHolderCache.get(metricName);

                      if (cachedMetricVals == null) {
                        MetricHolder holder = index.getMetricHolder(metricName);
                        if (holder != null) {
                          cachedMetricVals = holder.getFloatType();
                          metricHolderCache.put(metricName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new FloatMetricSelector()
                        {
                          @Override
                          public float get()
                          {
                            return 0.0f;
                          }
                        };
                      }

                      final IndexedFloats metricVals = cachedMetricVals;
                      return new FloatMetricSelector()
                      {
                        @Override
                        public float get()
                        {
                          return metricVals.get(cursorOffset.getOffset());
                        }
                      };
                    }

                    @Override
                    public ComplexMetricSelector makeComplexMetricSelector(String metricName)
                    {
                      Indexed cachedMetricVals = (Indexed) metricHolderCache.get(metricName);

                      if (cachedMetricVals == null) {
                        MetricHolder holder = index.getMetricHolder(metricName);
                        if (holder != null) {
                          cachedMetricVals = holder.getComplexType();
                          metricHolderCache.put(metricName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return null;
                      }

                      final Indexed metricVals = cachedMetricVals;
                      return new ComplexMetricSelector()
                      {
                        @Override
                        public Class classOfObject()
                        {
                          return metricVals.getClazz();
                        }

                        @Override
                        public Object get()
                        {
                          return metricVals.get(cursorOffset.getOffset());
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
              for (Object object : metricHolderCache.values()) {
                if (object instanceof Closeable) {
                  Closeables.closeQuietly((Closeable) object);
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
    private final IndexedLongs timestamps;
    private final long threshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        IndexedLongs timestamps,
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
      return baseOffset.withinBounds() && timestamps.get(baseOffset.getOffset()) < threshold;
    }

    @Override
    public void increment()
    {
      baseOffset.increment();
    }
  }

  private static class NoFilterCursorIterable implements Iterable<Cursor>
  {
    private final MMappedIndex index;
    private final Interval interval;
    private final QueryGranularity gran;

    public NoFilterCursorIterable(
        MMappedIndex index,
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
      final Map<String, Object> metricCacheMap = Maps.newHashMap();
      final IndexedLongs timestamps = index.getReadOnlyTimestamps();

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
                  while (currRow < timestamps.size() && timestamps.get(currRow) < timeStart) {
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
                    public boolean isDone()
                    {
                      return currRow >= timestamps.size() || timestamps.get(currRow) >= nextBucket;
                    }

                    @Override
                    public void reset()
                    {
                      currRow = initRow;
                    }

                    @Override
                    public DimensionSelector makeDimensionSelector(final String dimensionName)
                    {
                      final Indexed<? extends IndexedInts> rowVals = index.getDimColumn(dimensionName);
                      final Indexed<String> dimValueLookup = index.getDimValueLookup(dimensionName);

                      if (rowVals == null) {
                        return null;
                      }

                      return new DimensionSelector()
                      {
                        @Override
                        public IndexedInts getRow()
                        {
                          return rowVals.get(currRow);
                        }

                        @Override
                        public int getValueCardinality()
                        {
                          return dimValueLookup.size();
                        }

                        @Override
                        public String lookupName(int id)
                        {
                          final String retVal = dimValueLookup.get(id);
                          return retVal == null ? "" : retVal;
                        }

                        @Override
                        public int lookupId(String name)
                        {
                          return ("".equals(name)) ? dimValueLookup.indexOf(null) : dimValueLookup.indexOf(name);
                        }
                      };
                    }

                    @Override
                    public FloatMetricSelector makeFloatMetricSelector(String metricName)
                    {
                      IndexedFloats cachedMetricVals = (IndexedFloats) metricCacheMap.get(metricName);

                      if (cachedMetricVals == null) {
                        final MetricHolder metricHolder = index.getMetricHolder(metricName);
                        if (metricHolder != null) {
                          cachedMetricVals = metricHolder.getFloatType();
                          if (cachedMetricVals != null) {
                            metricCacheMap.put(metricName, cachedMetricVals);
                          }
                        }
                      }

                      if (cachedMetricVals == null) {
                        return new FloatMetricSelector()
                        {
                          @Override
                          public float get()
                          {
                            return 0.0f;
                          }
                        };
                      }

                      final IndexedFloats metricVals = cachedMetricVals;
                      return new FloatMetricSelector()
                      {
                        @Override
                        public float get()
                        {
                          return metricVals.get(currRow);
                        }
                      };
                    }

                    @Override
                    public ComplexMetricSelector makeComplexMetricSelector(String metricName)
                    {
                      Indexed cachedMetricVals = (Indexed) metricCacheMap.get(metricName);

                      if (cachedMetricVals == null) {
                        final MetricHolder metricHolder = index.getMetricHolder(metricName);

                        if (metricHolder != null) {
                          cachedMetricVals = metricHolder.getComplexType();
                          if (cachedMetricVals != null) {
                            metricCacheMap.put(metricName, cachedMetricVals);
                          }
                        }
                      }

                      if (cachedMetricVals == null) {
                        return null;
                      }

                      final Indexed metricVals = cachedMetricVals;
                      return new ComplexMetricSelector()
                      {
                        @Override
                        public Class classOfObject()
                        {
                          return metricVals.getClazz();
                        }

                        @Override
                        public Object get()
                        {
                          return metricVals.get(currRow);
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
              for (Object object : metricCacheMap.values()) {
                if (object instanceof Closeable) {
                  Closeables.closeQuietly((Closeable) object);
                }
              }
            }
          }
      );
    }
  }

  private static class MMappedInvertedIndexSelector implements InvertedIndexSelector
  {
    private final MMappedIndex index;

    public MMappedInvertedIndexSelector(final MMappedIndex index)
    {
      this.index = index;
    }

    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      return index.getDimValueLookup(dimension);
    }

    @Override
    public int getNumRows()
    {
      return index.getReadOnlyTimestamps().size();
    }

    @Override
    public int[] getInvertedIndex(String dimension, String value)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Offset getInvertedIndexOffset(String dimension, String value)
    {
      return new ConciseOffset(index.getInvertedIndex(dimension, value));
    }

    @Override
    public ImmutableConciseSet getConciseInvertedIndex(String dimension, String value)
    {
      return index.getInvertedIndex(dimension, value);
    }
  }
}
