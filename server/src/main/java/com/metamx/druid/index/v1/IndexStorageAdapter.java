/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.Pair;
import com.metamx.common.collect.MoreIterators;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.common.logger.Logger;
import com.metamx.druid.BaseStorageAdapter;
import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.index.brita.BitmapIndexSelector;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.ArrayBasedOffset;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.index.v1.processing.DimensionSelector;
import com.metamx.druid.index.v1.processing.Offset;
import com.metamx.druid.index.v1.processing.StartLimitedOffset;
import com.metamx.druid.kv.ArrayBasedIndexedInts;
import com.metamx.druid.kv.ArrayIndexed;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedFloats;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.ListIndexed;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.FloatMetricSelector;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class IndexStorageAdapter extends BaseStorageAdapter
{
  private final Logger log = new Logger(IndexStorageAdapter.class);

  private final Index index;

  private final int[] ids;

  private final Capabilities capabilities;

  public IndexStorageAdapter(
      Index index
  )
  {
    this.index = index;

    capabilities = Capabilities.builder()
                               .dimensionValuesSorted(isReverseDimSorted())
                               .build();

    ids = new int[index.timeOffsets.length];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i;
    }
  }

  @Override
  public String getSegmentIdentifier()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Interval getInterval()
  {
    return index.dataInterval;
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    final String[] strings = index.reverseDimLookup.get(dimension);
    return strings == null ? 0 : strings.length;
  }

  @Override
  public DateTime getMinTime()
  {
    return new DateTime(index.timeOffsets[0]);
  }

  @Override
  public DateTime getMaxTime()
  {
    return new DateTime(index.timeOffsets[index.timeOffsets.length - 1]);
  }

  @Override
  public Iterable<Cursor> makeCursors(final Filter filter, final Interval interval, final QueryGranularity gran)
  {
    Interval actualIntervalTmp = interval;
    if (!actualIntervalTmp.overlaps(index.dataInterval)) {
      return ImmutableList.of();
    }

    if (actualIntervalTmp.getStart().isBefore(index.dataInterval.getStart())) {
      actualIntervalTmp = actualIntervalTmp.withStart(index.dataInterval.getStart());
    }
    if (actualIntervalTmp.getEnd().isAfter(index.dataInterval.getEnd())) {
      actualIntervalTmp = actualIntervalTmp.withEnd(index.dataInterval.getEnd());
    }

    final Interval actualInterval = actualIntervalTmp;

    final Pair<Integer, Integer> intervalStartAndEnd = computeTimeStartEnd(actualInterval);

    return new Iterable<Cursor>()
    {
      @Override
      public Iterator<Cursor> iterator()
      {
        final Offset baseOffset;
        if (filter == null) {
          baseOffset = new ArrayBasedOffset(ids, intervalStartAndEnd.lhs);
        } else {
          baseOffset = new StartLimitedOffset(
              new ConciseOffset(filter.goConcise(new IndexBasedBitmapIndexSelector(index))),
              intervalStartAndEnd.lhs
          );
        }

        final Map<String, Object> metricHolderCache = Maps.newHashMap();

        // This after call is not perfect, if there is an exception during processing, it will never get called,
        // but it's better than nothing and doing this properly all the time requires a lot more fixerating
        return MoreIterators.after(
            FunctionalIterator
                .create(gran.iterable(actualInterval.getStartMillis(), actualInterval.getEndMillis()).iterator())
                .keep(
                    new Function<Long, Cursor>()
                    {
                      @Override
                      public Cursor apply(final Long intervalStart)
                      {
                        final Offset offset = new TimestampCheckingOffset(
                            baseOffset,
                            index.timeOffsets,
                            Math.min(actualInterval.getEndMillis(), gran.next(intervalStart))
                        );

                        return new Cursor()
                        {

                          private final Offset initOffset = offset.clone();
                          private Offset cursorOffset = offset;
                          private final DateTime timestamp = gran.toDateTime(intervalStart);

                          @Override
                          public DateTime getTime()
                          {
                            return timestamp;
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
                          public DimensionSelector makeDimensionSelector(String dimension)
                          {
                            final String dimensionName = dimension.toLowerCase();
                            final String[] nameLookup = index.reverseDimLookup.get(dimensionName);
                            if (nameLookup == null) {
                              return null;
                            }

                            return new DimensionSelector()
                            {
                              final Map<String, Integer> dimValLookup = index.dimIdLookup.get(dimensionName);
                              final DimensionColumn dimColumn = index.dimensionValues.get(dimensionName);
                              final int[][] dimensionExpansions = dimColumn.getDimensionExpansions();
                              final int[] dimensionRowValues = dimColumn.getDimensionRowValues();

                              @Override
                              public IndexedInts getRow()
                              {
                                return new ArrayBasedIndexedInts(dimensionExpansions[dimensionRowValues[cursorOffset.getOffset()]]);
                              }

                              @Override
                              public int getValueCardinality()
                              {
                                return nameLookup.length;
                              }

                              @Override
                              public String lookupName(int id)
                              {
                                return nameLookup[id];
                              }

                              @Override
                              public int lookupId(String name)
                              {
                                final Integer retVal = dimValLookup.get(name);

                                return retVal == null ? -1 : retVal;
                              }
                            };
                          }

                          @Override
                          public FloatMetricSelector makeFloatMetricSelector(String metric)
                          {
                            String metricName = metric.toLowerCase();
                            IndexedFloats cachedFloats = (IndexedFloats) metricHolderCache.get(metric);
                            if (cachedFloats == null) {
                              MetricHolder holder = index.metricVals.get(metricName);
                              if (holder == null) {
                                return new FloatMetricSelector()
                                {
                                  @Override
                                  public float get()
                                  {
                                    return 0.0f;
                                  }
                                };
                              }

                              cachedFloats = holder.getFloatType();
                              metricHolderCache.put(metricName, cachedFloats);
                            }

                            final IndexedFloats metricVals = cachedFloats;
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
                          public ComplexMetricSelector makeComplexMetricSelector(String metric)
                          {
                            final String metricName = metric.toLowerCase();
                            Indexed cachedComplex = (Indexed) metricHolderCache.get(metricName);
                            if (cachedComplex == null) {
                              MetricHolder holder = index.metricVals.get(metricName);
                              if (holder != null) {
                                cachedComplex = holder.getComplexType();
                                metricHolderCache.put(metricName, cachedComplex);
                              }
                            }

                            if (cachedComplex == null) {
                              return null;
                            }

                            final Indexed vals = cachedComplex;
                            return new ComplexMetricSelector()
                            {
                              @Override
                              public Class classOfObject()
                              {
                                return vals.getClazz();
                              }

                              @Override
                              public Object get()
                              {
                                return vals.get(cursorOffset.getOffset());
                              }
                            };
                          }
                        };
                      }
                    }
                ),
            new Runnable()
            {
              @Override
              public void run()
              {
                for (Object object : metricHolderCache.values()) {
                  if (object instanceof Closeable) {
                    Closeables.closeQuietly((Closeable) object);
                  }
                }
              }
            }
        );
      }
    };
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ArrayIndexed<String>(index.dimensions, String.class);
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return new ListIndexed<String>(
        Lists.newArrayList(index.dimIdLookup.get(dimension.toLowerCase()).keySet()), String.class
    );
  }

  @Override
  public ImmutableConciseSet getInvertedIndex(String dimension, String dimVal)
  {
    return index.getInvertedIndex(dimension.toLowerCase(), dimVal);
  }

  @Override
  public Offset getFilterOffset(Filter filter)
  {
    return new ConciseOffset(filter.goConcise(new IndexBasedBitmapIndexSelector(index)));
  }

  @Override
  public Capabilities getCapabilities()
  {
    return capabilities;
  }

  private boolean isReverseDimSorted()
  {
    for (Map.Entry<String, String[]> entry : index.reverseDimLookup.entrySet()) {
      String[] arr = entry.getValue();
      for (int i = 0; i < arr.length - 1; i++) {
        if (arr[i].compareTo(arr[i + 1]) > 0) {
          return false;
        }
      }
    }
    return true;
  }

  private Pair<Integer, Integer> computeTimeStartEnd(Interval interval)
  {
    DateTime actualIntervalStart = index.dataInterval.getStart();
    DateTime actualIntervalEnd = index.dataInterval.getEnd();

    if (index.dataInterval.contains(interval.getStart())) {
      actualIntervalStart = interval.getStart();
    }

    if (index.dataInterval.contains(interval.getEnd())) {
      actualIntervalEnd = interval.getEnd();
    }

    return computeOffsets(actualIntervalStart.getMillis(), 0, actualIntervalEnd.getMillis(), index.timeOffsets.length);
  }

  private Pair<Integer, Integer> computeOffsets(long startMillis, int startOffset, long endMillis, int endOffset)
  {
    int startIndex = startOffset;
    int endIndex = endOffset;

    if (index.timeOffsets[startIndex] < startMillis) {
      startIndex = Math.abs(Arrays.binarySearch(index.timeOffsets, startMillis));

      if (startIndex >= endOffset) {
        return new Pair<Integer, Integer>(0, 0);
      }

      while (startIndex > 0 && index.timeOffsets[startIndex - 1] == startMillis) {
        --startIndex;
      }
    }

    if (index.timeOffsets[endIndex - 1] >= endMillis) {
      endIndex = Math.abs(Arrays.binarySearch(index.timeOffsets, endMillis));

      while (endIndex > startIndex && index.timeOffsets[endIndex - 1] == endMillis) {
        --endIndex;
      }
    }

    return new Pair<Integer, Integer>(startIndex, endIndex);
  }

  private static class TimestampCheckingOffset implements Offset
  {
    private final Offset baseOffset;
    private final long[] timestamps;
    private final long threshold;

    public TimestampCheckingOffset(
        Offset baseOffset,
        long[] timestamps,
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
      return baseOffset.withinBounds() && timestamps[baseOffset.getOffset()] < threshold;
    }

    @Override
    public void increment()
    {
      baseOffset.increment();
    }
  }

  private static class IndexBasedBitmapIndexSelector implements BitmapIndexSelector
  {
    private final Index index;

    public IndexBasedBitmapIndexSelector(final Index index)
    {
      this.index = index;
    }

    @Override
    public Indexed<String> getDimensionValues(final String dimension)
    {
      return new Indexed<String>()
      {
        private final String[] dimVals = index.reverseDimLookup.get(dimension.toLowerCase());

        @Override
        public Class<? extends String> getClazz()
        {
          return String.class;
        }

        @Override
        public int size()
        {
          return dimVals.length;
        }

        @Override
        public String get(int index)
        {
          return dimVals[index];
        }

        @Override
        public int indexOf(String value)
        {
          return Arrays.binarySearch(dimVals, value);
        }

        @Override
        public Iterator<String> iterator()
        {
          return Arrays.asList(dimVals).iterator();
        }
      };
    }

    @Override
    public int getNumRows()
    {
      return index.timeOffsets.length;
    }

    @Override
    public ImmutableConciseSet getConciseInvertedIndex(String dimension, String value)
    {
      return index.getInvertedIndex(dimension.toLowerCase(), value);
    }
  }
}
