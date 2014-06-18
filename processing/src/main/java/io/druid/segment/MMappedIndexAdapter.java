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

import com.google.common.collect.Maps;
import com.metamx.common.guava.CloseQuietly;
import io.druid.segment.data.ConciseCompressedIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 */
public class MMappedIndexAdapter implements IndexableAdapter
{
  private final MMappedIndex index;
  private final int numRows;

  public MMappedIndexAdapter(MMappedIndex index)
  {
    this.index = index;

    numRows = index.getReadOnlyTimestamps().size();
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Indexed<String> getAvailableMetrics()
  {
    return index.getAvailableMetrics();
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    return index.getDimValueLookup(dimension);
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return new Iterable<Rowboat>()
    {
      @Override
      public Iterator<Rowboat> iterator()
      {
        return new Iterator<Rowboat>()
        {
          final IndexedLongs timestamps = index.getReadOnlyTimestamps();
          final MetricHolder[] metrics;
          final IndexedFloats[] floatMetrics;
          final Map<String, Indexed<? extends IndexedInts>> dimensions;

          final int numMetrics = index.getAvailableMetrics().size();

          int currRow = 0;
          boolean done = false;

          {
            dimensions = Maps.newLinkedHashMap();
            for (String dim : index.getAvailableDimensions()) {
              dimensions.put(dim, index.getDimColumn(dim));
            }

            final Indexed<String> availableMetrics = index.getAvailableMetrics();
            metrics = new MetricHolder[availableMetrics.size()];
            floatMetrics = new IndexedFloats[availableMetrics.size()];
            for (int i = 0; i < metrics.length; ++i) {
              metrics[i] = index.getMetricHolder(availableMetrics.get(i));
              if (metrics[i].getType() == MetricHolder.MetricType.FLOAT) {
                floatMetrics[i] = metrics[i].getFloatType();
              }
            }
          }

          @Override
          public boolean hasNext()
          {
            final boolean hasNext = currRow < numRows;
            if (!hasNext && !done) {
              CloseQuietly.close(timestamps);
              for (IndexedFloats floatMetric : floatMetrics) {
                CloseQuietly.close(floatMetric);
              }
              done = true;
            }
            return hasNext;
          }

          @Override
          public Rowboat next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            int[][] dims = new int[dimensions.size()][];
            int dimIndex = 0;
            for (String dim : dimensions.keySet()) {
              IndexedInts dimVals = dimensions.get(dim).get(currRow);

              int[] theVals = new int[dimVals.size()];
              for (int j = 0; j < theVals.length; ++j) {
                theVals[j] = dimVals.get(j);
              }

              dims[dimIndex++] = theVals;
            }

            Object[] metricArray = new Object[numMetrics];
            for (int i = 0; i < metricArray.length; ++i) {
              switch (metrics[i].getType()) {
                case FLOAT:
                  metricArray[i] = floatMetrics[i].get(currRow);
                  break;
                case COMPLEX:
                  metricArray[i] = metrics[i].getComplexType().get(currRow);
              }
            }

            Map<String, String> descriptions = Maps.newHashMap();
            for (String spatialDim : index.getSpatialIndexes().keySet()) {
              descriptions.put(spatialDim, "spatial");
            }
            final Rowboat retVal = new Rowboat(timestamps.get(currRow), dims, metricArray, currRow, descriptions);

            ++currRow;

            return retVal;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public IndexedInts getInverteds(String dimension, String value)
  {
    return new ConciseCompressedIndexedInts(index.getInvertedIndex(dimension, value));
  }

  @Override
  public String getMetricType(String metric)
  {
    MetricHolder holder = index.getMetricHolder(metric);
    if (holder == null) {
      return null;
    }
    return holder.getTypeName();
  }
}
