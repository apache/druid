/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IndexedFloatsGenericColumn;
import io.druid.segment.column.IndexedLongsGenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.BitmapCompressedIndexedInts;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 */
public class QueryableIndexIndexableAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(QueryableIndexIndexableAdapter.class);
  private final int numRows;
  private final QueryableIndex input;
  private final List<String> availableDimensions;

  public QueryableIndexIndexableAdapter(QueryableIndex input)
  {
    this.input = input;
    numRows = input.getNumRows();

    // It appears possible that the dimensions have some columns listed which do not have a DictionaryEncodedColumn
    // This breaks current logic, but should be fine going forward.  This is a work-around to make things work
    // in the current state.  This code shouldn't be needed once github tracker issue #55 is finished.
    this.availableDimensions = Lists.newArrayList();
    for (String dim : input.getAvailableDimensions()) {
      final Column col = input.getColumn(dim);

      if (col == null) {
        log.warn("Wtf!? column[%s] didn't exist!?!?!?", dim);
      } else if (col.getDictionaryEncoding() != null) {
        availableDimensions.add(dim);
      } else {
        log.info("No dictionary on dimension[%s]", dim);
      }
    }
  }

  @Override
  public Interval getDataInterval()
  {
    return input.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public Indexed<String> getDimensionNames()
  {
    return new ListIndexed<>(availableDimensions, String.class);
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    final Set<String> columns = Sets.newLinkedHashSet(input.getColumnNames());
    final HashSet<String> dimensions = Sets.newHashSet(getDimensionNames());

    return new ListIndexed<>(
        Lists.newArrayList(Sets.difference(columns, dimensions)),
        String.class
    );
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return null;
    }

    final DictionaryEncodedColumn dict = column.getDictionaryEncoding();

    if (dict == null) {
      return null;
    }

    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return dict.getCardinality();
      }

      @Override
      public String get(int index)
      {
        return dict.lookupName(index);
      }

      @Override
      public int indexOf(String value)
      {
        return dict.lookupId(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
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
          final GenericColumn timestamps = input.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
          final Object[] metrics;

          final Map<String, DictionaryEncodedColumn> dimensions;

          final int numMetrics = getMetricNames().size();

          int currRow = 0;
          boolean done = false;

          {
            dimensions = Maps.newLinkedHashMap();
            for (String dim : getDimensionNames()) {
              dimensions.put(dim, input.getColumn(dim).getDictionaryEncoding());
            }

            final Indexed<String> availableMetrics = getMetricNames();
            metrics = new Object[availableMetrics.size()];
            for (int i = 0; i < metrics.length; ++i) {
              final Column column = input.getColumn(availableMetrics.get(i));
              final ValueType type = column.getCapabilities().getType();
              switch (type) {
                case FLOAT:
                case LONG:
                  metrics[i] = column.getGenericColumn();
                  break;
                case COMPLEX:
                  metrics[i] = column.getComplexColumn();
                  break;
                default:
                  throw new ISE("Cannot handle type[%s]", type);
              }
            }
          }

          @Override
          public boolean hasNext()
          {
            final boolean hasNext = currRow < numRows;
            if (!hasNext && !done) {
              CloseQuietly.close(timestamps);
              for (Object metric : metrics) {
                if (metric instanceof Closeable) {
                  CloseQuietly.close((Closeable) metric);
                }
              }
              for (Object dimension : dimensions.values()) {
                if(dimension instanceof Closeable) {
                  CloseQuietly.close((Closeable) dimension);
                }
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
              final DictionaryEncodedColumn dict = dimensions.get(dim);
              final IndexedInts dimVals;
              if (dict.hasMultipleValues()) {
                dimVals = dict.getMultiValueRow(currRow);
              } else {
                dimVals = new ArrayBasedIndexedInts(new int[]{dict.getSingleValueRow(currRow)});
              }

              int[] theVals = new int[dimVals.size()];
              for (int j = 0; j < theVals.length; ++j) {
                theVals[j] = dimVals.get(j);
              }

              dims[dimIndex++] = theVals;
            }

            Object[] metricArray = new Object[numMetrics];
            for (int i = 0; i < metricArray.length; ++i) {
              if (metrics[i] instanceof IndexedFloatsGenericColumn) {
                metricArray[i] = ((GenericColumn) metrics[i]).getFloatSingleValueRow(currRow);
              } else if (metrics[i] instanceof IndexedLongsGenericColumn) {
                metricArray[i] = ((GenericColumn) metrics[i]).getLongSingleValueRow(currRow);
              } else if (metrics[i] instanceof ComplexColumn) {
                metricArray[i] = ((ComplexColumn) metrics[i]).getRowValue(currRow);
              }
            }

            final Rowboat retVal = new Rowboat(
                timestamps.getLongSingleValueRow(currRow), dims, metricArray, currRow
            );

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
  public IndexedInts getBitmapIndex(String dimension, String value)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return new EmptyIndexedInts();
    }

    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return new EmptyIndexedInts();
    }

    return new BitmapCompressedIndexedInts(bitmaps.getBitmap(value));
  }

  @Override
  public String getMetricType(String metric)
  {
    final Column column = input.getColumn(metric);

    final ValueType type = column.getCapabilities().getType();
    switch (type) {
      case FLOAT:
        return "float";
      case LONG:
        return "long";
      case COMPLEX:
        return column.getComplexColumn().getTypeName();
      default:
        throw new ISE("Unknown type[%s]", type);
    }
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return input.getColumn(column).getCapabilities();
  }
}
