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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.EmptyBitmapIndexSeeker;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IndexedFloatsGenericColumn;
import io.druid.segment.column.IndexedLongsGenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.BitmapCompressedIndexedInts;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

          final DictionaryEncodedColumn[] dictionaryEncodedColumns;

          final int numMetrics = getMetricNames().size();

          int currRow = 0;
          boolean done = false;

          {
            this.dictionaryEncodedColumns = FluentIterable
                .from(getDimensionNames())
                .transform(
                    new Function<String, DictionaryEncodedColumn>()
                    {
                      @Override
                      public DictionaryEncodedColumn apply(String dimName)
                      {
                        return input.getColumn(dimName)
                                    .getDictionaryEncoding();
                      }
                    }
                ).toArray(DictionaryEncodedColumn.class);

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
              for (Object dimension : dictionaryEncodedColumns) {
                if (dimension instanceof Closeable) {
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

            final int[][] dims = new int[dictionaryEncodedColumns.length][];
            int dimIndex = 0;
            for (final DictionaryEncodedColumn dict : dictionaryEncodedColumns) {
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

  @Override
  public BitmapIndexSeeker getBitmapIndexSeeker(String dimension)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return new EmptyBitmapIndexSeeker();
    }

    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return new EmptyBitmapIndexSeeker();
    }

    final Indexed<String> dimSet = getDimValueLookup(dimension);

    // BitmapIndexSeeker is the main performance boost comes from.
    // In the previous version of index merge, during the creation of invert index, we do something like
    // merge sort of multiply bitmap indexes. It simply iterator all the previous sorted values,
    // and "binary find" the id in each bitmap indexes,  which involves disk IO and is really slow.
    // Suppose we have N (which is 100 in our test) small segments,  each have M (which is 50000 in our case) rows.
    // In high cardinality scenario, we will almost have N * M uniq values. So the complexity will be O(N * M * M * LOG(M)).

    // There are 2 properties we did not use during the merging:
    // 1. We always travel the dimension values sequentially
    // 2. One single dimension value is valid only in one index when cardinality is high enough
    // So we introduced the BitmapIndexSeeker, which can only seek value sequentially and can never seek back.
    // By using this and the help of "getDimValueLookup", we only need to translate all dimension value to its ID once,
    // and the translation is done by self-increase of the integer. We only need to change the CACHED value once after
    // previous value is hit, renew the value and increase the ID. The complexity now is O(N * M * LOG(M)).
    return new BitmapIndexSeeker()
    {
      private int currIndex = 0;
      private String currVal = null;
      private String lastVal = null;

      @Override
      public IndexedInts seek(String value)
      {
        if (dimSet == null || dimSet.size() == 0) {
          return new EmptyIndexedInts();
        }
        if (lastVal != null) {
          if (GenericIndexed.STRING_STRATEGY.compare(value, lastVal) <= 0) {
            throw new ISE("Value[%s] is less than the last value[%s] I have, cannot be.",
                value, lastVal);
          }
          return new EmptyIndexedInts();
        }
        if (currVal == null) {
          currVal = dimSet.get(currIndex);
        }
        int compareResult = GenericIndexed.STRING_STRATEGY.compare(currVal, value);
        if (compareResult == 0) {
          IndexedInts ret = new BitmapCompressedIndexedInts(bitmaps.getBitmap(currIndex));
          ++currIndex;
          if (currIndex == dimSet.size()) {
            lastVal = value;
          } else {
            currVal = dimSet.get(currIndex);
          }
          return ret;
        } else if (compareResult < 0) {
          throw new ISE("Skipped currValue[%s], currIndex[%,d]; incoming value[%s]",
              currVal, currIndex, value);
        } else {
          return new EmptyIndexedInts();
        }
      }
    };
  }
}
