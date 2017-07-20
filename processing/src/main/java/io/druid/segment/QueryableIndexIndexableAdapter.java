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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IndexedDoublesGenericColumn;
import io.druid.segment.column.IndexedFloatsGenericColumn;
import io.druid.segment.column.IndexedLongsGenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapCompressedIndexedInts;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Collection;
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
  private final Metadata metadata;

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
      } else {
        if (col.getDictionaryEncoding() == null) {
          log.info("No dictionary on dimension[%s]", dim);
        }
        availableDimensions.add(dim);
      }
    }

    this.metadata = input.getMetadata();
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
  public Indexed<Comparable> getDimValueLookup(String dimension)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return null;
    }

    final DictionaryEncodedColumn dict = column.getDictionaryEncoding();

    if (dict == null) {
      return null;
    }

    return new Indexed<Comparable>()
    {
      @Override
      public Class<? extends Comparable> getClazz()
      {
        return Comparable.class;
      }

      @Override
      public int size()
      {
        return dict.getCardinality();
      }

      @Override
      public Comparable get(int index)
      {
        return dict.lookupName(index);
      }

      @Override
      public int indexOf(Comparable value)
      {
        return dict.lookupId(value);
      }

      @Override
      public Iterator<Comparable> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("dict", dict);
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
          final Closeable[] metrics;
          final Closeable[] columns;
          final Closer closer = Closer.create();

          final int numMetrics = getMetricNames().size();

          final DimensionHandler[] handlers = new DimensionHandler[availableDimensions.size()];
          Collection<DimensionHandler> handlerSet = input.getDimensionHandlers().values();

          int currRow = 0;
          boolean done = false;

          {
            closer.register(timestamps);

            handlerSet.toArray(handlers);
            this.columns = FluentIterable
                .from(handlerSet)
                .transform(
                    new Function<DimensionHandler, Closeable>()
                    {
                      @Override
                      public Closeable apply(DimensionHandler handler)
                      {
                        Column column = input.getColumn(handler.getDimensionName());
                        return handler.getSubColumn(column);
                      }
                    }
                ).toArray(Closeable.class);
            for (Closeable column : columns) {
              closer.register(column);
            }

            final Indexed<String> availableMetrics = getMetricNames();
            metrics = new Closeable[availableMetrics.size()];
            for (int i = 0; i < metrics.length; ++i) {
              final Column column = input.getColumn(availableMetrics.get(i));
              final ValueType type = column.getCapabilities().getType();
              switch (type) {
                case FLOAT:
                case LONG:
                case DOUBLE:
                  metrics[i] = column.getGenericColumn();
                  break;
                case COMPLEX:
                  metrics[i] = column.getComplexColumn();
                  break;
                default:
                  throw new ISE("Cannot handle type[%s]", type);
              }
            }
            for (Closeable metricColumn : metrics) {
              closer.register(metricColumn);
            }
          }

          @Override
          public boolean hasNext()
          {
            final boolean hasNext = currRow < numRows;
            if (!hasNext && !done) {
              CloseQuietly.close(closer);
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

            final Object[] dims = new Object[columns.length];
            int dimIndex = 0;
            for (final Closeable column : columns) {
              dims[dimIndex] = handlers[dimIndex].getEncodedKeyComponentFromColumn(column, currRow);
              dimIndex++;
            }

            Object[] metricArray = new Object[numMetrics];
            for (int i = 0; i < metricArray.length; ++i) {
              if (metrics[i] instanceof IndexedFloatsGenericColumn) {
                metricArray[i] = ((GenericColumn) metrics[i]).getFloatSingleValueRow(currRow);
              } else if (metrics[i] instanceof IndexedDoublesGenericColumn) {
                metricArray[i] = ((GenericColumn) metrics[i]).getDoubleSingleValueRow(currRow);
              } else if (metrics[i] instanceof IndexedLongsGenericColumn) {
                metricArray[i] = ((GenericColumn) metrics[i]).getLongSingleValueRow(currRow);
              } else if (metrics[i] instanceof ComplexColumn) {
                metricArray[i] = ((ComplexColumn) metrics[i]).getRowValue(currRow);
              }
            }

            final Rowboat retVal = new Rowboat(
                timestamps.getLongSingleValueRow(currRow), dims, metricArray, currRow, handlers
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

  @VisibleForTesting
  IndexedInts getBitmapIndex(String dimension, String value)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    return new BitmapCompressedIndexedInts(bitmaps.getBitmap(bitmaps.getIndex(value)));
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
      case DOUBLE:
        return "double";
      case COMPLEX: {
        try (ComplexColumn complexColumn = column.getComplexColumn() ) {
          return complexColumn.getTypeName();
        }
      }
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
  public IndexedInts getBitmapIndex(String dimension, int dictId)
  {
    final Column column = input.getColumn(dimension);
    if (column == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    if (dictId >= 0) {
      return new BitmapCompressedIndexedInts(bitmaps.getBitmap(dictId));
    } else {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return input.getDimensionHandlers();
  }
}
