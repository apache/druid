/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.rowsandcols;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ColumnValueSwapper;
import org.apache.druid.query.rowsandcols.column.DefaultVectorCopier;
import org.apache.druid.query.rowsandcols.column.LimitedColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.column.VectorCopier;
import org.apache.druid.query.rowsandcols.column.accessor.ObjectColumnAccessorBase;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.NaiveSortMaker;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * ArrayListRowsAndColumns is a RowsAndColumns implementation that believes it has all of its data on-heap.
 * <p>
 * It is an AppendableRowsAndColumns and, as with all RowsAndColumns, it is not thread-safe for multiple writes.
 * Under certain circumstances, concurrent reads from multiple threads can be correct, but the code has to be follow
 * a very strict ordering of code that ensures that reads only happen after writes and once a value is read, it will
 * never be overwritten.
 * <p>
 * Additionally, this object implements various of the semantic interfaces directly to provide some degree
 * of processing and memory optimization.
 *
 * @param <RowType>
 */
public class ArrayListRowsAndColumns<RowType> implements AppendableRowsAndColumns
{
  @SuppressWarnings("rawtypes")
  private static final Map<Class<?>, Function<ArrayListRowsAndColumns, ?>> AS_MAP = RowsAndColumns
      .makeAsMap(ArrayListRowsAndColumns.class);

  private final ArrayList<RowType> rows;
  private final RowAdapter<RowType> rowAdapter;
  private final RowSignature rowSignature;
  private final Map<String, Column> extraColumns;
  private final Set<String> columnNames;
  private final int startOffset;
  private final int endOffset;


  public ArrayListRowsAndColumns(
      ArrayList<RowType> rows,
      RowAdapter<RowType> rowAdapter,
      RowSignature rowSignature
  )
  {
    this(
        rows,
        rowAdapter,
        rowSignature,
        new LinkedHashMap<>(),
        new LinkedHashSet<>(rowSignature.getColumnNames()),
        0,
        rows.size()
    );
  }

  private ArrayListRowsAndColumns(
      ArrayList<RowType> rows,
      RowAdapter<RowType> rowAdapter,
      RowSignature rowSignature,
      Map<String, Column> extraColumns,
      Set<String> columnNames,
      int startOffset,
      int endOffset
  )
  {
    if (endOffset - startOffset < 0) {
      throw new ISE("endOffset[%,d] - startOffset[%,d] was somehow negative!?", endOffset, startOffset);
    }
    this.rows = rows;
    this.rowAdapter = rowAdapter;
    this.rowSignature = rowSignature;
    this.extraColumns = extraColumns;
    this.columnNames = columnNames;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public int numRows()
  {
    return endOffset - startOffset;
  }

  @Override
  @Nullable
  public Column findColumn(String name)
  {
    if (!rowSignature.contains(name)) {
      final Column retVal = extraColumns.get(name);
      if (numRows() == rows.size()) {
        return retVal;
      }
      return new LimitedColumn(retVal, startOffset, endOffset);
    }

    final Function<RowType, Object> adapterForValue = rowAdapter.columnFunction(name);
    final Optional<ColumnType> maybeColumnType = rowSignature.getColumnType(name);
    final ColumnType columnType = maybeColumnType.orElse(ColumnType.UNKNOWN_COMPLEX);
    final Comparator<Object> comparator = Comparator.nullsFirst(columnType.getStrategy());

    return new Column()
    {
      @Nonnull
      @Override
      public ColumnAccessor toAccessor()
      {
        return new ObjectColumnAccessorBase()
        {
          @Override
          protected Object getVal(int rowNum)
          {
            return adapterForValue.apply(rows.get(startOffset + rowNum));
          }

          @Override
          protected Comparator<Object> getComparator()
          {
            return comparator;
          }

          @Override
          public ColumnType getType()
          {
            return columnType;
          }

          @Override
          public int numRows()
          {
            return endOffset - startOffset;
          }
        };
      }

      @Nullable
      @Override
      public <T> T as(Class<? extends T> clazz)
      {
        return null;
      }
    };
  }

  @Nullable
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> T as(Class<T> clazz)
  {
    final Function<ArrayListRowsAndColumns, ?> fn = AS_MAP.get(clazz);
    if (fn == null) {
      return null;
    }
    return (T) fn.apply(this);
  }


  @Override
  public void addColumn(String name, Column column)
  {
    if (rows.size() == numRows()) {
      extraColumns.put(name, column);
      columnNames.add(name);
      return;
    }

    // When an ArrayListRowsAndColumns is only a partial view, but adds a column, it believes that the same column
    // will eventually be added for all of the rows so we pre-allocate storage for the entire set of data and
    // copy.

    final ColumnAccessor columnAccessor = column.toAccessor();
    if (columnAccessor.numRows() != numRows()) {
      throw new ISE("More rows[%,d] than expected[%,d]", columnAccessor.numRows(), numRows());
    }

    final Column extraColumn = extraColumns.get(name);
    final ObjectArrayColumn existingColumn;
    if (extraColumn == null) {
      existingColumn = new ObjectArrayColumn(new Object[rows.size()], columnAccessor.getType());
      extraColumns.put(name, existingColumn);
      columnNames.add(name);
    } else if (extraColumn instanceof ObjectArrayColumn) {
      existingColumn = (ObjectArrayColumn) extraColumn;
    } else {
      throw new ISE(
          "Partial column[%s] was added, but already have full column[%s]",
          column.getClass(),
          extraColumn.getClass()
      );
    }

    VectorCopier copier = column.as(VectorCopier.class);
    if (copier == null) {
      copier = new DefaultVectorCopier(columnAccessor);
    }

    copier.copyInto(existingColumn.getObjects(), startOffset);
  }

  private ArrayListRowsAndColumns<RowType> limited(int startOffset, int endOffset)
  {
    return new ArrayListRowsAndColumns<>(
        rows,
        rowAdapter,
        rowSignature,
        extraColumns,
        columnNames,
        startOffset,
        endOffset
    );
  }

  public void sort(ArrayList<ColumnWithDirection> ordering)
  {
    ArrayList<IntComparator> comparators = new ArrayList<>(ordering.size());
    for (ColumnWithDirection columnWithDirection : ordering) {
      final Column column = findColumn(columnWithDirection.getColumn());
      if (column != null) {
        comparators.add(new IntComparator()
        {
          final ColumnAccessor accessor = column.toAccessor();
          private final int directionInt = columnWithDirection.getDirection().getDirectionInt();

          @Override
          public int compare(int lhs, int rhs)
          {
            return accessor.compareRows(lhs, rhs) * directionInt;
          }
        });
      }
    }

    ArrayList<ColumnValueSwapper> swappers = new ArrayList<>(extraColumns.size());
    for (Map.Entry<String, Column> entry : extraColumns.entrySet()) {
      final Column column = entry.getValue();
      final ColumnValueSwapper swapper = column.as(ColumnValueSwapper.class);
      if (swapper == null) {
        throw new ISE("Column[%s] of type[%s] cannot be sorted.", entry.getKey(), column.getClass());
      }
      swappers.add(swapper);
    }

    // Use stable sort, so peer rows retain original order.
    Arrays.mergeSort(
        0,
        rows.size(),
        (lhs, rhs) -> {
          for (IntComparator comparator : comparators) {
            final int retVal = comparator.compare(lhs, rhs);
            if (retVal != 0) {
              return retVal;
            }
          }
          return 0;
        },
        (lhs, rhs) -> {
          RowType tmp = rows.get(lhs);
          rows.set(lhs, rows.get(rhs));
          rows.set(rhs, tmp);

          for (ColumnValueSwapper swapper : swappers) {
            swapper.swapValues(lhs, rhs);
          }
        }
    );
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public ClusteredGroupPartitioner toClusteredGroupPartitioner()
  {
    return new MyClusteredGroupPartitioner();
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public NaiveSortMaker toNaiveSortMaker()
  {
    if (startOffset != 0) {
      throw new ISE(
          "The NaiveSortMaker should happen on the first RAC, start was [%,d], end was [%,d]",
          startOffset,
          endOffset
      );
    }
    if (endOffset == rows.size()) {
      // In this case, we are being sorted along with other RowsAndColumns objects, we don't have an optimized
      // implementation for that, so just return null
      //noinspection ReturnOfNull
      return null;
    }

    // When we are doing a naive sort and we are dealing with the first sub-window from ourselves, then we assume
    // that we will see all of the other sub-windows as well, we can run through them and then sort the underlying
    // rows at the very end.
    return new MyNaiveSortMaker();
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public StorageAdapter toStorageAdapter()
  {
    return new RowBasedStorageAdapter<RowType>(Sequences.simple(rows), rowAdapter, rowSignature);
  }

  private class MyClusteredGroupPartitioner implements ClusteredGroupPartitioner
  {
    @Override
    public int[] computeBoundaries(List<String> columns)
    {
      if (numRows() == 0) {
        return new int[]{};
      }

      boolean allInSignature = true;
      for (String column : columns) {
        if (!rowSignature.contains(column)) {
          allInSignature = false;
        }
      }

      if (allInSignature) {
        return computeBoundariesAllInSignature(columns);
      } else {
        return computeBoundariesSomeAppended(columns);
      }
    }

    /**
     * Computes boundaries assuming all columns are defined as in the signature.  Given that
     * ArrayListRowsAndColumns is a fundamentally row-oriented data structure, using a row-oriented
     * algorithm should prove better than the column-oriented implementation in DefaultClusteredGroupPartitioner
     *
     * @param columns the columns to partition on as in {@link #computeBoundaries(List)}
     * @return the partition boundaries as in {@link #computeBoundaries(List)}
     */
    private int[] computeBoundariesAllInSignature(List<String> columns)
    {
      ArrayList<Comparator<Object>> comparators = new ArrayList<>(columns.size());
      ArrayList<Function<RowType, Object>> adapters = new ArrayList<>(columns.size());
      for (String column : columns) {
        final Optional<ColumnType> columnType = rowSignature.getColumnType(column);
        if (columnType.isPresent()) {
          comparators.add(Comparator.nullsFirst(columnType.get().getStrategy()));
          adapters.add(rowAdapter.columnFunction(column));
        } else {
          throw new ISE("column didn't exist!?  Other method should've been called...");
        }
      }

      IntList boundaries = new IntArrayList();
      Object[] prevVal = new Object[comparators.size()];
      Object[] nextVal = new Object[comparators.size()];

      int numRows = endOffset - startOffset;

      boundaries.add(0);
      RowType currRow = rows.get(startOffset);
      for (int i = 0; i < adapters.size(); ++i) {
        prevVal[i] = adapters.get(i).apply(currRow);
      }

      for (int i = 1; i < numRows; ++i) {
        currRow = rows.get(startOffset + i);
        for (int j = 0; j < adapters.size(); ++j) {
          nextVal[j] = adapters.get(j).apply(currRow);
        }

        for (int j = 0; j < comparators.size(); ++j) {
          final int comparison = comparators.get(j).compare(prevVal[j], nextVal[j]);
          if (comparison != 0) {
            // Swap references
            Object[] tmpRef = prevVal;
            prevVal = nextVal;
            nextVal = tmpRef;

            boundaries.add(i);
            break;
          }
        }
      }
      boundaries.add(numRows);

      return boundaries.toIntArray();
    }

    /**
     * Computes boundaries including some columns that were appended later.  In this case, we are fundamentally
     * mixing some row-oriented format with some column-oriented format. It's hard to determine if there's really
     * an optimized form for this (or, really, it's hard to know if the optimized form would actually be worth
     * the code complexity), so just fall back to the DefaultClusteredGroupPartitioner to compute these boundaries,
     * the optimizations that come from re-using the ArrayListRowsAndColumns will continue to exist.
     *
     * @param columns the columns to partition on as in {@link #computeBoundaries(List)}
     * @return the partition boundaries as in {@link #computeBoundaries(List)}
     */
    private int[] computeBoundariesSomeAppended(List<String> columns)
    {
      return new DefaultClusteredGroupPartitioner(ArrayListRowsAndColumns.this).computeBoundaries(columns);
    }


    @Override
    public ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns)
    {
      final int[] boundaries = computeBoundaries(partitionColumns);
      if (boundaries.length < 2) {
        return new ArrayList<>();
      }

      ArrayList<RowsAndColumns> retVal = new ArrayList<>(boundaries.length - 1);

      for (int i = 1; i < boundaries.length; ++i) {
        int start = boundaries[i - 1];
        int end = boundaries[i];
        retVal.add(limited(start, end));
      }

      return retVal;
    }
  }

  private class MyNaiveSortMaker implements NaiveSortMaker
  {

    @Override
    public NaiveSorter make(ArrayList<ColumnWithDirection> ordering)
    {
      return new NaiveSorter()
      {
        private int currEnd = endOffset;

        @SuppressWarnings("unchecked")
        @Override
        public RowsAndColumns moreData(RowsAndColumns rac)
        {
          if (currEnd == rows.size()) {
            // It's theoretically possible that this object is used in a place where it sees a bunch of parts from
            // the same ArrayListRowsAndColumns and then, continues to receive more and more RowsAndColumns objects
            // from other ArrayListRowsAndColumns.  In that case, we can
            //   1. do a localized sort
            //   2. continue to build up the other objects
            //   3. once all objects are complete, do a merge-sort between them and return that
            //
            // This is not currently implemented, however, as the code cannot actually ever generate that sequence
            // of objects.  Additionally, if the code ever did generate that sequence, the proper solution could be
            // to implement something else differently (perhaps use a different type of RowsAndColumns entirely).
            // As such, we leave this implementation as an exercise for the future when it is better known why the
            // code needed to work with this specific series of concrete objects.
            throw new ISE("More data came after completing the ArrayList, not supported yet.");
          }

          if (rac instanceof ArrayListRowsAndColumns) {
            ArrayListRowsAndColumns<RowType> arrayRac = (ArrayListRowsAndColumns<RowType>) rac;
            if (arrayRac.startOffset != currEnd) {
              throw new ISE(
                  "ArrayRAC instances seen out-of-order!? currEnd[%,d], arrayRac[%,d][%,d]",
                  currEnd,
                  arrayRac.startOffset,
                  arrayRac.endOffset
              );
            }
            currEnd = arrayRac.endOffset;

            return null;
          } else {
            throw new ISE("Expected an ArrayListRowsAndColumns, got[%s], fall back to default?", rac.getClass());
          }
        }

        @Override
        public RowsAndColumns complete()
        {
          if (currEnd != rows.size()) {
            throw new ISE("Didn't see all of the rows? currEnd[%,d], rows.size()[%,d]", currEnd, rows.size());
          }

          final ArrayListRowsAndColumns<RowType> retVal = limited(0, rows.size());
          retVal.sort(ordering);
          return retVal;
        }
      };
    }
  }
}
