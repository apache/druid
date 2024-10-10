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

package org.apache.druid.query.operator;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LimitedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This glueing partitioning operator is supposed to continuously receive data, and output batches of partitioned RACs.
 * It maintains a last-partitioning-boundary of the last-pushed-RAC, and attempts to glue it with the next RAC it receives,
 * ensuring that partitions are handled correctly, even across multiple RACs.
 * <p>
 * Additionally, this assumes that data has been pre-sorted according to the partitioning columns.
 */
public class GlueingPartitioningOperator extends AbstractPartitioningOperator
{
  private final int maxRowsMaterialized;
  private final AtomicReference<RowsAndColumns> previousRacRef = new AtomicReference<>(null);

  private static final Integer MAX_ROWS_MATERIALIZED_NO_LIMIT = Integer.MAX_VALUE;

  public GlueingPartitioningOperator(
      Operator child,
      List<String> partitionColumns
  )
  {
    this(child, partitionColumns, MAX_ROWS_MATERIALIZED_NO_LIMIT);
  }

  public GlueingPartitioningOperator(
      Operator child,
      List<String> partitionColumns,
      Integer maxRowsMaterialized
  )
  {
    super(partitionColumns, child);
    this.maxRowsMaterialized = maxRowsMaterialized;
  }

  @Override
  protected HandleContinuationResult handleContinuation(Receiver receiver, Continuation cont)
  {
    while (cont.iter.hasNext()) {
      RowsAndColumns next = cont.iter.next();

      if (!cont.iter.hasNext()) {
        // We are at the last RAC. Process it only if subContinuation is null, otherwise save it in previousRac.
        if (cont.subContinuation == null) {
          receiver.push(next);
          receiver.completed();
          return HandleContinuationResult.of(null);
        } else {
          previousRacRef.set(next);
          break;
        }
      }

      final Signal signal = receiver.push(next);
      if (signal != Signal.GO) {
        return handleNonGoCases(signal, cont.iter, receiver, cont);
      }
    }
    return HandleContinuationResult.CONTINUE_PROCESSING;
  }

  private static class StaticReceiver implements Receiver
  {
    private final Receiver delegate;
    private final AtomicReference<Iterator<RowsAndColumns>> iterHolder;
    private final AtomicReference<RowsAndColumns> previousRacRef;
    private final int maxRowsMaterialized;
    private final List<String> partitionColumns;

    public StaticReceiver(
        Receiver delegate,
        AtomicReference<Iterator<RowsAndColumns>> iterHolder,
        AtomicReference<RowsAndColumns> previousRacRef,
        List<String> partitionColumns,
        int maxRowsMaterialized
    )
    {
      this.delegate = delegate;
      this.iterHolder = iterHolder;
      this.previousRacRef = previousRacRef;
      this.partitionColumns = partitionColumns;
      this.maxRowsMaterialized = maxRowsMaterialized;
    }

    @Override
    public Signal push(RowsAndColumns rac)
    {
      ensureMaxRowsMaterializedConstraint(rac.numRows(), maxRowsMaterialized);
      if (rac == null) {
        throw DruidException.defensive("Should never get a null rac here.");
      }

      Iterator<RowsAndColumns> partitionsIter = getIteratorForRAC(rac, previousRacRef, partitionColumns, maxRowsMaterialized);

      AtomicReference<Signal> keepItGoing = new AtomicReference<>(Signal.GO);
      while (keepItGoing.get() == Signal.GO && partitionsIter.hasNext()) {
        handleKeepItGoing(keepItGoing, partitionsIter, delegate, previousRacRef);
      }

      if (keepItGoing.get() == Signal.PAUSE && partitionsIter.hasNext()) {
        iterHolder.set(partitionsIter);
        return Signal.PAUSE;
      }

      return keepItGoing.get();
    }

    @Override
    public void completed()
    {
      if (previousRacRef.get() != null) {
        delegate.push(previousRacRef.get());
        previousRacRef.set(null);
      }
      if (iterHolder.get() == null) {
        delegate.completed();
      }
    }
  }

  /**
   * Iterator implementation for gluing partitioned RowsAndColumns.
   * It handles the boundaries of partitions within a single RAC and potentially glues
   * the first partition of the current RAC with the previous RAC if needed.
   */
  private static class GluedRACsIterator implements Iterator<RowsAndColumns>
  {
    private final RowsAndColumns rac;
    private final int[] boundaries;
    private int currentIndex = 0;
    private boolean firstPartitionHandled = false;
    private final AtomicReference<RowsAndColumns> previousRacRef;
    private final int maxRowsMaterialized;
    private final List<String> partitionColumns;

    public GluedRACsIterator(RowsAndColumns rac, AtomicReference<RowsAndColumns> previousRacRef, List<String> partitionColumns, int maxRowsMaterialized)
    {
      this.rac = rac;
      ClusteredGroupPartitioner groupPartitioner = rac.as(ClusteredGroupPartitioner.class);
      if (groupPartitioner == null) {
        groupPartitioner = new DefaultClusteredGroupPartitioner(rac);
      }
      this.boundaries = groupPartitioner.computeBoundaries(partitionColumns);
      this.previousRacRef = previousRacRef;
      this.partitionColumns = partitionColumns;
      this.maxRowsMaterialized = maxRowsMaterialized;
    }

    @Override
    public boolean hasNext()
    {
      return currentIndex < boundaries.length - 1;
    }

    /**
     * Retrieves the next partition in the RowsAndColumns. If the first partition has not been handled yet,
     * it may be glued with the previous RowsAndColumns if the partition columns match.
     *
     * @return The next RowsAndColumns partition, potentially glued with the previous one.
     * @throws NoSuchElementException if there are no more partitions.
     */
    @Override
    public RowsAndColumns next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (!firstPartitionHandled) {
        firstPartitionHandled = true;
        int start = boundaries[currentIndex];
        int end = boundaries[currentIndex + 1];
        LimitedRowsAndColumns limitedRAC = new LimitedRowsAndColumns(rac, start, end);

        final ConcatRowsAndColumns concatRacForFirstPartition = getConcatRacForFirstPartition(previousRacRef.get(), limitedRAC);
        if (previousRacRef.get() != null && isGlueingNeeded(concatRacForFirstPartition, previousRacRef.get())) {
          ensureMaxRowsMaterializedConstraint(concatRacForFirstPartition.numRows(), maxRowsMaterialized);
          previousRacRef.set(null);
          currentIndex++;
          return concatRacForFirstPartition;
        } else {
          if (previousRacRef.get() != null) {
            RowsAndColumns temp = previousRacRef.get();
            previousRacRef.set(null);
            return temp;
          }
        }
      }

      int start = boundaries[currentIndex];
      int end = boundaries[currentIndex + 1];
      currentIndex++;
      return new LimitedRowsAndColumns(rac, start, end);
    }

    /**
     * Determines whether glueing is needed between 2 RACs represented as a ConcatRowsAndColumns, by comparing a row belonging to each RAC.
     * The rows of different RACs are expected to be present at index1 and index2 respectively in the ConcatRAC. If the columns match, we
     * can glue the 2 RACs and use the ConcatRAC.
     * @param rac A {@link ConcatRowsAndColumns containing 2 RACs}
     * @param firstRac The 1st of two RACs present in the Concat RAC
     * @return true if gluing is needed, false otherwise.
     */
    private boolean isGlueingNeeded(ConcatRowsAndColumns rac, RowsAndColumns firstRac)
    {
      if (firstRac == null) {
        return false;
      }

      for (String column : partitionColumns) {
        final Column theCol = rac.findColumn(column);
        if (theCol == null) {
          throw new ISE("Partition column [%s] not found in RAC.", column);
        }
        final ColumnAccessor accessor = theCol.toAccessor();
        int comparison = accessor.compareRows(0, firstRac.numRows());
        if (comparison != 0) {
          return false;
        }
      }
      return true;
    }

    private ConcatRowsAndColumns getConcatRacForFirstPartition(RowsAndColumns previousRac, RowsAndColumns firstPartitionOfCurrentRac)
    {
      if (previousRac == null) {
        return new ConcatRowsAndColumns(new ArrayList<>(Collections.singletonList(firstPartitionOfCurrentRac)));
      }
      return new ConcatRowsAndColumns(new ArrayList<>(Arrays.asList(previousRac, firstPartitionOfCurrentRac)));
    }
  }

  private static void ensureMaxRowsMaterializedConstraint(int numRows, int maxRowsMaterialized)
  {
    if (numRows > maxRowsMaterialized) {
      throw InvalidInput.exception(
          "Too many rows to process (requested = %d, max = %d).",
          numRows,
          maxRowsMaterialized
      );
    }
  }

  protected static Iterator<RowsAndColumns> getIteratorForRAC(
      RowsAndColumns rac,
      AtomicReference<RowsAndColumns> previousRacRef,
      List<String> partitionColumns,
      int maxRowsMaterialized
  )
  {
    return new GluedRACsIterator(rac, previousRacRef, partitionColumns, maxRowsMaterialized);
  }

  protected static void handleKeepItGoing(
      AtomicReference<Signal> signalRef,
      Iterator<RowsAndColumns> iterator,
      Receiver receiver,
      AtomicReference<RowsAndColumns> previousRacRef
  )
  {
    RowsAndColumns rowsAndColumns = iterator.next();
    if (iterator.hasNext()) {
      signalRef.set(receiver.push(rowsAndColumns));
    } else {
      // If it's the last element, save it in previousRac instead of pushing to receiver.
      previousRacRef.set(rowsAndColumns);
    }
  }

  @Override
  protected Receiver createReceiver(Receiver delegate, AtomicReference<Iterator<RowsAndColumns>> iterHolder)
  {
    return new StaticReceiver(delegate, iterHolder, previousRacRef, partitionColumns, maxRowsMaterialized);
  }
}
