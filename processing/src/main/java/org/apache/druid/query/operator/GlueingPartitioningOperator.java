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
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LimitedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * todo: write detailed javadoc for the class, all the methods, etc.
 */
public class GlueingPartitioningOperator implements Operator
{
  private final List<String> partitionColumns;
  private final Operator child;
  private RowsAndColumns previousRac;

  public GlueingPartitioningOperator(
      List<String> partitionColumns,
      Operator child
  )
  {
    this.partitionColumns = partitionColumns;
    this.child = child;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    if (continuation != null) {
      Continuation cont = (Continuation) continuation;

      if (cont.iter != null) {
        while (cont.iter.hasNext()) {
          RowsAndColumns next = cont.iter.next();

          if (!cont.iter.hasNext()) {
            // We are at the last RAC. Process it only if subContinuation is null, otherwise save it in previousRac.
            if (cont.subContinuation == null) {
              receiver.push(next);
              receiver.completed();
              return null;
            } else {
              previousRac = next;
              break;
            }
          }

          final Signal signal = receiver.push(next);
          switch (signal) {
            case GO:
              break;
            case PAUSE:
              if (cont.iter.hasNext()) {
                return cont;
              }

              if (cont.subContinuation == null) {
                // We were finished anyway
                receiver.completed();
                return null;
              }

              return new Continuation(null, cont.subContinuation);
            case STOP:
              receiver.completed();
              try {
                cont.close();
              }
              catch (IOException e) {
                throw new RE(e, "Unable to close continuation");
              }
              return null;
            default:
              throw new RE("Unknown signal[%s]", signal);
          }
        }

        if (cont.subContinuation == null) {
          receiver.completed();
          return null;
        }
      }

      continuation = cont.subContinuation;
    }

    AtomicReference<Iterator<RowsAndColumns>> iterHolder = new AtomicReference<>();

    final Closeable retVal = child.goOrContinue(
        continuation,
        new Receiver()
        {
          @Override
          public Signal push(RowsAndColumns rac)
          {
            if (rac == null) {
              throw DruidException.defensive("Should never get a null rac here.");
            }

            ClusteredGroupPartitioner groupPartitioner = rac.as(ClusteredGroupPartitioner.class);
            if (groupPartitioner == null) {
              groupPartitioner = new DefaultClusteredGroupPartitioner(rac);
            }

            int[] computedBoundaries = groupPartitioner.computeBoundaries(partitionColumns);

            final ArrayList<RowsAndColumns> gluedRACs = getGluedRACs(rac, computedBoundaries);
            Iterator<RowsAndColumns> partitionsIter = gluedRACs.iterator();

            Signal keepItGoing = Signal.GO;
            while (keepItGoing == Signal.GO && partitionsIter.hasNext()) {
              RowsAndColumns rowsAndColumns = partitionsIter.next();
              if (partitionsIter.hasNext()) {
                keepItGoing = receiver.push(rowsAndColumns);
              } else {
                // If it's the last element, save it in previousRac instead of pushing to receiver.
                previousRac = rowsAndColumns;
              }
            }

            if (keepItGoing == Signal.PAUSE && partitionsIter.hasNext()) {
              iterHolder.set(partitionsIter);
              return Signal.PAUSE;
            }

            return keepItGoing;
          }

          @Override
          public void completed()
          {
            if (previousRac != null) {
              receiver.push(previousRac);
              previousRac = null;
            }
            if (iterHolder.get() == null) {
              receiver.completed();
            }
          }
        }
    );

    if (iterHolder.get() != null || retVal != null) {
      return new Continuation(
          iterHolder.get(),
          retVal
      );
    } else {
      return null;
    }
  }

  private static class Continuation implements Closeable
  {
    Iterator<RowsAndColumns> iter;
    Closeable subContinuation;

    public Continuation(Iterator<RowsAndColumns> iter, Closeable subContinuation)
    {
      this.iter = iter;
      this.subContinuation = subContinuation;
    }

    @Override
    public void close() throws IOException
    {
      if (subContinuation != null) {
        subContinuation.close();
      }
    }
  }

  private boolean isGlueingNeeded(RowsAndColumns previousRac, RowsAndColumns firstPartitionOfCurrentRac)
  {
    if (previousRac == null) {
      return false;
    }

    final ConcatRowsAndColumns concatRac = getConcatRacForFirstPartition(previousRac, firstPartitionOfCurrentRac);
    for (String column : partitionColumns) {
      final Column theCol = concatRac.findColumn(column);
      if (theCol == null) {
        // The column doesn't exist.  In this case, we assume it's always the same value: null.  If it's always
        // the same, then it doesn't impact grouping at all and can be entirely skipped.
        continue;
      }
      final ColumnAccessor accessor = theCol.toAccessor();
      int comparison = accessor.compareRows(0, previousRac.numRows()); // Compare 1st row of previousRac and currentRac in [previousRac, currentRac] form.
      if (comparison != 0) {
        return false;
      }
    }
    return true;
  }

  private ArrayList<RowsAndColumns> getGluedRACs(RowsAndColumns rac, int[] boundaries)
  {
    final ArrayList<RowsAndColumns> gluedRACs = new ArrayList<>();
    for (int i = 1; i < boundaries.length; ++i) {
      int start = boundaries[i - 1];
      int end = boundaries[i];
      final LimitedRowsAndColumns limitedRAC = new LimitedRowsAndColumns(rac, start, end);
      if (i == 1) {
        handleFirstPartition(gluedRACs, limitedRAC);
      } else {
        gluedRACs.add(limitedRAC);
      }
    }
    return gluedRACs;
  }

  private void handleFirstPartition(ArrayList<RowsAndColumns> gluedRACs, LimitedRowsAndColumns firstPartitionOfCurrentRac)
  {
    if (isGlueingNeeded(previousRac, firstPartitionOfCurrentRac)) {
      gluedRACs.add(getConcatRacForFirstPartition(previousRac, firstPartitionOfCurrentRac));
    } else {
      if (previousRac != null) {
        gluedRACs.add(previousRac);
      }
      gluedRACs.add(firstPartitionOfCurrentRac);
    }
    previousRac = null;
  }

  private ConcatRowsAndColumns getConcatRacForFirstPartition(RowsAndColumns previousRac, RowsAndColumns firstPartitionOfCurrentRac)
  {
    return new ConcatRowsAndColumns(new ArrayList<>(Arrays.asList(previousRac, firstPartitionOfCurrentRac)));
  }
}
