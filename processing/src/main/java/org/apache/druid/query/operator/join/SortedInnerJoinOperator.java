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

package org.apache.druid.query.operator.join;

import org.apache.druid.collections.fastutil.DruidIntList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RearrangedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.SortedMatrixMaker;
import org.apache.druid.query.rowsandcols.util.FindResult;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An operator that can join the data streams from other operators.  Data must be provided in the same sorted
 * order between the different operators.
 * <p>
 * Performs an InnerJoin based on the equality of two sets of fields.  Null is considered a meaningful value
 * when comparing the two streams.  If null is intended to be excluded, it should be removed through a filter.
 * <p>
 * This class was created more as an exercise in ensuring that something meaningful can be made to do combinations
 * of Operators (and ensure the interface is correct).  There are tests that show that this class works, but those
 * tests are not (yet) considered exhaustive.  This paragraph in the comments should exist as a cautionary indication
 * that if/when this class is dusted off for use again, there might be bugs yet lurking and it should likely start
 * with fleshing out the tests.
 */
public class SortedInnerJoinOperator implements Operator
{
  private static final Logger log = new Logger(SortedInnerJoinOperator.class);

  private final ArrayList<JoinPart> parts;
  private final JoinConfig config;

  public SortedInnerJoinOperator(
      List<JoinPartDefn> partDefns,
      JoinConfig config
  )
  {
    this.parts = new ArrayList<>(partDefns.size());
    for (JoinPartDefn partDefn : partDefns) {
      parts.add(new JoinPart(partDefn.getOp(), partDefn.getJoinFields(), partDefn.getProjectFields()));
    }

    this.config = config;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    JoinLogic joinLogic;
    if (continuation == null) {
      joinLogic = new JoinLogic(config, parts);
    } else {
      joinLogic = (JoinLogic) continuation;
    }

    try {
      joinLogic.go(receiver);
      switch (joinLogic.state) {
        case NEEDS_DATA:
        case READY:
          throw new ISE("joinLogic.go() exited with state[%s], should never happen.", joinLogic.state);
        case COMPLETE:
          return null;
        case PAUSED:
          return joinLogic;
        default:
          throw new ISE("Unknown state[%s]", joinLogic.state);
      }
    }
    catch (RuntimeException e) {
      try {
        joinLogic.close();
      }
      catch (Exception ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }
  }

  private static class JoinLogic implements Closeable
  {
    private final JoinConfig config;

    private final ArrayList<JoinPart> joinParts;

    private State state;
    private int nextPositionToLoad;

    private JoinLogic(
        JoinConfig config,
        ArrayList<JoinPart> joinParts
    )
    {
      this.config = config;
      this.joinParts = joinParts;

      setNextPositionToLoad(joinParts.size() - 1);
    }

    public void go(Receiver receiver)
    {
      while (state != State.COMPLETE) {
        final int position = nextPositionToLoad;
        final JoinPart joinPart = joinParts.get(position);
        //noinspection VariableNotUsedInsideIf
        if (joinPart.curr != null) {
          throw new ISE("loading data for position[%d], but it already had data!?  Probably a bug!", position);
        }

        joinPart.goOrContinue(new Receiver()
        {
          @Override
          public Signal push(RowsAndColumns rac)
          {
            joinPart.setCurr(rac);
            process(receiver);

            switch (state) {
              case COMPLETE:
                return Signal.STOP;
              case NEEDS_DATA:
                return Signal.GO;
              case PAUSED:
                return Signal.PAUSE;
              case READY:
                throw new ISE("Was in state READY after process returned!?");
              default:
                throw new ISE("Unknown state[%s]", state);
            }
          }

          @Override
          public void completed()
          {
            joinPart.complete.set(true);
          }
        });

        if (joinPart.continuation == null && joinPart.needsData() && joinPart.isComplete()) {
          // In this case, (1) the op returned null, (2) we don't have anything buffered to process and (3) completed
          // was called.  We are done.
          state = State.COMPLETE;
        }

        switch (state) {
          case READY:
            throw new ISE("Don't expect READY state here, process() should've changed it to something else");
          case PAUSED:
            return;
          case COMPLETE:
            receiver.completed();

            try {
              close();
            }
            catch (IOException e) {
              log.warn("Problem closing a join part[%d], ignoring because we are done anyway.", position);
            }
            break;
          case NEEDS_DATA:
            break;
          default:
            throw new ISE("Unknown state[%s]", state);
        }
      }
    }

    /**
     * Processes whatever it can from the buffers, pushes the created RowsAndColumns to the receiver and
     * returns which side we need more data from.
     * <p>
     * updates {@link #nextPositionToLoad} to a positive number if there is another position to load.
     * sets it to -1 if processing is complete.
     */
    private void process(Receiver receiver)
    {
      // First check that we have something to work with for all parts of the join
      for (int i = joinParts.size() - 1; i >= 0; --i) {
        final JoinPart joinPart = joinParts.get(i);
        if (joinPart.needsData()) {
          if (joinPart.isComplete()) {
            state = State.COMPLETE;
          } else {
            setNextPositionToLoad(i);
          }
          return;
        }
      }
      state = State.READY;

      DruidIntList[] rowsToInclude = new DruidIntList[joinParts.size()];
      for (int i = 0; i < rowsToInclude.length; ++i) {
        rowsToInclude[i] = new DruidIntList(config.getBufferSize());
        if (joinParts.get(i).needsData()) {
          throw new ISE("doJoin called while joinPart[%d] needed data.  This is likely a bug", i);
        }
      }

      final int finalIndex = joinParts.size() - 1;
      final JoinPart finalPart = joinParts.get(finalIndex);
      SortedMatrixMaker.SortedMatrix.MatrixRow row = null;
      while (state == State.READY) {
        if (row == null) {
          row = finalPart.currMatrix.getRow(finalPart.currRowIndex);
        }

        row = joinRows(receiver, finalIndex, rowsToInclude, row);
      }

      pushRows(receiver, rowsToInclude);
    }

    private void pushRows(Receiver receiver, DruidIntList[] rowsToInclude)
    {
      int size = rowsToInclude[0].size();
      if (size == 0) {
        return;
      }

      LinkedHashMap<String, Column> cols = new LinkedHashMap<>();

      for (int i = 0; i < joinParts.size(); ++i) {
        final JoinPart part = joinParts.get(i);
        RowsAndColumns remapped =
            new RearrangedRowsAndColumns(rowsToInclude[i].elements(), 0, rowsToInclude[i].size(), part.curr);

        for (String field : part.projectFields) {
          cols.put(field, remapped.findColumn(field));
        }
      }

      final Signal signal = receiver.push(new MapOfColumnsRowsAndColumns(cols, size));
      switch (signal) {
        case STOP:
          state = State.COMPLETE;
          break;
        case PAUSE:
          state = State.PAUSED;
          break;
        case GO:
          break;
        default:
          throw new ISE("Unknown state[%s]", signal);
      }
    }

    /**
     * Joins rows by recursively walking the joinParts backwards.
     * <p>
     * Populates rowsToInclude such it represents the row mapping between the different parts of the join.  That is
     * if there are 2 sides to the inner join and there are only 2 matches:
     * 1) row 50 on part 0, and row 8 on part 1
     * 2) row 8372 on part 0, and row 9 on part 1
     * <p>
     * Then rowsToInclude would be the equivalent of {@code new int[][]{{50, 8372}, {8, 9}}}
     * <p>
     * If a specific part cannot match the currently provided row, then it will seek to the next possible row that it
     * *could* match and return that as an alternative option.  The previous callers can then try to find that
     * alternative row until either one of the parts is exhausted of data OR a match is found.
     * <p>
     * This method returns null when there is no more work that can be done on the current row.  This could be because
     * a match was found and results generated, or it could be because one of the parts was exhausted and needs more.
     *
     * @param joinPartIndex the index of the join part that the current call should be looking at
     * @param rowsToInclude a mapping of rows that should be included in results
     * @param row           the current candidate join value to match
     * @return null if no more work can be done, non-null if there is an alternative row that a part wants to search for
     */
    @Nullable
    private SortedMatrixMaker.SortedMatrix.MatrixRow joinRows(
        Receiver receiver,
        int joinPartIndex,
        DruidIntList[] rowsToInclude,
        SortedMatrixMaker.SortedMatrix.MatrixRow row
    )
    {
      final JoinPart joinPart = joinParts.get(joinPartIndex);

      final FindResult findResult = joinPart.currMatrix.findRow(
          joinPart.currRowIndex,
          row
      );

      if (findResult.wasFound()) {
        joinPart.currRowIndex = findResult.getStartRow();
        joinPart.scanToRowIndex = findResult.getEndRow();

        if (joinPartIndex == 0) {
          // We have walked through all of the joinParts and have matches, so time to add to rowsToInclude

          // We have ranges in each of the parts, we will do the cartesian product, so we will produce the product
          // of the length of those ranges number of rows.  Let's compute it to see how many rows we will produce.
          int numRowsExpected = joinPart.scanToRowIndex - joinPart.currRowIndex;
          for (int i = 1; i < joinParts.size(); ++i) {
            final JoinPart subPart = joinParts.get(i);
            numRowsExpected *= subPart.scanToRowIndex - subPart.currRowIndex;
          }

          if (numRowsExpected == 1) {
            for (int i = 0; i < joinParts.size(); ++i) {
              rowsToInclude[i].add(joinParts.get(i).currRowIndex);
            }
          } else {
            if (numRowsExpected > 1_000_000) {
              // It would be helpful to serialize the actual value out with this error, but that risks leaking data
              throw new IAE("Got a join, with a cartesian product that exceeds 1,000,000 rows, cannot handle it");
            }

            // The rowIds that will be used in the result of the join will be a cartesian product, which means that
            // the "deepest" row ids will be repeated in-order over and over, then the next layer will have each
            // value repeated runSize times, forming a new run of length * runSize, and so on and so forth
            int partIndex = joinParts.size() - 1;
            int runSize = 1;
            // We are guaranteed that there is a runSize greater than 1 because otherwise numRowsExpected would be 1
            while (partIndex >= 0) {
              final JoinPart part = joinParts.get(partIndex);
              final int size = part.scanToRowIndex - part.currRowIndex;
              if (size == 1) {
                rowsToInclude[partIndex].fill(part.currRowIndex, numRowsExpected);
              } else {
                int[] vals = new int[size];
                for (int i = 0; i < vals.length; ++i) {
                  vals[i] = i + part.currRowIndex;
                }
                final int newRunSize = size * runSize;
                rowsToInclude[partIndex].fillRuns(vals, runSize, numRowsExpected / newRunSize);
                runSize = newRunSize;
              }

              --partIndex;
            }
          }

          if (rowsToInclude[0].size() > config.getReleaseSize()) {
            // Incrementally push stuff out once we've collected this number of rows
            pushRows(receiver, rowsToInclude);
            if (state == State.READY) {
              // We have more to do, so reinitialize rowsToInclude
              for (DruidIntList intList : rowsToInclude) {
                intList.resetToSize(config.getBufferSize());
              }
            }
          }

          return null;
        } else {
          // Continue the march through the joinParts
          final SortedMatrixMaker.SortedMatrix.MatrixRow alternativeRow =
              joinRows(receiver, joinPartIndex - 1, rowsToInclude, row);

          if (consumeCurrMaybePush(receiver, joinPartIndex, rowsToInclude, joinPart)) {
            return null;
          }

          if (alternativeRow == null) {
            return null;
          } else {
            // The next part didn't have the row we wanted, so let's check for the candidate that it returned
            final FindResult alternativeFind =
                joinPart.currMatrix.findRow(joinPart.currRowIndex, alternativeRow);

            if (alternativeFind == null) {
              joinPart.reinitCurr();
              setNextPositionToLoad(joinPartIndex);
              return null;
            } else if (alternativeFind.wasFound()) {
              // We update our values to make finding it again easier
              joinPart.currRowIndex = alternativeFind.getStartRow();
              joinPart.scanToRowIndex = alternativeFind.getEndRow();
              return alternativeRow;
            } else {
              if (consumeCurrMaybePush(receiver, joinPartIndex, rowsToInclude, joinPart)) {
                return null;
              } else {
                return joinPart.currMatrix.getRow(joinPart.currRowIndex);
              }
            }
          }
        }
      } else {
        final int next = findResult.getNext();
        if (next >= joinPart.currMatrix.numRows()) {
          // the next item is beyond the current RAC, so we need to load more data.
          joinPart.reinitCurr();
          setNextPositionToLoad(joinPartIndex);
          return null;
        }
        // No match, so return the next possible match such that the caller can seek to that.
        return joinPart.currMatrix.getRow(next);
      }
    }

    private boolean consumeCurrMaybePush(
        Receiver receiver,
        int joinPartIndex,
        DruidIntList[] rowsToInclude,
        JoinPart joinPart
    )
    {
      // Consume the current range of data by jumping to the end
      joinPart.jumpTo(joinPart.scanToRowIndex);
      if (joinPart.needsData()) {
        // If we need more data, we must first push out anything we've built up so far
        // before we drop the reference to the RAC
        pushRows(receiver, rowsToInclude);
        for (DruidIntList intList : rowsToInclude) {
          intList.clear();
        }
        // We ran out of data, so we need to just mark that we need data and return
        if (joinPart.isComplete()) {
          state = State.COMPLETE;
          joinPart.reinitCurr();
        } else {
          setNextPositionToLoad(joinPartIndex);
        }
        return true;
      }
      return false;
    }

    private void setNextPositionToLoad(int joinPartIndex)
    {
      state = State.NEEDS_DATA;
      nextPositionToLoad = joinPartIndex;
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      closer.registerAll(joinParts);
      closer.close();
    }
  }

  private enum State
  {
    NEEDS_DATA,
    COMPLETE,
    PAUSED,
    READY
  }

  private static class JoinPart implements Closeable
  {
    private final Operator op;
    private final List<String> joinFields;
    private final List<String> projectFields;
    private final AtomicBoolean complete;

    private RowsAndColumns curr;
    private SortedMatrixMaker.SortedMatrix currMatrix;
    private int currRowIndex;
    private int scanToRowIndex;

    private Closeable continuation;

    private JoinPart(
        Operator op,
        List<String> joinFields,
        List<String> projectFields
    )
    {
      this.op = op;
      this.joinFields = joinFields;
      this.projectFields = projectFields;

      complete = new AtomicBoolean(false);
      reinitCurr();
      continuation = null;
    }

    public void setCurr(RowsAndColumns rac)
    {
      //noinspection VariableNotUsedInsideIf
      if (curr != null) {
        throw new ISE("Asked to setCurr even though it was not null!?");
      }

      curr = rac;
      currMatrix = SortedMatrixMaker.fromRAC(rac).make(joinFields);
      jumpTo(0);
    }

    public boolean needsData()
    {
      return curr == null || currRowIndex >= currMatrix.numRows();
    }

    public boolean isComplete()
    {
      return complete.get();
    }

    public void goOrContinue(Receiver receiver)
    {
      Closeable theContinuation = continuation;
      continuation = null;
      continuation = op.goOrContinue(theContinuation, receiver);
    }

    @Override
    public void close() throws IOException
    {
      if (continuation != null) {
        continuation.close();
      }
    }

    /**
     * @param rowIndex the rowIndex to jump to
     */
    public void jumpTo(int rowIndex)
    {
      currRowIndex = rowIndex;
      scanToRowIndex = -1;
    }

    public void reinitCurr()
    {
      curr = null;
      currMatrix = null;
      currRowIndex = -1;
      scanToRowIndex = -1;
    }
  }
}
