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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class WindowOperatorQueryFrameProcessor extends BaseLeafFrameProcessor
{

  private static final Logger log = new Logger(WindowOperatorQueryFrameProcessor.class);
  private final WindowOperatorQuery query;

  private final List<OperatorFactory> operatorFactoryList;
  private final ObjectMapper jsonMapper;
  private final Closer closer = Closer.create();
  private final RowSignature outputStageSignature;
  private final ArrayList<RowsAndColumns> frameRowsAndCols;
  private final ArrayList<RowsAndColumns> resultRowAndCols;
  ArrayList<ResultRow> objectsOfASingleRac;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private Cursor frameCursor = null;
  private Supplier<ResultRow> rowSupplierFromFrameCursor;
  private ResultRow outputRow = null;
  private FrameWriter frameWriter = null;

  public WindowOperatorQueryFrameProcessor(
      final WindowOperatorQuery query,
      final List<OperatorFactory> operatorFactoryList,
      final ObjectMapper jsonMapper,
      final ReadableInput baseInput,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      final RowSignature rowSignature
  )
  {
    super(
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
    this.query = query;
    this.jsonMapper = jsonMapper;
    this.operatorFactoryList = operatorFactoryList;
    this.outputStageSignature = rowSignature;
    this.frameRowsAndCols = new ArrayList<>();
    this.resultRowAndCols = new ArrayList<>();
    this.objectsOfASingleRac = new ArrayList<>();
  }

  @Override
  protected ReturnOrAwait<Unit> runWithSegment(SegmentWithDescriptor segment)
  {
    throw new RuntimeException("Window stage can run only on the output of a previous stage");
  }

  @Override
  protected ReturnOrAwait<Unit> runWithLoadedSegment(SegmentWithDescriptor segment)
  {
    throw new RuntimeException("Window stage can run only on the output of a previous stage");
  }

  // previous stage output
  @Override
  protected ReturnOrAwait<Unit> runWithInputChannel(ReadableFrameChannel inputChannel, FrameReader inputFrameReader)
  {
    /**
     *
     * PARTITION BY A ORDER BY B
     *
     * Frame 1   -> rac1
     * A  B
     * 1, 2
     * 1, 3
     * 2, 1 --> key changed
     * 2, 2
     *
     *
     * Frame 2 -> rac2
     * 3, 1 --> key changed
     * 3, 2
     * 3, 3
     * 3, 4
     *
     * Frame 3 -> rac3
     *
     * 3, 5
     * 3, 6
     * 4, 1 --> key changed
     * 4, 2
     *
     * In case of empty OVER clause, all these racs need to be added to a single rows and columns
     * to be processed. The way we can do this is to use a ConcatRowsAndColumns
     * ConcatRC [rac1, rac2, rac3]
     * Run all ops on this
     *
     *
     * The flow would look like:
     * 1. Validate if the operator has an empty OVER clause
     * 2. If 1 is true make a giant rows and columns (R&C) using concat as shown above
     *    Let all operators run amok on that R&C
     * 3. If 1 is false
     *    Read a frame
     *    keep the older row in a class variable
     *    check row by row and compare current with older row to check if partition boundary is reached
     *    when frame partition by changes
     *    create R&C for those particular set of columns, they would have the same partition key
     *    output will be a single R&C
     *    write to output channel
     *
     *
     *  Future thoughts:
     *
     *  1. We are writing 1 partition to each frame in this way. In case of high cardinality data
     *      we will me making a large number of small frames. We can have a check to keep size of frame to a value
     *      say 20k rows and keep on adding to the same pending frame and not create a new frame
     *
     *  2. Current approach with R&C and operators materialize a single R&C for processing. In case of data
     *     with low cardinality a single R&C might be too big to consume. Same for the case of empty OVER() clause
     *     Most of the window operations like SUM(), RANK(), RANGE() etc. can be made with 2 passes of the data.
     *     We might think to reimplement them in the MSQ way so that we do not have to materialize so much data
     */

    // Phase 1 of the execution
    // eagerly validate presence of empty OVER() clause
    boolean status = checkEagerlyForEmptyWindow(operatorFactoryList);
    List<Integer> partitionColsIndex = null;
    if (status) {
      // if OVER() found
      // convert each frame to rac
      // concat all the racs to make a giant rac
      // let all operators run on the giant rac when channel is finished
      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        convertRowFrameToRowsAndColumns(frame, inputFrameReader.signature());
      } else if (inputChannel.isFinished()) {
        runAllOpsOnMultipleRac(frameRowsAndCols);
        return ReturnOrAwait.returnObject(Unit.instance());
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
      return ReturnOrAwait.runAgain();
    } else {
      // Aha, you found a PARTITION BY and maybe ORDER BY TO
      // PARTITION BY can also be on multiple keys
      // typically the last stage would already partition and sort for you
      // figure out frame boundaries and convert each distince group to a rac
      // then run the windowing operator only on each rac
      // work in progress
      if (frameCursor == null || frameCursor.isDone()) {
        if (inputChannel.canRead()) {
          final Frame frame = inputChannel.read();
          frameCursor = FrameProcessors.makeCursor(frame, inputFrameReader);
          final ColumnSelectorFactory frameColumnSelectorFactory = frameCursor.getColumnSelectorFactory();
          partitionColsIndex = findPartitionColumns(inputFrameReader.signature());
          final Supplier<Object>[] fieldSuppliers = new Supplier[inputFrameReader.signature().size()];
          for (int i = 0; i < fieldSuppliers.length; i++) {
            final ColumnValueSelector<?> selector =
                frameColumnSelectorFactory.makeColumnValueSelector(inputFrameReader.signature().getColumnName(i));
            fieldSuppliers[i] = selector::getObject;

          }
          rowSupplierFromFrameCursor = () -> {
            final ResultRow row = ResultRow.create(fieldSuppliers.length);
            for (int i = 0; i < fieldSuppliers.length; i++) {
              row.set(i, fieldSuppliers[i].get());
            }
            return row;
          };
        } else if (inputChannel.isFinished()) {
          // reaached end of channel
          // if there is data remaining
          // write it into a rac
          // and run operators on it
          if (!objectsOfASingleRac.isEmpty()) {
            RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromResultRow(
                objectsOfASingleRac,
                inputFrameReader.signature()
            );
            runAllOpsOnSingleRac(rac);
            objectsOfASingleRac.clear();
          }
          return ReturnOrAwait.returnObject(Unit.instance());
        } else {
          return ReturnOrAwait.runAgain();
        }
      }

      while (!frameCursor.isDone()) {
        final ResultRow currentRow = rowSupplierFromFrameCursor.get();
        if (outputRow == null) {
          outputRow = currentRow.copy();
          objectsOfASingleRac.add(currentRow);
        } else if (comparePartitionKeys(outputRow, currentRow, partitionColsIndex)) {
          // if they have the same partition key
          // keep adding them
          objectsOfASingleRac.add(currentRow);
        } else {
          // key change noted
          // create rac from the rows seen before
          // run the operators on this rows and columns
          // clean up the object to hold the new row only
          RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromResultRow(
              objectsOfASingleRac,
              inputFrameReader.signature()
          );
          runAllOpsOnSingleRac(rac);
          objectsOfASingleRac.clear();
          outputRow = currentRow.copy();
          return ReturnOrAwait.runAgain();
        }
        frameCursor.advance();
      }
    }
    return ReturnOrAwait.runAgain();
  }


  private boolean comparePartitionKeys(ResultRow row1, ResultRow row2, List<Integer> partitionIndices)
  {
    if (partitionIndices == null || partitionIndices.isEmpty()) {
      return row1.equals(row2);
    } else {
      int match = 0;
      for (int i : partitionIndices) {
        if (Objects.equals(row1.get(i), row2.get(i))) {
          match++;
        }
      }
      return match == partitionIndices.size();
    }
  }

  private List<Integer> findPartitionColumns(RowSignature rowSignature)
  {
    List<Integer> indexList = new ArrayList<>();
    for (OperatorFactory of : operatorFactoryList) {
      if (of instanceof NaivePartitioningOperatorFactory) {
        for (String s : ((NaivePartitioningOperatorFactory) of).getPartitionColumns()) {
          indexList.add(rowSignature.indexOf(s));
        }
      }
    }
    return indexList;
  }

  private boolean checkEagerlyForEmptyWindow(List<OperatorFactory> operatorFactoryList)
  {
    for (OperatorFactory of : operatorFactoryList) {
      if (of instanceof NaivePartitioningOperatorFactory) {
        if (((NaivePartitioningOperatorFactory) of).getPartitionColumns().isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }


  private void runOperatorsAfterThis(Operator op)
  {
    for (OperatorFactory of : operatorFactoryList) {
      op = of.wrap(op);
    }
    Operator.go(op, new Operator.Receiver()
    {
      @Override
      public Operator.Signal push(RowsAndColumns rac)
      {
        resultRowAndCols.add(rac);
        return Operator.Signal.GO;
      }

      @Override
      public void completed()
      {
        try {
          flushAllRowsAndCols(resultRowAndCols);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        finally {
          resultRowAndCols.clear();
        }
      }
    });
  }

  private void runAllOpsOnMultipleRac(ArrayList<RowsAndColumns> listOfRacs)
  {
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        RowsAndColumns rac = new ConcatRowsAndColumns(listOfRacs);
        receiver.push(rac);
        receiver.completed();
        return null;
      }
    };
    runOperatorsAfterThis(op);
  }

  private void runAllOpsOnSingleRac(RowsAndColumns singleRac)
  {
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        receiver.push(singleRac);
        receiver.completed();
        return null;
      }
    };
    runOperatorsAfterThis(op);
  }

  private void convertRowFrameToRowsAndColumns(Frame frame, RowSignature signature)
  {
    RowBasedFrameRowAndColumns frameRowsAndColumns = new RowBasedFrameRowAndColumns(frame, signature);
    LazilyDecoratedRowsAndColumns ldrc = new LazilyDecoratedRowsAndColumns(
        frameRowsAndColumns,
        null,
        null,
        null,
        OffsetLimit.limit(Integer.MAX_VALUE),
        null,
        null
    );
    frameRowsAndCols.add(ldrc);
  }

  private void flushAllRowsAndCols(ArrayList<RowsAndColumns> resultRowAndCols) throws IOException
  {
    RowsAndColumns rac = new ConcatRowsAndColumns(resultRowAndCols);
    rac.getColumnNames();
    AtomicInteger rowId = new AtomicInteger(0);
    createFrameWriterIfNeeded(rac, rowId);
    writeRacToFrame(rac, rowId);
  }

  public void writeRacToFrame(RowsAndColumns rac, AtomicInteger rowId) throws IOException
  {
    final int numRows = rac.numRows();
    rowId.set(0);
    while (rowId.get() < numRows) {
      final boolean didAddToFrame = frameWriter.addSelection();
      if (didAddToFrame) {
        rowId.incrementAndGet();
      } else if (frameWriter.getNumRows() == 0) {
        throw new FrameRowTooLargeException(currentAllocatorCapacity);
      } else {
        flushFrameWriter();
        return;
      }
    }
    flushFrameWriter();
  }

  private void createFrameWriterIfNeeded(RowsAndColumns rac, AtomicInteger rowId)
  {
    if (frameWriter == null) {
      final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
      final ColumnSelectorFactory frameWriterColumnSelectorFactory = csfm.make(rowId);
      final FrameWriterFactory frameWriterFactory = getFrameWriterFactory();
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactory);
      currentAllocatorCapacity = frameWriterFactory.allocatorCapacity();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  private long flushFrameWriter() throws IOException
  {
    if (frameWriter == null || frameWriter.getNumRows() <= 0) {
      if (frameWriter != null) {
        frameWriter.close();
        frameWriter = null;
      }
      return 0;
    } else {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      Iterables.getOnlyElement(outputChannels()).write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
      return frame.numRows();
    }
  }
}
