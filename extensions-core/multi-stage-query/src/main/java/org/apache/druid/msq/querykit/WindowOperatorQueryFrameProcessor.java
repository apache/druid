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
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyRowsInAWindowFault;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowOperatorQueryFrameProcessor implements FrameProcessor<Object>
{
  private static final Logger log = new Logger(WindowOperatorQueryFrameProcessor.class);

  private final List<OperatorFactory> operatorFactoryList;
  private final ArrayList<RowsAndColumns> frameRowsAndCols;
  private final ArrayList<RowsAndColumns> resultRowAndCols;
  private int numRowsInFrameRowsAndCols;
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameWriterFactory frameWriterFactory;
  private final FrameReader frameReader;
  private final int maxRowsMaterialized;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private FrameWriter frameWriter = null;

  private final VirtualColumns frameWriterVirtualColumns;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;

  private Operator op = null;

  public WindowOperatorQueryFrameProcessor(
      WindowOperatorQuery query,
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameWriterFactory frameWriterFactory,
      FrameReader frameReader,
      ObjectMapper jsonMapper,
      final List<OperatorFactory> operatorFactoryList
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameWriterFactory = frameWriterFactory;
    this.operatorFactoryList = operatorFactoryList;
    this.frameRowsAndCols = new ArrayList<>();
    this.resultRowAndCols = new ArrayList<>();
    this.maxRowsMaterialized = MultiStageQueryContext.getMaxRowsMaterializedInWindow(query.context());

    this.frameReader = frameReader;

    // Get virtual columns to be added to the frame writer.
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);
    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query);
    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }
    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);

    initialiseOperator();
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs)
  {
    if (inputChannel.canRead()) {
      final Frame frame = inputChannel.read();
      convertRowFrameToRowsAndColumns(frame);

      if (needToProcessBatch()) {
        runAllOpsOnBatch();
        try {
          flushAllRowsAndCols(resultRowAndCols);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return ReturnOrAwait.runAgain();
    } else if (inputChannel.isFinished()) {
      runAllOpsOnBatch();
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }
  }

  private void initialiseOperator()
  {
    op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        RowsAndColumns rac = new ConcatRowsAndColumns(new ArrayList<>(frameRowsAndCols));
        frameRowsAndCols.clear();
        numRowsInFrameRowsAndCols = 0;
        ensureMaxRowsInAWindowConstraint(rac.numRows());
        receiver.push(rac);

        if (inputChannel.isFinished()) {
          // Only call completed() when the input channel is finished.
          receiver.completed();
          return null; // Signal that the operator has completed its work
        }

        // Return a non-null continuation object to indicate that we want to continue processing.
        return () -> {};
      }
    };
    for (OperatorFactory of : operatorFactoryList) {
      op = of.wrap(op);
    }
  }

  private void runAllOpsOnBatch()
  {
    op.goOrContinue(null, new Operator.Receiver()
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
          // resultRowsAndCols has reference to frameRowsAndCols
          // due to the chain of calls across the ops
          // so we can clear after writing to output
          flushAllRowsAndCols(resultRowAndCols);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * @param resultRowAndCols Flush the list of {@link RowsAndColumns} to a frame
   * @throws IOException
   */
  private void flushAllRowsAndCols(ArrayList<RowsAndColumns> resultRowAndCols) throws IOException
  {
    RowsAndColumns rac = new ConcatRowsAndColumns(resultRowAndCols);
    AtomicInteger rowId = new AtomicInteger(0);
    createFrameWriterIfNeeded(rac, rowId);
    writeRacToFrame(rac, rowId);
    frameRowsAndCols.clear();
    resultRowAndCols.clear();
    numRowsInFrameRowsAndCols = 0;
  }

  /**
   * @param rac   The frame writer to write this {@link RowsAndColumns} object
   * @param rowId RowId to get the column selector factory from the {@link RowsAndColumns} object
   */
  private void createFrameWriterIfNeeded(RowsAndColumns rac, AtomicInteger rowId)
  {
    if (frameWriter == null) {
      final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
      final ColumnSelectorFactory frameWriterColumnSelectorFactory = csfm.make(rowId);
      final ColumnSelectorFactory frameWriterColumnSelectorFactoryWithVirtualColumns =
          frameWriterVirtualColumns.wrap(frameWriterColumnSelectorFactory);
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactoryWithVirtualColumns);
      currentAllocatorCapacity = frameWriterFactory.allocatorCapacity();
    }
  }

  /**
   * @param rac   {@link RowsAndColumns} to be written to frame
   * @param rowId Counter to keep track of how many rows are added
   * @throws IOException
   */
  public void writeRacToFrame(RowsAndColumns rac, AtomicInteger rowId) throws IOException
  {
    final int numRows = rac.numRows();
    rowId.set(0);
    while (rowId.get() < numRows) {
      final boolean didAddToFrame = frameWriter.addSelection();
      if (didAddToFrame) {
        rowId.incrementAndGet();
        partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
      } else if (frameWriter.getNumRows() == 0) {
        throw new FrameRowTooLargeException(currentAllocatorCapacity);
      } else {
        flushFrameWriter();
        return;
      }
    }
    flushFrameWriter();
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), frameWriter);
  }

  /**
   * @return Number of rows flushed to the output channel
   * @throws IOException
   */
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

  /**
   * @param frame Row based frame to be converted to a {@link RowsAndColumns} object
   * Throw an exception if the resultant rac used goes above the guardrail value
   */
  private void convertRowFrameToRowsAndColumns(Frame frame)
  {
    final RowSignature signature = frameReader.signature();
    RowBasedFrameRowsAndColumns frameRowsAndColumns = new RowBasedFrameRowsAndColumns(frame, signature);
    LazilyDecoratedRowsAndColumns ldrc = new LazilyDecoratedRowsAndColumns(
        frameRowsAndColumns,
        null,
        null,
        null,
        OffsetLimit.limit(Integer.MAX_VALUE),
        null,
        null
    );
    // check if existing + newly added rows exceed guardrails
    ensureMaxRowsInAWindowConstraint(frameRowsAndCols.size() + ldrc.numRows());
    frameRowsAndCols.add(ldrc);
    numRowsInFrameRowsAndCols += ldrc.numRows();
  }

  private void ensureMaxRowsInAWindowConstraint(int numRowsInWindow)
  {
    if (numRowsInWindow > maxRowsMaterialized) {
      throw new MSQException(new TooManyRowsInAWindowFault(
          numRowsInWindow,
          maxRowsMaterialized
      ));
    }
  }

  private boolean needToProcessBatch()
  {
    return numRowsInFrameRowsAndCols >= maxRowsMaterialized / 2; // Can this be improved further?
  }
}
