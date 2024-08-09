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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyRowsInAWindowFault;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
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
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameWriterFactory frameWriterFactory;
  private final FrameReader frameReader;
  private final int maxRowsMaterialized;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private FrameWriter frameWriter = null;

  public WindowOperatorQueryFrameProcessor(
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameWriterFactory frameWriterFactory,
      FrameReader frameReader,
      final List<OperatorFactory> operatorFactoryList,
      final int maxRowsMaterializedInWindow
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameWriterFactory = frameWriterFactory;
    this.operatorFactoryList = operatorFactoryList;
    this.frameRowsAndCols = new ArrayList<>();
    this.resultRowAndCols = new ArrayList<>();
    this.maxRowsMaterialized = maxRowsMaterializedInWindow;
    this.frameReader = frameReader;
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
    /*
     * We add all the RACs to a single ConcatRowsAndColumns and pass it to the list of operators to process.
     *  Future thoughts: {@link https://github.com/apache/druid/issues/16126}
     */

    // Bring all data to a single executor for processing.
    // Convert each frame to RAC.
    // Concatenate all the racs to make a giant RAC.
    // Let all operators run on the giant RAC until channel is finished.
    if (inputChannel.canRead()) {
      final Frame frame = inputChannel.read();
      convertRowFrameToRowsAndColumns(frame);
    } else if (inputChannel.isFinished()) {
      runAllOpsOnMultipleRac(frameRowsAndCols);
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }
    return ReturnOrAwait.runAgain();
  }

  /**
   * @param listOfRacs Concat this list of {@link RowsAndColumns} to a {@link ConcatRowsAndColumns} to use as a single input for the operators to be run
   */
  private void runAllOpsOnMultipleRac(ArrayList<RowsAndColumns> listOfRacs)
  {
    if (listOfRacs.isEmpty()) {
      return;
    }
    RowsAndColumns rac = new ConcatRowsAndColumns(listOfRacs);
    if (rac.numRows() > maxRowsMaterialized) {
      throw new MSQException(new TooManyRowsInAWindowFault(rac.numRows(), maxRowsMaterialized));
    }
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        receiver.push(rac);
        receiver.completed();
        return null;
      }
    };
    runOperatorsAfterThis(op);
  }

  /**
   * @param op Base operator for the operators to be run. Other operators are wrapped under this to run
   */
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
          // resultRowsAndCols has reference to frameRowsAndCols
          // due to the chain of calls across the ops
          // so we can clear after writing to output
          flushAllRowsAndCols(resultRowAndCols);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        finally {
          frameRowsAndCols.clear();
          resultRowAndCols.clear();
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
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactory);
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

  private void flushFrameWriter() throws IOException
  {
    if (frameWriter == null || frameWriter.getNumRows() <= 0) {
      if (frameWriter != null) {
        frameWriter.close();
        frameWriter = null;
      }
    } else {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      outputChannel.write(frame);
      frameWriter.close();
      frameWriter = null;
      frame.numRows();
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
    if (frameRowsAndCols.size() + ldrc.numRows() > maxRowsMaterialized) {
      throw new MSQException(new TooManyRowsInAWindowFault(
          frameRowsAndCols.size() + ldrc.numRows(),
          maxRowsMaterialized
      ));
    }
    frameRowsAndCols.add(ldrc);
  }
}
