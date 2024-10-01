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
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.rowsandcols.ConcatRowsAndColumns;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class WindowOperatorQueryFrameProcessor implements FrameProcessor<Object>
{
  private static final Logger log = new Logger(WindowOperatorQueryFrameProcessor.class);
  private final WindowOperatorQuery query;

  private final List<OperatorFactory> operatorFactoryList;
  private final List<String> partitionColumnNames;
  private final ObjectMapper jsonMapper;
  private final ArrayList<RowsAndColumns> frameRowsAndCols;
  private final ArrayList<RowsAndColumns> resultRowAndCols;
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameWriterFactory frameWriterFactory;
  private final FrameReader frameReader;
  private final int maxRowsMaterialized;
  private Cursor frameCursor = null;
  private Supplier<ResultRow> rowSupplierFromFrameCursor;
  private ResultRow outputRow = null;
  private FrameWriter frameWriter = null;

  private final VirtualColumns frameWriterVirtualColumns;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;

  // List of type strategies to compare the partition columns across rows.
  // Type strategies are pushed in the same order as column types in frameReader.signature()
  private final NullableTypeStrategy[] typeStrategies;

  private final ArrayList<ResultRow> rowsToProcess;
  private int lastPartitionIndex = -1;

  final AtomicInteger rowId = new AtomicInteger(0);

  public WindowOperatorQueryFrameProcessor(
      WindowOperatorQuery query,
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameWriterFactory frameWriterFactory,
      FrameReader frameReader,
      ObjectMapper jsonMapper,
      final List<OperatorFactory> operatorFactoryList,
      final RowSignature rowSignature,
      final int maxRowsMaterializedInWindow,
      final List<String> partitionColumnNames
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameWriterFactory = frameWriterFactory;
    this.operatorFactoryList = operatorFactoryList;
    this.jsonMapper = jsonMapper;
    this.query = query;
    this.frameRowsAndCols = new ArrayList<>();
    this.resultRowAndCols = new ArrayList<>();
    this.rowsToProcess = new ArrayList<>();
    this.maxRowsMaterialized = maxRowsMaterializedInWindow;
    this.partitionColumnNames = partitionColumnNames;

    this.frameReader = frameReader;
    this.typeStrategies = new NullableTypeStrategy[frameReader.signature().size()];
    for (int i = 0; i < frameReader.signature().size(); i++) {
      typeStrategies[i] = frameReader.signature().getColumnType(i).get().getNullableStrategy();
    }

    // Get virtual columns to be added to the frame writer.
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);
    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query);
    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }
    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);
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
  public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs) throws IOException
  {
    /*
     There are 2 scenarios:

     *** Scenario 1: Query has atleast one window function with an OVER() clause without a PARTITION BY ***

     In this scenario, we add all the RACs to a single RowsAndColumns to be processed. We do it via ConcatRowsAndColumns, and run all the operators on the ConcatRowsAndColumns.
     This is done because we anyway need to run the operators on the entire set of rows when we have an OVER() clause without a PARTITION BY.
     This scenario corresponds to partitionColumnNames.isEmpty()=true code flow.

     *** Scenario 2: All window functions in the query have OVER() clause with a PARTITION BY ***

     In this scenario, we need to process rows for each PARTITION BY group together, but we can batch multiple PARTITION BY keys into the same RAC before passing it to the operators for processing.
     Batching is fine since the operators list would have the required NaivePartitioningOperatorFactory to segregate each PARTITION BY group during the processing.

     The flow for this scenario can be summarised as following:
     1. Frame Reading and Cursor Initialization: We start by reading a frame from the inputChannel and initializing frameCursor to iterate over the rows in that frame.
     2. Row Comparison: For each row in the frame, we decide whether it belongs to the same PARTITION BY group as the previous row.
                        This is determined by comparePartitionKeys() method.
                        Please refer to the Javadoc of that method for further details and an example illustration.
        2.1. If the PARTITION BY columns of current row matches the PARTITION BY columns of the previous row,
             they belong to the same PARTITION BY group, and gets added to rowsToProcess.
             If the number of total rows materialized exceed maxRowsMaterialized, we process the pending batch via processRowsUpToLastPartition() method.
        2.2. If they don't match, then we have reached a partition boundary.
             In this case, we update the value for lastPartitionIndex.
     3. End of Input: If the input channel is finished, any remaining rows in rowsToProcess are processed.

     *Illustration of Row Comparison step*

     Let's say we have window_function() OVER (PARTITION BY A ORDER BY B) in our query, and we get 3 frames in the input channel to process.

     Frame 1
     A, B
     1, 2
     1, 3
     2, 1 --> PARTITION BY key (column A) changed from 1 to 2.
     2, 2

     Frame 2
     A, B
     3, 1 --> PARTITION BY key (column A) changed from 2 to 3.
     3, 2
     3, 3
     3, 4

     Frame 3
     A, B
     3, 5
     3, 6
     4, 1 --> PARTITION BY key (column A) changed from 3 to 4.
     4, 2

     *Why batching?*
     We batch multiple PARTITION BY keys for processing together to avoid the overhead of creating different RACs for each PARTITION BY keys, as that would be unnecessary in scenarios where we have a large number of PARTITION BY keys, but each key having a single row.

     *Future thoughts: https://github.com/apache/druid/issues/16126*
     Current approach with R&C and operators materialize a single R&C for processing. In case of data with low cardinality a single R&C might be too big to consume. Same for the case of empty OVER() clause.
     Most of the window operations like SUM(), RANK(), RANGE() etc. can be made with 2 passes of the data. We might think to reimplement them in the MSQ way so that we do not have to materialize so much data.
     */

    // If there are rows pending flush, flush them and run again before processing any more rows.
    if (frameHasRowsPendingFlush()) {
      flushAllRowsAndCols();
      return ReturnOrAwait.runAgain();
    }

    if (partitionColumnNames.isEmpty()) {
      // Scenario 1: Query has atleast one window function with an OVER() clause without a PARTITION BY.
      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        convertRowFrameToRowsAndColumns(frame);
        return ReturnOrAwait.runAgain();
      }

      if (inputChannel.isFinished()) {
        // If no rows are flushed yet, process all rows.
        if (rowId.get() == 0) {
          runAllOpsOnMultipleRac(frameRowsAndCols);
        }

        // If there are still rows pending after operations, run again.
        if (frameHasRowsPendingFlush()) {
          return ReturnOrAwait.runAgain();
        }
        return ReturnOrAwait.returnObject(Unit.instance());
      }
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }

    // Scenario 2: All window functions in the query have OVER() clause with a PARTITION BY
    if (frameCursor == null || frameCursor.isDone()) {
      if (readableInputs.isEmpty()) {
        return ReturnOrAwait.awaitAll(1);
      }

      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read();
        frameCursor = FrameProcessors.makeCursor(frame, frameReader);
        makeRowSupplierFromFrameCursor();
      } else if (inputChannel.isFinished()) {
        // If we have some rows pending processing, process them.
        // We run it again as it's possible that frame writer's capacity got reached and some output rows are
        // pending flush to the output channel.
        if (!rowsToProcess.isEmpty()) {
          lastPartitionIndex = rowsToProcess.size() - 1;
          processRowsUpToLastPartition();
          return ReturnOrAwait.runAgain();
        }
        return ReturnOrAwait.returnObject(Unit.instance());
      } else {
        return ReturnOrAwait.runAgain();
      }
    }

    while (!frameCursor.isDone()) {
      final ResultRow currentRow = rowSupplierFromFrameCursor.get();
      if (outputRow == null) {
        outputRow = currentRow;
        rowsToProcess.add(currentRow);
      } else if (comparePartitionKeys(outputRow, currentRow, partitionColumnNames)) {
        // Add current row to the same batch of rows for processing.
        rowsToProcess.add(currentRow);
      } else {
        lastPartitionIndex = rowsToProcess.size() - 1;
        outputRow = currentRow.copy();
        rowsToProcess.add(currentRow);
      }
      frameCursor.advance();

      if (rowsToProcess.size() > maxRowsMaterialized) {
        // We don't want to materialize more than maxRowsMaterialized rows at any point in time, so process the pending batch.
        processRowsUpToLastPartition();
        ensureMaxRowsInAWindowConstraint(rowsToProcess.size());
        return ReturnOrAwait.runAgain();
      }
    }
    return ReturnOrAwait.runAgain();
  }

  /**
   * @param listOfRacs Concat this list of {@link RowsAndColumns} to a {@link ConcatRowsAndColumns} to use as a single input for the operators to be run
   */
  private void runAllOpsOnMultipleRac(ArrayList<RowsAndColumns> listOfRacs)
  {
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        RowsAndColumns rac = new ConcatRowsAndColumns(listOfRacs);
        ensureMaxRowsInAWindowConstraint(rac.numRows());
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
          flushAllRowsAndCols();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Flushes {@link #resultRowAndCols} to the frame starting from {@link #rowId}, upto the frame writer's capacity.
   * @throws IOException
   */
  private void flushAllRowsAndCols() throws IOException
  {
    RowsAndColumns rac = new ConcatRowsAndColumns(resultRowAndCols);
    createFrameWriterIfNeeded(rac);
    writeRacToFrame(rac);
  }

  /**
   * @param rac   The frame writer to write this {@link RowsAndColumns} object
   */
  private void createFrameWriterIfNeeded(RowsAndColumns rac)
  {
    if (frameWriter == null) {
      final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
      final ColumnSelectorFactory frameWriterColumnSelectorFactory = csfm.make(rowId);
      final ColumnSelectorFactory frameWriterColumnSelectorFactoryWithVirtualColumns =
          frameWriterVirtualColumns.wrap(frameWriterColumnSelectorFactory);
      frameWriter = frameWriterFactory.newFrameWriter(frameWriterColumnSelectorFactoryWithVirtualColumns);
    }
  }

  /**
   * @param rac   {@link RowsAndColumns} to be written to frame
   * @throws IOException
   */
  public void writeRacToFrame(RowsAndColumns rac) throws IOException
  {
    final int numRows = rac.numRows();
    while (rowId.get() < numRows) {
      if (frameWriter.addSelection()) {
        incrementBoostColumn();
        rowId.incrementAndGet();
      } else if (frameWriter.getNumRows() > 0) {
        flushFrameWriter();
        createFrameWriterIfNeeded(rac);

        if (frameWriter.addSelection()) {
          incrementBoostColumn();
          rowId.incrementAndGet();
          return;
        } else {
          throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
        }
      } else {
        throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
      }
    }

    flushFrameWriter();
    clearRACBuffers();
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
  }

  /**
   * Compare two rows based on the columns in partitionColumnNames.
   * If the partitionColumnNames is empty, the method will end up returning true.
   * <p>
   * For example, say:
   * <ul>
   *   <li>partitionColumnNames = ["d1", "d2"]</li>
   *   <li>frameReader's row signature = {d1:STRING, d2:STRING, p0:STRING}</li>
   *   <li>frameReader.signature.indexOf("d1") = 0</li>
   *   <li>frameReader.signature.indexOf("d2") = 1</li>
   *   <li>row1 = [d1_row1, d2_row1, p0_row1]</li>
   *   <li>row2 = [d1_row2, d2_row2, p0_row2]</li>
   * </ul>
   * <p>
   * Then this method will return true if d1_row1==d1_row2 && d2_row1==d2_row2, false otherwise.
   * Returning true would indicate that these 2 rows can be put into the same partition for window function processing.
   */
  private boolean comparePartitionKeys(ResultRow row1, ResultRow row2, List<String> partitionColumnNames)
  {
    int match = 0;
    for (String columnName : partitionColumnNames) {
      int i = frameReader.signature().indexOf(columnName);
      if (typeStrategies[i].compare(row1.get(i), row2.get(i)) == 0) {
        match++;
      }
    }
    return match == partitionColumnNames.size();
  }

  private void makeRowSupplierFromFrameCursor()
  {
    final ColumnSelectorFactory frameColumnSelectorFactory = frameCursor.getColumnSelectorFactory();
    final Supplier<Object>[] fieldSuppliers = new Supplier[frameReader.signature().size()];
    for (int i = 0; i < fieldSuppliers.length; i++) {
      final ColumnValueSelector<?> selector =
          frameColumnSelectorFactory.makeColumnValueSelector(frameReader.signature().getColumnName(i));
      fieldSuppliers[i] = selector::getObject;
    }
    rowSupplierFromFrameCursor = () -> {
      final ResultRow row = ResultRow.create(fieldSuppliers.length);
      for (int i = 0; i < fieldSuppliers.length; i++) {
        row.set(i, fieldSuppliers[i].get());
      }
      return row;
    };
  }

  /**
   * Process rows from rowsToProcess[0, lastPartitionIndex].
   */
  private void processRowsUpToLastPartition()
  {
    if (lastPartitionIndex == -1) {
      return;
    }

    RowsAndColumns singleRac = MapOfColumnsRowsAndColumns.fromResultRowTillIndex(
        rowsToProcess,
        frameReader.signature(),
        lastPartitionIndex
    );
    ArrayList<RowsAndColumns> rowsAndColumns = new ArrayList<>();
    rowsAndColumns.add(singleRac);
    runAllOpsOnMultipleRac(rowsAndColumns);

    // Remove elements in the range [0, lastPartitionIndex] from the list.
    // The call to list.subList(a, b).clear() deletes the elements in the range [a, b - 1],
    // causing the remaining elements to shift and start from index 0.
    rowsToProcess.subList(0, lastPartitionIndex + 1).clear();
    lastPartitionIndex = -1;
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

  /**
   * Increments the value of the partition boosting column. It should be called once the row value has been written
   * to the frame
   */
  private void incrementBoostColumn()
  {
    partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
  }

  /**
   * @return true if frame has rows pending flush to the output channel, false otherwise.
   */
  private boolean frameHasRowsPendingFlush()
  {
    return frameWriter != null && frameWriter.getNumRows() > 0;
  }

  private void clearRACBuffers()
  {
    frameRowsAndCols.clear();
    resultRowAndCols.clear();
    rowId.set(0);
  }
}
