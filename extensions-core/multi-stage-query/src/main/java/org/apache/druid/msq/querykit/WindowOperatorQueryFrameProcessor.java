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
  private final ArrayList<ResultRow> objectsOfASingleRac;
  private final int maxRowsMaterialized;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed
  private Cursor frameCursor = null;
  private Supplier<ResultRow> rowSupplierFromFrameCursor;
  private ResultRow outputRow = null;
  private FrameWriter frameWriter = null;

  private final VirtualColumns frameWriterVirtualColumns;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;

  // List of type strategies to compare the partition columns across rows.
  // Type strategies are pushed in the same order as column types in frameReader.signature()
  private final NullableTypeStrategy[] typeStrategies;

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
    this.objectsOfASingleRac = new ArrayList<>();
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
  public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs)
  {
    /*
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
     * 1. Validate if the operator doesn't have any OVER() clause with PARTITION BY for this stage.
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
     *  Future thoughts: {@link https://github.com/apache/druid/issues/16126}
     *
     *  1. We are writing 1 partition to each frame in this way. In case of high cardinality data
     *      we will be making a large number of small frames. We can have a check to keep size of frame to a value
     *      say 20k rows and keep on adding to the same pending frame and not create a new frame
     *
     *  2. Current approach with R&C and operators materialize a single R&C for processing. In case of data
     *     with low cardinality a single R&C might be too big to consume. Same for the case of empty OVER() clause
     *     Most of the window operations like SUM(), RANK(), RANGE() etc. can be made with 2 passes of the data.
     *     We might think to reimplement them in the MSQ way so that we do not have to materialize so much data
     */

    if (partitionColumnNames.isEmpty()) {
      // If we do not have any OVER() clause with PARTITION BY for this stage.
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
    } else {
      // Aha, you found a PARTITION BY and maybe ORDER BY TO
      // PARTITION BY can also be on multiple keys
      // typically the last stage would already partition and sort for you
      // figure out frame boundaries and convert each distinct group to a rac
      // then run the windowing operator only on each rac
      if (frameCursor == null || frameCursor.isDone()) {
        if (readableInputs.isEmpty()) {
          return ReturnOrAwait.awaitAll(1);
        } else if (inputChannel.canRead()) {
          final Frame frame = inputChannel.read();
          frameCursor = FrameProcessors.makeCursor(frame, frameReader);
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
        } else if (inputChannel.isFinished()) {
          // reaached end of channel
          // if there is data remaining
          // write it into a rac
          // and run operators on it
          if (!objectsOfASingleRac.isEmpty()) {
            if (objectsOfASingleRac.size() > maxRowsMaterialized) {
              throw new MSQException(new TooManyRowsInAWindowFault(objectsOfASingleRac.size(), maxRowsMaterialized));
            }
            RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromResultRow(
                objectsOfASingleRac,
                frameReader.signature()
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
          outputRow = currentRow;
          objectsOfASingleRac.add(currentRow);
        } else if (comparePartitionKeys(outputRow, currentRow, partitionColumnNames)) {
          // if they have the same partition key
          // keep adding them after checking
          // guardrails
          objectsOfASingleRac.add(currentRow);
          if (objectsOfASingleRac.size() > maxRowsMaterialized) {
            throw new MSQException(new TooManyRowsInAWindowFault(
                objectsOfASingleRac.size(),
                maxRowsMaterialized
            ));
          }
        } else {
          // key change noted
          // create rac from the rows seen before
          // run the operators on these rows and columns
          // clean up the object to hold the new rows only
          if (objectsOfASingleRac.size() > maxRowsMaterialized) {
            throw new MSQException(new TooManyRowsInAWindowFault(
                objectsOfASingleRac.size(),
                maxRowsMaterialized
            ));
          }
          RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromResultRow(
              objectsOfASingleRac,
              frameReader.signature()
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

  /**
   * @param singleRac Use this {@link RowsAndColumns} as a single input for the operators to be run
   */
  private void runAllOpsOnSingleRac(RowsAndColumns singleRac)
  {
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        receiver.push(singleRac);
        if (singleRac.numRows() > maxRowsMaterialized) {
          throw new MSQException(new TooManyRowsInAWindowFault(singleRac.numRows(), maxRowsMaterialized));
        }
        receiver.completed();
        return null;
      }
    };
    runOperatorsAfterThis(op);
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
        if (rac.numRows() > maxRowsMaterialized) {
          throw new MSQException(new TooManyRowsInAWindowFault(rac.numRows(), maxRowsMaterialized));
        }
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
          frameRowsAndCols.clear();

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
    if (frameRowsAndCols.size() + ldrc.numRows() > maxRowsMaterialized) {
      throw new MSQException(new TooManyRowsInAWindowFault(
          frameRowsAndCols.size() + ldrc.numRows(),
          maxRowsMaterialized
      ));
    }
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
}
