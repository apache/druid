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
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.Operator;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class WindowOperatorQueryFrameProcessor extends BaseLeafFrameProcessor
{

  private static final Logger log = new Logger(WindowOperatorQueryFrameProcessor.class);
  private final WindowOperatorQuery query;

  private final List<OperatorFactory> operatorFactoryList;
  private final ObjectMapper jsonMapper;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;
  private final VirtualColumns frameWriterVirtualColumns;
  private final Closer closer = Closer.create();
  private final RowSignature outputStageSignature;

  private Cursor cursor;
  private Segment segment;
  private final SimpleSettableOffset cursorOffset = new SimpleAscendingOffset(Integer.MAX_VALUE);
  private FrameWriter frameWriter;
  private long currentAllocatorCapacity; // Used for generating FrameRowTooLargeException if needed

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
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);
    this.operatorFactoryList = operatorFactoryList;
    this.outputStageSignature = rowSignature;

    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    frameWriterVirtualColumns.add(partitionBoostVirtualColumn);

    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(jsonMapper, query);

    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }

    this.frameWriterVirtualColumns = VirtualColumns.create(frameWriterVirtualColumns);
  }

  @Override
  protected ReturnOrAwait<Unit> runWithSegment(SegmentWithDescriptor segment)
  {
    return null;
  }

  @Override
  protected ReturnOrAwait<Unit> runWithLoadedSegment(SegmentWithDescriptor segment)
  {
    return null;
  }

  // previous stage output
  @Override
  protected ReturnOrAwait<Unit> runWithInputChannel(ReadableFrameChannel inputChannel, FrameReader inputFrameReader)
  {
    // Read the frames from the channel
    // convert to FrameRowsAndColumns

    if (inputChannel.canRead()) {
      Frame f = inputChannel.read();

      // the frame here is row based
      // frame rows and columns need columnar. discuss with Eric
      // Action item: need to implement a new rows and columns that accept a row-based frame

      // Create a frame rows and columns what would
      /**
       *  OVER(PARTITION BY m1)
       */
      RowBasedFrameRowAndColumns frameRowsAndColumns = new RowBasedFrameRowAndColumns(f, inputFrameReader.signature());
      Operator op = getOperator(frameRowsAndColumns);
      //
      //Operator op = new SegmentToRowsAndColumnsOperator(frameSegment);
      // On the operator created above add the operators present in the query that we want to chain

      // previous shuffle has partitioning
      // need to remove partitioning here

      for (OperatorFactory of : operatorFactoryList) {
        op = of.wrap(op);
      }

      // Let the operator run
      // the results that come in the receiver must be pushed to the outout channel
      // need to transform the output rows and columns back to frame
      Operator.go(op, new Operator.Receiver()
      {
        @Override
        public Operator.Signal push(RowsAndColumns rac)
        {
          // convert the rac to a row-based frame
          // Note that there can be multiple racs here
          // one per partition
          // But one rac will be written to 1 frame
          Pair<byte[], RowSignature> pairFrames = materializeRacToRowFrames(rac, outputStageSignature);
          Frame f = Frame.wrap(pairFrames.lhs);
          try {
            Iterables.getOnlyElement(outputChannels())
                     .write(new FrameWithPartition(f, FrameWithPartition.NO_PARTITION));
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          return Operator.Signal.GO;
        }

        @Override
        public void completed()
        {

        }
      });

    } else if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    }
    return ReturnOrAwait.runAgain();
  }

  private static Operator getOperator(RowBasedFrameRowAndColumns frameRowsAndColumns)
  {
    LazilyDecoratedRowsAndColumns ldrc = new LazilyDecoratedRowsAndColumns(
        frameRowsAndColumns,
        null,
        null,
        null,
        OffsetLimit.limit(Integer.MAX_VALUE),
        null,
        null
    );
    // Create an operator on top of the created rows and columns
    Operator op = new Operator()
    {
      @Nullable
      @Override
      public Closeable goOrContinue(Closeable continuationObject, Receiver receiver)
      {
        receiver.push(ldrc);
        receiver.completed();
        return continuationObject;
      }
    };
    return op;
  }

  public Pair<byte[], RowSignature> materializeRacToRowFrames(RowsAndColumns rac, RowSignature outputSignature)
  {
    final int numRows = rac.numRows();

    BitSet rowsToSkip = null;

    AtomicInteger rowId = new AtomicInteger(0);
    final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
    final ColumnSelectorFactory selectorFactory = csfm.make(rowId);

    ArrayList<String> columnsToGenerate = new ArrayList<>(outputSignature.getColumnNames());


    final RowSignature.Builder sigBob = RowSignature.builder();
    final ArenaMemoryAllocatorFactory memFactory = new ArenaMemoryAllocatorFactory(200 << 20);


    for (String column : columnsToGenerate) {
      final Column racColumn = rac.findColumn(column);
      if (racColumn == null) {
        continue;
      }
      sigBob.add(column, racColumn.toAccessor().getType());
    }

    long remainingRowsToSkip = 0;
    long remainingRowsToFetch = Integer.MAX_VALUE;

    final FrameWriter frameWriter;
    frameWriter = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        memFactory,
        sigBob.build(),
        Collections.emptyList()
    ).newFrameWriter(selectorFactory);

    rowId.set(0);
    for (; rowId.get() < numRows && remainingRowsToFetch > 0; rowId.incrementAndGet()) {
      final int theId = rowId.get();
      if (rowsToSkip != null && rowsToSkip.get(theId)) {
        continue;
      }
      if (remainingRowsToSkip > 0) {
        remainingRowsToSkip--;
        continue;
      }
      remainingRowsToFetch--;
      frameWriter.addSelection();
    }
    return Pair.of(frameWriter.toByteArray(), sigBob.build());
  }
}
