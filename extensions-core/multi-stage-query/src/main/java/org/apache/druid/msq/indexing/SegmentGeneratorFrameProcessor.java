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

package org.apache.druid.msq.indexing;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.util.SequenceUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SegmentGeneratorFrameProcessor implements FrameProcessor<DataSegment>
{
  private static final Logger log = new Logger(SegmentGeneratorFrameProcessor.class);

  private final ReadableFrameChannel inChannel;
  private final FrameReader frameReader;
  private final Appenderator appenderator;
  private final SegmentIdWithShardSpec segmentIdWithShardSpec;
  private final List<String> dimensionsForInputRows;
  private final Object2IntMap<String> outputColumnNameToFrameColumnNumberMap;

  private boolean firstRun = true;
  private long rowsWritten = 0L;

  SegmentGeneratorFrameProcessor(
      final ReadableInput readableInput,
      final ColumnMappings columnMappings,
      final List<String> dimensionsForInputRows,
      final Appenderator appenderator,
      final SegmentIdWithShardSpec segmentIdWithShardSpec
  )
  {
    this.inChannel = readableInput.getChannel();
    this.frameReader = readableInput.getChannelFrameReader();
    this.appenderator = appenderator;
    this.segmentIdWithShardSpec = segmentIdWithShardSpec;
    this.dimensionsForInputRows = dimensionsForInputRows;

    outputColumnNameToFrameColumnNumberMap = new Object2IntOpenHashMap<>();
    outputColumnNameToFrameColumnNumberMap.defaultReturnValue(-1);

    for (final ColumnMapping columnMapping : columnMappings.getMappings()) {
      outputColumnNameToFrameColumnNumberMap.put(
          columnMapping.getOutputColumn(),
          frameReader.signature().indexOf(columnMapping.getQueryColumn())
      );
    }
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<DataSegment> runIncrementally(final IntSet readableInputs) throws InterruptedException
  {
    if (firstRun) {
      log.debug("Starting job for segment [%s].", segmentIdWithShardSpec.asSegmentId());
      appenderator.startJob();
      firstRun = false;
    }

    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inChannel.isFinished()) {
      if (rowsWritten == 0) {
        log.debug("Finished reading. No data for segment [%s], skipping.", segmentIdWithShardSpec.asSegmentId());
        return ReturnOrAwait.returnObject(null);
      } else {
        log.debug("Finished reading. Pushing segment [%s].", segmentIdWithShardSpec.asSegmentId());

        // This is a blocking action which violates the FrameProcessor's contract.
        // useUniquePath = false because this class is meant to be used by batch jobs.
        final ListenableFuture<SegmentsAndCommitMetadata> pushFuture =
            appenderator.push(Collections.singletonList(segmentIdWithShardSpec), null, false);

        final SegmentsAndCommitMetadata metadata;

        try {
          metadata = FutureUtils.get(pushFuture, true);
        }
        catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }

        appenderator.clear();

        log.debug("Finished work for segment [%s].", segmentIdWithShardSpec.asSegmentId());
        return ReturnOrAwait.returnObject(Iterables.getOnlyElement(metadata.getSegments()));
      }
    } else {
      if (appenderator.getSegments().isEmpty()) {
        log.debug("Received first frame for segment [%s].", segmentIdWithShardSpec.asSegmentId());
      }

      addFrame(inChannel.read());
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), appenderator::close);
  }

  private void addFrame(final Frame frame)
  {
    final RowSignature signature = frameReader.signature();

    // Reuse input row to avoid redoing allocations.
    final MSQInputRow inputRow = new MSQInputRow();

    final Sequence<Cursor> cursorSequence =
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null);

    SequenceUtils.forEach(
        cursorSequence,
        cursor -> {
          final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

          //noinspection rawtypes
          @SuppressWarnings("rawtypes")
          final List<BaseObjectColumnValueSelector> selectors =
              frameReader.signature()
                         .getColumnNames()
                         .stream()
                         .map(columnSelectorFactory::makeColumnValueSelector)
                         .collect(Collectors.toList());

          while (!cursor.isDone()) {
            for (int j = 0; j < signature.size(); j++) {
              inputRow.getBackingArray()[j] = selectors.get(j).getObject();
            }

            try {
              rowsWritten++;
              appenderator.add(segmentIdWithShardSpec, inputRow, null);
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }

            cursor.advance();
          }
        }
    );
  }

  private class MSQInputRow implements InputRow
  {
    private final Object[] backingArray;
    private final int timeColumnNumber = outputColumnNameToFrameColumnNumberMap.getInt(ColumnHolder.TIME_COLUMN_NAME);

    public MSQInputRow()
    {
      this.backingArray = new Object[frameReader.signature().size()];
    }

    @Override
    public long getTimestampFromEpoch()
    {
      if (timeColumnNumber < 0) {
        return 0;
      } else {
        return MSQTasks.primaryTimestampFromObjectForInsert(backingArray[timeColumnNumber]);
      }
    }

    @Override
    public DateTime getTimestamp()
    {
      return DateTimes.utc(getTimestampFromEpoch());
    }

    @Override
    public List<String> getDimensions()
    {
      return dimensionsForInputRows;
    }

    @Nullable
    @Override
    public Object getRaw(String columnName)
    {
      final int columnNumber = outputColumnNameToFrameColumnNumberMap.getInt(columnName);
      if (columnNumber < 0) {
        return null;
      } else {
        return backingArray[columnNumber];
      }
    }

    @Override
    public List<String> getDimension(String columnName)
    {
      return Rows.objectToStrings(getRaw(columnName));
    }

    @Nullable
    @Override
    public Number getMetric(String columnName)
    {
      return Rows.objectToNumber(columnName, getRaw(columnName), true);
    }

    @Override
    public int compareTo(Row other)
    {
      // Not used during indexing.
      throw new UnsupportedOperationException();
    }

    private Object[] getBackingArray()
    {
      return backingArray;
    }

    @Override
    public String toString()
    {
      return "MSQInputRow{" +
             "backingArray=" + Arrays.toString(backingArray) +
             ", timeColumnNumber=" + timeColumnNumber +
             '}';
    }
  }
}
