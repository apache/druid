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

package org.apache.druid.msq.input.external;

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.CountableInputSourceReader;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.NilInputSource;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Reads {@link ExternalInputSlice} using {@link RowBasedSegment} backed by {@link InputSource#reader}.
 */
public class ExternalInputSliceReader implements InputSliceReader
{
  public static final String SEGMENT_ID = "__external";
  public static final SegmentDescriptor SEGMENT_DESCRIPTOR = SegmentId.dummy(SEGMENT_ID).toDescriptor();
  private final File temporaryDirectory;

  public ExternalInputSliceReader(final File temporaryDirectory)
  {
    this.temporaryDirectory = temporaryDirectory;
  }

  public static boolean isFileBasedInputSource(final InputSource inputSource)
  {
    return !(inputSource instanceof NilInputSource) && !(inputSource instanceof InlineInputSource);
  }

  @Override
  public PhysicalInputSlice attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final ExternalInputSlice externalInputSlice = (ExternalInputSlice) slice;
    final ChannelCounters inputCounters = counters.channel(CounterNames.inputChannel(inputNumber))
                                                  .setTotalFiles(slice.fileCount());
    final List<LoadableSegment> loadableSegments = new ArrayList<>();

    for (final InputSource inputSource : externalInputSlice.getInputSources()) {
      // The LoadableSegment generated here does not acquire a real hold, and ends up loading the external data in a
      // processing thread (when the cursor is created). Ideally, this would be better integrated with the virtual
      // storage system, giving us storage holds and the ability to load data outside of a processing thread.
      final Segment segment = makeExternalSegment(
          inputSource,
          externalInputSlice.getInputFormat(),
          externalInputSlice.getSignature(),
          new File(temporaryDirectory, String.valueOf(inputNumber)),
          inputCounters,
          counters.warnings(),
          warningPublisher
      );
      loadableSegments.add(
          LoadableSegment.forSegment(
              segment,
              StringUtils.format("external[%s]", inputSource.toString()),
              null
          )
      );
    }

    return new PhysicalInputSlice(ReadablePartitions.empty(), loadableSegments, Collections.emptyList());
  }

  /**
   * Creates a lazy segment that fetches external data when a cursor is created.
   */
  private Segment makeExternalSegment(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature,
      final File temporaryDirectory,
      final ChannelCounters channelCounters,
      final WarningCounters warningCounters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "auto", DateTimes.utc(0)),
        new DimensionsSpec(
            signature.getColumnNames().stream().map(
                column ->
                    DimensionSchemaUtils.createDimensionSchemaForExtern(
                        column,
                        signature.getColumnType(column).orElse(null)
                    )
            ).collect(Collectors.toList())
        ),
        ColumnsFilter.all()
    );

    final InputSourceReader reader;
    final boolean incrementCounters = isFileBasedInputSource(inputSource);

    final InputStats inputStats = new SimpleRowIngestionMeters();
    if (incrementCounters) {
      reader = new CountableInputSourceReader(
          inputSource.reader(schema, inputFormat, temporaryDirectory),
          channelCounters
      );
    } else {
      reader = inputSource.reader(schema, inputFormat, temporaryDirectory);
    }

    return new ExternalSegment(
        inputSource,
        reader,
        inputStats,
        warningCounters,
        warningPublisher,
        channelCounters,
        signature
    );
  }
}
