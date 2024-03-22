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

import com.google.common.collect.Iterators;
import org.apache.druid.collections.ResourceHolder;
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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.CountableInputSourceReader;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.NilInputSource;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Reads {@link ExternalInputSlice} using {@link RowBasedSegment} backed by {@link InputSource#reader}.
 */
public class ExternalInputSliceReader implements InputSliceReader
{
  public static final String SEGMENT_ID = "__external";
  private final File temporaryDirectory;

  public ExternalInputSliceReader(final File temporaryDirectory)
  {
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    final ExternalInputSlice externalInputSlice = (ExternalInputSlice) slice;
    return externalInputSlice.getInputSources().size();
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final ExternalInputSlice externalInputSlice = (ExternalInputSlice) slice;

    return ReadableInputs.segments(
        () -> Iterators.transform(
            inputSourceSegmentIterator(
                externalInputSlice.getInputSources(),
                externalInputSlice.getInputFormat(),
                externalInputSlice.getSignature(),
                temporaryDirectory,
                counters.channel(CounterNames.inputChannel(inputNumber)).setTotalFiles(slice.fileCount()),
                counters.warnings(),
                warningPublisher
            ),
            ReadableInput::segment
        )
    );
  }

  private static Iterator<SegmentWithDescriptor> inputSourceSegmentIterator(
      final List<InputSource> inputSources,
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

    if (!temporaryDirectory.exists() && !temporaryDirectory.mkdir()) {
      throw new ISE("Cannot create temporary directory at [%s]", temporaryDirectory);
    }
    return Iterators.transform(
        inputSources.iterator(),
        inputSource -> {
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

          final SegmentId segmentId = SegmentId.dummy(SEGMENT_ID);
          final Segment segment = new ExternalSegment(
              inputSource,
              reader,
              inputStats,
              warningCounters,
              warningPublisher,
              channelCounters,
              signature
          );
          return new SegmentWithDescriptor(
              () -> ResourceHolder.fromCloseable(segment),
              new RichSegmentDescriptor(segmentId.toDescriptor(), null)
          );
        }
    );
  }

  public static boolean isFileBasedInputSource(final InputSource inputSource)
  {
    return !(inputSource instanceof NilInputSource) && !(inputSource instanceof InlineInputSource);
  }
}
