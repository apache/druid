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

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFilePointer;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.CountableInputSourceReader;
import org.apache.druid.msq.input.AdaptedLoadableSegment;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.NilInputSource;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.external.CachedFile;
import org.apache.druid.segment.loading.external.VirtualStorageManager;
import org.apache.druid.utils.CloseableUtils;

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
  private static final Logger log = new Logger(ExternalInputSliceReader.class);

  private final VirtualStorageManager virtualStorageManager;
  private final File temporaryDirectory;
  private final boolean backgroundFetchExternalFiles;

  public ExternalInputSliceReader(
      final VirtualStorageManager virtualStorageManager,
      final File temporaryDirectory,
      final boolean backgroundFetchExternalFiles
  )
  {
    this.virtualStorageManager = virtualStorageManager;
    this.temporaryDirectory = temporaryDirectory;
    this.backgroundFetchExternalFiles = backgroundFetchExternalFiles;
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
                                                  .addTotalFiles(slice.fileCount());
    final List<LoadableSegment> loadableSegments = new ArrayList<>();

    for (final InputSource inputSource : externalInputSlice.getInputSources()) {
      final LoadableSegment segment = makeExternalSegment(
          inputSource,
          externalInputSlice.getInputFormat(),
          externalInputSlice.getSignature(),
          inputCounters,
          counters.warnings(),
          warningPublisher
      );
      loadableSegments.add(segment);
    }

    return new PhysicalInputSlice(ReadablePartitions.empty(), loadableSegments, Collections.emptyList());
  }

  /**
   * Creates a lazy segment that fetches external data when a cursor is created.
   */
  private LoadableSegment makeExternalSegment(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature,
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

    final String description = StringUtils.format("external[%s]", inputSource.toString());
    final SegmentDescriptor descriptor = new SegmentDescriptor(Intervals.ETERNITY, "0", 0);
    final List<InputFilePointer> filePointers = inputSource.asFilePointers();

    if (filePointers != null && backgroundFetchExternalFiles) {
      // The LoadableSegment generated here will cause files to be fetched in VSF loading threads
      // when acquire() is called.
      return new AdaptedLoadableSegment(
          () -> {
            final long startTime = System.nanoTime();
            final List<AsyncResource<CachedFile>> cachedFileResources = new ArrayList<>(filePointers.size());
            try {
              for (final InputFilePointer filePointer : filePointers) {
                cachedFileResources.add(
                    virtualStorageManager.reserveAndPopulateAsync(
                        filePointer.uri().toString(),
                        filePointer.sizeSupplier(),
                        filePointer.populator()
                    )
                );
              }

              final AsyncResource<AcquireSegmentResult> fetched = AsyncResources.transform(
                  AsyncResources.collect(cachedFileResources),
                  cachedFiles -> {
                    long totalSize = 0;
                    final List<File> files = new ArrayList<>(cachedFiles.size());

                    for (final CachedFile cachedFile : cachedFiles) {
                      files.add(cachedFile.getFile());
                      totalSize += cachedFile.getFile().length();
                    }

                    final InputSource localInputSource = new LocalInputSource(null, null, files, null);
                    final ExternalSegment segment = new ExternalSegment(
                        localInputSource,
                        makeReader(schema, localInputSource, inputFormat, channelCounters),
                        warningCounters,
                        warningPublisher,
                        channelCounters,
                        signature
                    );

                    return new AcquireSegmentResult(
                        ReferenceCountedSegmentProvider.of(segment),
                        totalSize,
                        0L,
                        System.nanoTime() - startTime
                    );
                  }
              );

              // If the fetch fails because of insufficient storage space, release the fetched files and fall
              // back to creating an ExternalSegment that streams data directly from the original input source.
              return AsyncResources.recover(
                  fetched,
                  e -> {
                    if (VirtualStorageManager.isInsufficientStorage(e)) {
                      log.noStackTrace()
                         .info(e, "Insufficient storage space to prefetch[%s]; streaming instead.", description);
                      return AcquireSegmentResult.cached(
                          ReferenceCountedSegmentProvider.of(
                              new ExternalSegment(
                                  inputSource,
                                  makeReader(schema, inputSource, inputFormat, channelCounters),
                                  warningCounters,
                                  warningPublisher,
                                  channelCounters,
                                  signature
                              )
                          )
                      );
                    } else {
                      return null;
                    }
                  }
              );
            }
            catch (Throwable e) {
              // Close any cachedFileResources that have been created prior to this exception being thrown.
              throw CloseableUtils.closeAndWrapInCatch(e, CloseableUtils.forIterable(cachedFileResources));
            }
          },
          descriptor,
          description,
          channelCounters
      );
    } else {
      // The LoadableSegment generated here does not acquire a real hold, and ends up loading the external data in a
      // processing thread (when the cursor is created).
      return AdaptedLoadableSegment.fromUnmanagedSegment(
          new ExternalSegment(
              inputSource,
              makeReader(schema, inputSource, inputFormat, channelCounters),
              warningCounters,
              warningPublisher,
              channelCounters,
              signature
          ),
          descriptor,
          description,
          channelCounters
      );
    }
  }

  private InputSourceReader makeReader(
      final InputRowSchema schema,
      final InputSource inputSource,
      final InputFormat inputFormat,
      final ChannelCounters channelCounters
  )
  {
    final boolean incrementCounters = isFileBasedInputSource(inputSource);

    if (incrementCounters) {
      return new CountableInputSourceReader(
          inputSource.reader(schema, inputFormat, temporaryDirectory),
          channelCounters
      );
    } else {
      return inputSource.reader(schema, inputFormat, temporaryDirectory);
    }
  }
}
