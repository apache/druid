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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Optional;
import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class InputSourceProcessor
{
  private static final Logger LOG = new Logger(InputSourceProcessor.class);

  private final RowIngestionMeters buildSegmentsMeters;
  @Nullable
  private final CircularBuffer<Throwable> buildSegmentsSavedParseExceptions;
  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final long pushTimeout;
  private final IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder;

  public InputSourceProcessor(
      RowIngestionMeters buildSegmentsMeters,
      @Nullable CircularBuffer<Throwable> buildSegmentsSavedParseExceptions,
      boolean logParseExceptions,
      int maxParseExceptions,
      long pushTimeout,
      IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder
  )
  {
    this.buildSegmentsMeters = buildSegmentsMeters;
    this.buildSegmentsSavedParseExceptions = buildSegmentsSavedParseExceptions;
    this.logParseExceptions = logParseExceptions;
    this.maxParseExceptions = maxParseExceptions;
    this.pushTimeout = pushTimeout;
    this.inputRowIteratorBuilder = inputRowIteratorBuilder;
  }

  /**
   * This method opens the given {@link InputSource} and processes data via {@link InputSourceReader}.
   * All read data is consumed by {@link BatchAppenderatorDriver} which creates new segments.
   * All created segments are pushed when all input data is processed successfully.
   *
   * @return {@link SegmentsAndCommitMetadata} for the pushed segments.
   */
  public SegmentsAndCommitMetadata process(
      DataSchema dataSchema,
      BatchAppenderatorDriver driver,
      PartitionsSpec partitionsSpec,
      InputSource inputSource,
      @Nullable InputFormat inputFormat,
      File tmpDir,
      SequenceNameFunction sequenceNameFunction
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    @Nullable
    final DynamicPartitionsSpec dynamicPartitionsSpec = partitionsSpec instanceof DynamicPartitionsSpec
                                                        ? (DynamicPartitionsSpec) partitionsSpec
                                                        : null;
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();

    final List<String> metricsNames = Arrays.stream(dataSchema.getAggregators())
                                            .map(AggregatorFactory::getName)
                                            .collect(Collectors.toList());
    final InputSourceReader inputSourceReader = dataSchema.getTransformSpec().decorate(
        inputSource.reader(
            new InputRowSchema(
                dataSchema.getTimestampSpec(),
                dataSchema.getDimensionsSpec(),
                metricsNames
            ),
            inputFormat,
            tmpDir
        )
    );
    try (
        final CloseableIterator<InputRow> inputRowIterator = inputSourceReader.read();
        HandlingInputRowIterator iterator = inputRowIteratorBuilder
            .delegate(inputRowIterator)
            .granularitySpec(granularitySpec)
            .nullRowRunnable(buildSegmentsMeters::incrementThrownAway)
            .absentBucketIntervalConsumer(inputRow -> buildSegmentsMeters.incrementThrownAway())
            .build()
    ) {
      while (iterator.hasNext()) {
        try {
          final InputRow inputRow = iterator.next();
          if (inputRow == null) {
            continue;
          }

          // IndexTaskInputRowIteratorBuilder.absentBucketIntervalConsumer() ensures the interval will be present here
          Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          @SuppressWarnings("OptionalGetWithoutIsPresent")
          final Interval interval = optInterval.get();

          final String sequenceName = sequenceNameFunction.getSequenceName(interval, inputRow);
          final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName);

          if (addResult.isOk()) {
            // incremental segment publishment is allowed only when rollup doesn't have to be perfect.
            if (dynamicPartitionsSpec != null) {
              final boolean isPushRequired = addResult.isPushRequired(
                  dynamicPartitionsSpec.getMaxRowsPerSegment(),
                  dynamicPartitionsSpec.getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
              );
              if (isPushRequired) {
                // There can be some segments waiting for being pushed even though no more rows will be added to them
                // in the future.
                // If those segments are not pushed here, the remaining available space in appenderator will be kept
                // small which could lead to smaller segments.
                final SegmentsAndCommitMetadata pushed = driver.pushAllAndClear(pushTimeout);
                LOG.debugSegments(pushed.getSegments(), "Pushed segments");
              }
            }
          } else {
            throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
          }

          if (addResult.getParseException() != null) {
            handleParseException(addResult.getParseException());
          } else {
            buildSegmentsMeters.incrementProcessed();
          }
        }
        catch (ParseException e) {
          handleParseException(e);
        }
      }

      final SegmentsAndCommitMetadata pushed = driver.pushAllAndClear(pushTimeout);
      LOG.debugSegments(pushed.getSegments(), "Pushed segments");

      return pushed;
    }
  }

  private void handleParseException(ParseException e)
  {
    if (e.isFromPartiallyValidRow()) {
      buildSegmentsMeters.incrementProcessedWithError();
    } else {
      buildSegmentsMeters.incrementUnparseable();
    }

    if (logParseExceptions) {
      LOG.error(e, "Encountered parse exception");
    }

    if (buildSegmentsSavedParseExceptions != null) {
      buildSegmentsSavedParseExceptions.add(e);
    }

    if (buildSegmentsMeters.getUnparseable() + buildSegmentsMeters.getProcessedWithError() > maxParseExceptions) {
      LOG.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...", e);
    }
  }
}
