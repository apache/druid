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
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class FiniteFirehoseProcessor
{
  private static final Logger LOG = new Logger(FiniteFirehoseProcessor.class);

  private final RowIngestionMeters buildSegmentsMeters;
  @Nullable
  private final CircularBuffer<Throwable> buildSegmentsSavedParseExceptions;
  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final long pushTimeout;

  public FiniteFirehoseProcessor(
      RowIngestionMeters buildSegmentsMeters,
      @Nullable CircularBuffer<Throwable> buildSegmentsSavedParseExceptions,
      boolean logParseExceptions,
      int maxParseExceptions,
      long pushTimeout
  )
  {
    this.buildSegmentsMeters = buildSegmentsMeters;
    this.buildSegmentsSavedParseExceptions = buildSegmentsSavedParseExceptions;
    this.logParseExceptions = logParseExceptions;
    this.maxParseExceptions = maxParseExceptions;
    this.pushTimeout = pushTimeout;
  }

  /**
   * This method connects the given {@link FirehoseFactory} and processes data from the connected {@link Firehose}.
   * All read data is consumed by {@link BatchAppenderatorDriver} which creates new segments.
   * All created segments are pushed when all input data is processed successfully.
   *
   * @return {@link SegmentsAndMetadata} for the pushed segments.
   */
  public SegmentsAndMetadata process(
      DataSchema dataSchema,
      BatchAppenderatorDriver driver,
      PartitionsSpec partitionsSpec,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      IndexTaskSegmentAllocator segmentAllocator
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    @Nullable
    final DynamicPartitionsSpec dynamicPartitionsSpec = partitionsSpec instanceof DynamicPartitionsSpec
                                                        ? (DynamicPartitionsSpec) partitionsSpec
                                                        : null;
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    try (
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            buildSegmentsMeters.incrementThrownAway();
            continue;
          }

          if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                "Encountered row with timestamp that cannot be represented as a long: [%s]",
                inputRow
            );
            throw new ParseException(errorMsg);
          }

          final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          if (!optInterval.isPresent()) {
            buildSegmentsMeters.incrementThrownAway();
            continue;
          }

          final Interval interval = optInterval.get();
          final String sequenceName = segmentAllocator.getSequenceName(interval, inputRow);
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
                final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
                LOG.info("Pushed segments[%s]", pushed.getSegments());
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

      final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
      LOG.info("Pushed segments[%s]", pushed.getSegments());
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
      LOG.error(e, "Encountered parse exception:");
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
