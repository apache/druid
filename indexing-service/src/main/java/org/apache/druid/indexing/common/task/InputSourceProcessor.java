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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class InputSourceProcessor
{
  private static final Logger LOG = new Logger(InputSourceProcessor.class);

  /**
   * This method opens the given {@link InputSource} and processes data via {@link InputSourceReader}.
   * All read data is consumed by {@link BatchAppenderatorDriver} which creates new segments.
   * All created segments are pushed when all input data is processed successfully.
   *
   * @return {@link SegmentsAndCommitMetadata} for the pushed segments.
   */
  public static SegmentsAndCommitMetadata process(
      DataSchema dataSchema,
      BatchAppenderatorDriver driver,
      PartitionsSpec partitionsSpec,
      InputSource inputSource,
      @Nullable InputFormat inputFormat,
      File tmpDir,
      SequenceNameFunction sequenceNameFunction,
      IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder,
      RowIngestionMeters buildSegmentsMeters,
      ParseExceptionHandler parseExceptionHandler,
      long pushTimeout
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    @Nullable
    final DynamicPartitionsSpec dynamicPartitionsSpec = partitionsSpec instanceof DynamicPartitionsSpec
                                                        ? (DynamicPartitionsSpec) partitionsSpec
                                                        : null;
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();

    try (
        final CloseableIterator<InputRow> inputRowIterator = AbstractBatchIndexTask.inputSourceReader(
            tmpDir,
            dataSchema,
            inputSource,
            inputFormat,
            AbstractBatchIndexTask.defaultRowFilter(granularitySpec),
            buildSegmentsMeters,
            parseExceptionHandler
        );
        final HandlingInputRowIterator iterator = inputRowIteratorBuilder
            .delegate(inputRowIterator)
            .granularitySpec(granularitySpec)
            .build()
    ) {
      while (iterator.hasNext()) {
        final InputRow inputRow = iterator.next();
        if (inputRow == null) {
          continue;
        }

        // IndexTaskInputRowIteratorBuilder.absentBucketIntervalConsumer() ensures the interval will be present here
        Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        final Interval interval = optInterval.get().withChronology(ISOChronology.getInstanceUTC());

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
      }

      final SegmentsAndCommitMetadata pushed = driver.pushAllAndClear(pushTimeout);
      LOG.debugSegments(pushed.getSegments(), "Pushed segments");

      return pushed;
    }
  }
}
