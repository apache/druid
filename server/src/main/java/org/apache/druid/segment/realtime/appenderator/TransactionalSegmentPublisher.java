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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

public abstract class TransactionalSegmentPublisher
{
  private static final int QUIET_RETRIES = 3;
  private static final int MAX_RETRIES = 5;

  /**
   * Publish segments, along with some commit metadata, in a single transaction.
   *
   * @return publish result that indicates if segments were published or not. If it is unclear
   * if the segments were published or not, this method must throw an exception. The behavior is similar to
   * IndexerSQLMetadataStorageCoordinator's announceHistoricalSegments.
   *
   * @throws IOException if there was an I/O error when publishing
   * @throws RuntimeException if we cannot tell if the segments were published or not, for some other reason
   */
  public abstract SegmentPublishResult publishAnnotatedSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      @Nullable Object commitMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  ) throws IOException;

  /**
   * Applies the given annotate function on the segments and tries to publish
   * them. If the action fails with a retryable failure, it can be retried upto
   * {@link #MAX_RETRIES} times.
   */
  public final SegmentPublishResult publishSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      Function<Set<DataSegment>, Set<DataSegment>> outputSegmentsAnnotateFunction,
      @Nullable Object commitMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  ) throws IOException
  {
    final Function<Set<DataSegment>, Set<DataSegment>> annotateFunction = outputSegmentsAnnotateFunction
        .andThen(SegmentPublisherHelper::annotateShardSpec);
    final Set<DataSegment> annotatedSegmentsToPublish = annotateFunction.apply(segmentsToPublish);

    int attemptCount = 0;

    // Retry until success or until max retries are exhausted
    SegmentPublishResult result = publishAnnotatedSegments(
        segmentsToBeOverwritten,
        annotatedSegmentsToPublish,
        commitMetadata,
        segmentSchemaMapping
    );
    while (!result.isSuccess() && result.isRetryable() && attemptCount++ < MAX_RETRIES) {
      awaitNextRetry(result, attemptCount);
      result = publishAnnotatedSegments(
          segmentsToBeOverwritten,
          annotatedSegmentsToPublish,
          commitMetadata,
          segmentSchemaMapping
      );
    }

    return result;
  }

  /**
   * @return true if this publisher has action to take when publishing with an empty segment set.
   *         The publisher used by the seekable stream tasks is an example where this is true.
   */
  public boolean supportsEmptyPublish()
  {
    return false;
  }

  /**
   * Sleeps until the next attempt.
   */
  private static void awaitNextRetry(SegmentPublishResult lastResult, int attemptCount)
  {
    try {
      RetryUtils.awaitNextRetry(
          new ISE(lastResult.getErrorMsg()),
          StringUtils.format(
              "Segment publish failed due to error[%s]",
              lastResult.getErrorMsg()
          ),
          attemptCount,
          MAX_RETRIES,
          attemptCount <= QUIET_RETRIES
      );
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
