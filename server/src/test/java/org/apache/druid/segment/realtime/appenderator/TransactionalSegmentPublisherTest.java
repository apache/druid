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
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.timeline.DataSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TransactionalSegmentPublisherTest
{
  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testPublishSegments_retriesUpto5Times_ifFailureIsRetryable() throws IOException
  {
    final AtomicInteger attemptCount = new AtomicInteger(0);
    final TransactionalSegmentPublisher publisher = createPublisher(
        SegmentPublishResult.retryableFailure("this error is retryable"),
        attemptCount
    );

    Assertions.assertEquals(
        SegmentPublishResult.retryableFailure("this error is retryable"),
        publisher.publishSegments(null, Set.of(), Function.identity(), null, null)
    );
    Assertions.assertEquals(14, attemptCount.get());
  }

  @Test
  public void testPublishSegments_doesNotRetry_ifFailureIsNotRetryable() throws IOException
  {
    final AtomicInteger attemptCount = new AtomicInteger(0);
    final TransactionalSegmentPublisher publisher = createPublisher(
        SegmentPublishResult.fail("this error is not retryable"),
        attemptCount
    );

    Assertions.assertEquals(
        SegmentPublishResult.fail("this error is not retryable"),
        publisher.publishSegments(null, Set.of(), Function.identity(), null, null)
    );
    Assertions.assertEquals(1, attemptCount.get());
  }

  @Test
  public void testPublishAnnotatedSegments_doesNotRetry() throws Exception
  {
    final AtomicInteger attemptCount = new AtomicInteger(0);
    final TransactionalSegmentPublisher publisher = createPublisher(
        SegmentPublishResult.retryableFailure("this error is retryable"),
        attemptCount
    );

    Assertions.assertEquals(
        SegmentPublishResult.retryableFailure("this error is retryable"),
        publisher.publishAnnotatedSegments(null, Set.of(), null, null)
    );
    Assertions.assertEquals(1, attemptCount.get());
  }

  private TransactionalSegmentPublisher createPublisher(
      SegmentPublishResult publishResult,
      AtomicInteger attemptCount
  )
  {
    return new TransactionalSegmentPublisher()
    {
      @Override
      public SegmentPublishResult publishAnnotatedSegments(
          @Nullable Set<DataSegment> segmentsToBeOverwritten,
          Set<DataSegment> segmentsToPublish,
          @Nullable Object commitMetadata,
          @Nullable SegmentSchemaMapping segmentSchemaMapping
      )
      {
        attemptCount.incrementAndGet();
        return publishResult;
      }

      @Override
      void awaitNextRetry(SegmentPublishResult lastResult, int attemptCount)
      {
        // return immediately
      }
    };
  }
}
