/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ListenableFutures;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This class is specifialized for batch ingestion. In batch ingestion, the segment lifecycle is like:
 * <p>
 * <pre>
 * APPENDING -> PUSHED_AND_DROPPED -> PUBLISHED
 * </pre>
 * <p>
 * <ul>
 * <li>APPENDING: Segment is available for appending.</li>
 * <li>PUSHED_AND_DROPPED: Segment is pushed to deep storage and dropped from the local storage.</li>
 * <li>PUBLISHED: Segment's metadata is published to metastore.</li>
 * </ul>
 */
public class BatchAppenderatorDriver extends BaseAppenderatorDriver
{
  /**
   * Create a driver.
   *
   * @param appenderator       appenderator
   * @param segmentAllocator   segment allocator
   * @param usedSegmentChecker used segment checker
   */
  public BatchAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      UsedSegmentChecker usedSegmentChecker,
      DataSegmentKiller dataSegmentKiller
  )
  {
    super(appenderator, segmentAllocator, usedSegmentChecker, dataSegmentKiller);
  }

  /**
   * This method always returns null because batch ingestion doesn't support restoring tasks on failures.
   *
   * @return always null
   */
  @Override
  @Nullable
  public Object startJob()
  {
    final Object metadata = appenderator.startJob();
    if (metadata != null) {
      throw new ISE("Metadata should be null because BatchAppenderatorDriver never persists it");
    }
    return null;
  }

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   *
   * @param row          the row to add
   * @param sequenceName sequenceName for this row's segment
   *
   * @return {@link AppenderatorDriverAddResult}
   *
   * @throws IOException if there is an I/O error while allocating or writing to a segment
   */
  public AppenderatorDriverAddResult add(
      InputRow row,
      String sequenceName
  ) throws IOException
  {
    return append(row, sequenceName, null, false, true);
  }

  /**
   * Push and drop all segments in the {@link SegmentState#APPENDING} state.
   *
   * @param pushAndClearTimeoutMs timeout for pushing and dropping segments
   *
   * @return {@link SegmentsAndMetadata} for pushed and dropped segments
   */
  public SegmentsAndMetadata pushAllAndClear(long pushAndClearTimeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    final Collection<String> sequences;
    synchronized (segments) {
      sequences = ImmutableList.copyOf(segments.keySet());
    }

    return pushAndClear(sequences, pushAndClearTimeoutMs);
  }

  private SegmentsAndMetadata pushAndClear(
      Collection<String> sequenceNames,
      long pushAndClearTimeoutMs
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<SegmentIdentifier> segmentIdentifierList = getSegmentWithStates(sequenceNames)
        .filter(segmentWithState -> segmentWithState.getState() == SegmentState.APPENDING)
        .map(SegmentWithState::getSegmentIdentifier)
        .collect(Collectors.toList());

    final ListenableFuture<SegmentsAndMetadata> future = ListenableFutures.transformAsync(
        pushInBackground(null, segmentIdentifierList, false),
        this::dropInBackground
    );

    final SegmentsAndMetadata segmentsAndMetadata = pushAndClearTimeoutMs == 0L ?
                                                    future.get() :
                                                    future.get(pushAndClearTimeoutMs, TimeUnit.MILLISECONDS);

    // Sanity check
    final Map<SegmentIdentifier, DataSegment> pushedSegmentIdToSegmentMap = segmentsAndMetadata
        .getSegments()
        .stream()
        .collect(Collectors.toMap(
            SegmentIdentifier::fromDataSegment,
            dataSegment -> dataSegment
        ));

    final Map<SegmentIdentifier, SegmentWithState> requestedSegmentIdsForSequences = getSegmentWithStates(sequenceNames)
        .filter(segmentWithState -> segmentWithState.getState() == SegmentState.APPENDING)
        .collect(Collectors.toMap(
            SegmentWithState::getSegmentIdentifier,
            segmentWithState -> segmentWithState
        ));

    if (!pushedSegmentIdToSegmentMap.keySet().equals(requestedSegmentIdsForSequences.keySet())) {
      throw new ISE(
          "Pushed segments[%s] are different from the requested ones[%s]",
          pushedSegmentIdToSegmentMap.keySet(),
          requestedSegmentIdsForSequences.keySet()
      );
    }

    // State transition
    requestedSegmentIdsForSequences.forEach(
        (segmentId, segmentWithState) -> {
          segmentWithState.pushAndDrop(pushedSegmentIdToSegmentMap.get(segmentId));
        }
    );

    return segmentsAndMetadata;
  }

  /**
   * Publish all segments.
   *
   * @param publisher segment publisher
   *
   * @return a {@link ListenableFuture} for the publish task
   */
  public ListenableFuture<SegmentsAndMetadata> publishAll(final TransactionalSegmentPublisher publisher)
  {
    final Map<String, SegmentsForSequence> snapshot;
    synchronized (segments) {
      snapshot = ImmutableMap.copyOf(segments);
    }

    return publishInBackground(
        new SegmentsAndMetadata(
            snapshot
                .values()
                .stream()
                .flatMap(SegmentsForSequence::segmentStateStream)
                .map(segmentWithState -> Preconditions
                    .checkNotNull(
                        segmentWithState.getDataSegment(),
                        "dataSegment for segmentId[%s]",
                        segmentWithState.getSegmentIdentifier()
                    )
                )
                .collect(Collectors.toList()),
            null
        ),
        publisher
    );
  }
}
