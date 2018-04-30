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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ListenableFutures;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class is specialized for streaming ingestion. In streaming ingestion, the segment lifecycle is like:
 *
 * <pre>
 * APPENDING -> APPEND_FINISHED -> PUBLISHED
 * </pre>
 *
 * <ul>
 * <li>APPENDING: Segment is available for appending.</li>
 * <li>APPEND_FINISHED: Segment cannot be updated (data cannot be added anymore) and is waiting for being published.</li>
 * <li>PUBLISHED: Segment is pushed to deep storage, its metadata is published to metastore, and finally the segment is
 * dropped from local storage</li>
 * </ul>
 */
public class StreamAppenderatorDriver extends BaseAppenderatorDriver
{
  private static final Logger log = new Logger(StreamAppenderatorDriver.class);

  private final SegmentHandoffNotifier handoffNotifier;
  private final FireDepartmentMetrics metrics;
  private final ObjectMapper objectMapper;

  /**
   * Create a driver.
   *
   * @param appenderator           appenderator
   * @param segmentAllocator       segment allocator
   * @param handoffNotifierFactory handoff notifier factory
   * @param usedSegmentChecker     used segment checker
   * @param objectMapper           object mapper, used for serde of commit metadata
   * @param metrics                Firedepartment metrics
   */
  public StreamAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      UsedSegmentChecker usedSegmentChecker,
      DataSegmentKiller dataSegmentKiller,
      ObjectMapper objectMapper,
      FireDepartmentMetrics metrics
  )
  {
    super(appenderator, segmentAllocator, usedSegmentChecker, dataSegmentKiller);

    this.handoffNotifier = Preconditions.checkNotNull(handoffNotifierFactory, "handoffNotifierFactory")
                                        .createSegmentHandoffNotifier(appenderator.getDataSource());
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
  }

  @Override
  @Nullable
  public Object startJob()
  {
    handoffNotifier.start();

    final AppenderatorDriverMetadata metadata = objectMapper.convertValue(
        appenderator.startJob(),
        AppenderatorDriverMetadata.class
    );

    log.info("Restored metadata[%s].", metadata);

    if (metadata != null) {
      synchronized (segments) {
        final Map<String, String> lastSegmentIds = metadata.getLastSegmentIds();
        Preconditions.checkState(
            metadata.getSegments().keySet().equals(lastSegmentIds.keySet()),
            "Sequences for segment states and last segment IDs are not same"
        );

        final Map<String, SegmentsForSequenceBuilder> builders = new TreeMap<>();

        for (Entry<String, List<SegmentWithState>> entry : metadata.getSegments().entrySet()) {
          final String sequenceName = entry.getKey();
          final SegmentsForSequenceBuilder builder = new SegmentsForSequenceBuilder(lastSegmentIds.get(sequenceName));
          builders.put(sequenceName, builder);
          entry.getValue().forEach(builder::add);
        }

        builders.forEach((sequence, builder) -> segments.put(sequence, builder.build()));
      }

      return metadata.getCallerMetadata();
    } else {
      return null;
    }
  }

  public AppenderatorDriverAddResult add(
      InputRow row,
      String sequenceName,
      final Supplier<Committer> committerSupplier
  ) throws IOException
  {
    return append(row, sequenceName, committerSupplier, false, true);
  }

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   *
   * @param row                      the row to add
   * @param sequenceName             sequenceName for this row's segment
   * @param committerSupplier        supplier of a committer associated with all data that has been added, including this row
   *                                 if {@param allowIncrementalPersists} is set to false then this will not be used
   * @param skipSegmentLineageCheck  if true, perform lineage validation using previousSegmentId for this sequence.
   *                                 Should be set to false if replica tasks would index events in same order
   * @param allowIncrementalPersists whether to allow persist to happen when maxRowsInMemory or intermediate persist period
   *                                 threshold is hit
   *
   * @return {@link AppenderatorDriverAddResult}
   *
   * @throws IOException if there is an I/O error while allocating or writing to a segment
   */
  public AppenderatorDriverAddResult add(
      final InputRow row,
      final String sequenceName,
      final Supplier<Committer> committerSupplier,
      final boolean skipSegmentLineageCheck,
      final boolean allowIncrementalPersists
  ) throws IOException
  {
    return append(row, sequenceName, committerSupplier, skipSegmentLineageCheck, allowIncrementalPersists);
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   * This method is to support KafkaIndexTask's legacy mode and will be removed in the future.
   * See KakfaIndexTask.runLegacy().
   */
  public void moveSegmentOut(final String sequenceName, final List<SegmentIdentifier> identifiers)
  {
    synchronized (segments) {
      final SegmentsForSequence activeSegmentsForSequence = segments.get(sequenceName);
      if (activeSegmentsForSequence == null) {
        throw new ISE("WTF?! Asked to remove segments for sequenceName[%s] which doesn't exist...", sequenceName);
      }

      for (final SegmentIdentifier identifier : identifiers) {
        log.info("Moving segment[%s] out of active list.", identifier);
        final long key = identifier.getInterval().getStartMillis();
        if (activeSegmentsForSequence.get(key) == null || activeSegmentsForSequence.get(key).stream().noneMatch(
            segmentWithState -> {
              if (segmentWithState.getSegmentIdentifier().equals(identifier)) {
                segmentWithState.finishAppending();
                return true;
              } else {
                return false;
              }
            }
        )) {
          throw new ISE("WTF?! Asked to remove segment[%s] that didn't exist...", identifier);
        }
      }
    }
  }

  /**
   * Persist all data indexed through this driver so far. Blocks until complete.
   *
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier, boolean, boolean)}.
   *
   * @param committer committer representing all data that has been added so far
   *
   * @return commitMetadata persisted
   */
  public Object persist(final Committer committer) throws InterruptedException
  {
    try {
      log.info("Persisting data.");
      final long start = System.currentTimeMillis();
      final Object commitMetadata = appenderator.persistAll(wrapCommitter(committer)).get();
      log.info("Persisted pending data in %,dms.", System.currentTimeMillis() - start);
      return commitMetadata;
    }
    catch (InterruptedException e) {
      throw e;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Persist all data indexed through this driver so far. Returns a future of persisted commitMetadata.
   *
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier, boolean, boolean)}.
   *
   * @param committer committer representing all data that has been added so far
   *
   * @return future containing commitMetadata persisted
   */
  public ListenableFuture<Object> persistAsync(final Committer committer)
  {
    return appenderator.persistAll(wrapCommitter(committer));
  }

  /**
   * Execute a task in background to publish all segments corresponding to the given sequence names.  The task
   * internally pushes the segments to the deep storage first, and then publishes the metadata to the metadata storage.
   *
   * @param publisher     segment publisher
   * @param committer     committer
   * @param sequenceNames a collection of sequence names to be published
   *
   * @return a {@link ListenableFuture} for the submitted task which removes published {@code sequenceNames} from
   * {@code activeSegments} and {@code publishPendingSegments}
   */
  public ListenableFuture<SegmentsAndMetadata> publish(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    final List<SegmentIdentifier> theSegments = getSegmentWithStates(sequenceNames)
        .map(SegmentWithState::getSegmentIdentifier)
        .collect(Collectors.toList());

    final ListenableFuture<SegmentsAndMetadata> publishFuture = ListenableFutures.transformAsync(
        // useUniquePath=true prevents inconsistencies in segment data when task failures or replicas leads to a second
        // version of a segment with the same identifier containing different data; see DataSegmentPusher.push() docs
        pushInBackground(wrapCommitter(committer), theSegments, true),
        sam -> publishInBackground(
            sam,
            publisher
        )
    );
    return Futures.transform(
        publishFuture,
        (Function<? super SegmentsAndMetadata, ? extends SegmentsAndMetadata>) sam -> {
          synchronized (segments) {
            sequenceNames.forEach(segments::remove);
          }
          return sam;
        }
    );
  }

  /**
   * Register the segments in the given {@link SegmentsAndMetadata} to be handed off and execute a background task which
   * waits until the hand off completes.
   *
   * @param segmentsAndMetadata the result segments and metadata of
   *                            {@link #publish(TransactionalSegmentPublisher, Committer, Collection)}
   *
   * @return null if the input segmentsAndMetadata is null. Otherwise, a {@link ListenableFuture} for the submitted task
   * which returns {@link SegmentsAndMetadata} containing the segments successfully handed off and the metadata
   * of the caller of {@link AppenderatorDriverMetadata}
   */
  public ListenableFuture<SegmentsAndMetadata> registerHandoff(SegmentsAndMetadata segmentsAndMetadata)
  {
    if (segmentsAndMetadata == null) {
      return Futures.immediateFuture(null);

    } else {
      final List<SegmentIdentifier> waitingSegmentIdList = segmentsAndMetadata.getSegments().stream()
                                                                              .map(SegmentIdentifier::fromDataSegment)
                                                                              .collect(Collectors.toList());
      final Object metadata = Preconditions.checkNotNull(segmentsAndMetadata.getCommitMetadata(), "commitMetadata");

      if (waitingSegmentIdList.isEmpty()) {
        return Futures.immediateFuture(
            new SegmentsAndMetadata(
                segmentsAndMetadata.getSegments(),
                ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
            )
        );
      }

      log.info("Register handoff of segments: [%s]", waitingSegmentIdList);

      final SettableFuture<SegmentsAndMetadata> resultFuture = SettableFuture.create();
      final AtomicInteger numRemainingHandoffSegments = new AtomicInteger(waitingSegmentIdList.size());

      for (final SegmentIdentifier segmentIdentifier : waitingSegmentIdList) {
        handoffNotifier.registerSegmentHandoffCallback(
            new SegmentDescriptor(
                segmentIdentifier.getInterval(),
                segmentIdentifier.getVersion(),
                segmentIdentifier.getShardSpec().getPartitionNum()
            ),
            MoreExecutors.sameThreadExecutor(),
            () -> {
              log.info("Segment[%s] successfully handed off, dropping.", segmentIdentifier);
              metrics.incrementHandOffCount();

              final ListenableFuture<?> dropFuture = appenderator.drop(segmentIdentifier);
              Futures.addCallback(
                  dropFuture,
                  new FutureCallback<Object>()
                  {
                    @Override
                    public void onSuccess(Object result)
                    {
                      if (numRemainingHandoffSegments.decrementAndGet() == 0) {
                        log.info("Successfully handed off [%d] segments.", segmentsAndMetadata.getSegments().size());
                        resultFuture.set(
                            new SegmentsAndMetadata(
                                segmentsAndMetadata.getSegments(),
                                ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
                            )
                        );
                      }
                    }

                    @Override
                    public void onFailure(Throwable e)
                    {
                      log.warn(e, "Failed to drop segment[%s]?!", segmentIdentifier);
                      numRemainingHandoffSegments.decrementAndGet();
                      resultFuture.setException(e);
                    }
                  }
              );
            }
        );
      }

      return resultFuture;
    }
  }

  public ListenableFuture<SegmentsAndMetadata> publishAndRegisterHandoff(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    return ListenableFutures.transformAsync(
        publish(publisher, committer, sequenceNames),
        this::registerHandoff
    );
  }

  @Override
  public void close()
  {
    super.close();
    handoffNotifier.close();
  }

  private static class SegmentsForSequenceBuilder
  {
    private final NavigableMap<Long, LinkedList<SegmentWithState>> intervalToSegmentStates;
    private final String lastSegmentId;

    SegmentsForSequenceBuilder(String lastSegmentId)
    {
      this.intervalToSegmentStates = new TreeMap<>();
      this.lastSegmentId = lastSegmentId;
    }

    void add(SegmentWithState segmentWithState)
    {
      final SegmentIdentifier identifier = segmentWithState.getSegmentIdentifier();
      final LinkedList<SegmentWithState> segmentsInInterval = intervalToSegmentStates.computeIfAbsent(
          identifier.getInterval().getStartMillis(),
          k -> new LinkedList<>()
      );
      // always keep APPENDING segments for an interval start millis in the front
      if (segmentWithState.getState() == SegmentState.APPENDING) {
        segmentsInInterval.addFirst(segmentWithState);
      } else {
        segmentsInInterval.addLast(segmentWithState);
      }
    }

    SegmentsForSequence build()
    {
      return new SegmentsForSequence(intervalToSegmentStates, lastSegmentId);
    }
  }
}
