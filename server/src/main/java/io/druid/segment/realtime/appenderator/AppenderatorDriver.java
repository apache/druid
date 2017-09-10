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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.SegmentTracker;
import io.druid.segment.realtime.SegmentTrackerMetadata;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A AppenderatorDriver drives an Appenderator to index a finite stream of data. This class does not help you
 * index unbounded streams. All handoff is done at the end of indexing.
 *
 * This class helps with doing things that Appenderators don't, including deciding which segments to use (with a
 * SegmentAllocator), publishing segments to the metadata store (with a SegmentPublisher), and monitoring handoff (with
 * a SegmentHandoffNotifier).
 *
 * Note that the commit metadata stored by this class via the underlying Appenderator is not the same metadata as
 * you pass in. It's wrapped in some extra metadata needed by the driver.
 */
public class AppenderatorDriver implements Closeable
{
  private static final Logger log = new Logger(AppenderatorDriver.class);

  private final Appenderator appenderator;
  private final SegmentTracker segmentTracker;
  private final SegmentHandoffNotifier handoffNotifier;
  private final UsedSegmentChecker usedSegmentChecker;
  private final ObjectMapper objectMapper;
  private final FireDepartmentMetrics metrics;
  private final ListeningExecutorService publishExecutor;

  /**
   * Create a driver.
   *
   * @param appenderator            appenderator
   * @param segmentAllocator        segment allocator
   * @param handoffNotifierFactory  handoff notifier factory
   * @param usedSegmentChecker      used segment checker
   * @param objectMapper            object mapper, used for serde of commit metadata
   * @param metrics                 Firedepartment metrics
   */
  public AppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      UsedSegmentChecker usedSegmentChecker,
      ObjectMapper objectMapper,
      FireDepartmentMetrics metrics
  )
  {
    this.appenderator = Preconditions.checkNotNull(appenderator, "appenderator");
    this.segmentTracker = new SegmentTracker(
        Preconditions.checkNotNull(segmentAllocator, "segmentAllocator")
    );
    this.handoffNotifier = Preconditions.checkNotNull(handoffNotifierFactory, "handoffNotifierFactory")
                                        .createSegmentHandoffNotifier(appenderator.getDataSource());
    this.usedSegmentChecker = Preconditions.checkNotNull(usedSegmentChecker, "usedSegmentChecker");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.publishExecutor = MoreExecutors.listeningDecorator(Execs.singleThreaded("publish-%d"));
  }

  @VisibleForTesting
  SegmentTracker getSegmentTracker()
  {
    return segmentTracker;
  }

  /**
   * Perform any initial setup and return currently persisted commit metadata.
   *
   * Note that this method returns the same metadata you've passed in with your Committers, even though this class
   * stores extra metadata on disk.
   *
   * @return currently persisted commit metadata
   */
  public Object startJob()
  {
    handoffNotifier.start();

    final SegmentTrackerMetadata metadata = objectMapper.convertValue(
        appenderator.startJob(),
        SegmentTrackerMetadata.class
    );

    log.info("Restored metadata[%s].", metadata);

    if (metadata != null) {
      segmentTracker.restoreFromMetadata(metadata);

      return metadata.getCallerMetadata();
    } else {
      return null;
    }
  }

  /**
   * Clears out all our state and also calls {@link Appenderator#clear()} on the underlying Appenderator.
   */
  public void clear() throws InterruptedException
  {
    segmentTracker.clear();
    appenderator.clear();
  }

  /**
   * Add a row. Must not be called concurrently from multiple threads.
   *
   * @param row               the row to add
   * @param sequenceName      sequenceName for this row's segment
   * @param committerSupplier supplier of a committer associated with all data that has been added, including this row
   *
   * @return segment to which this row was added, or null if segment allocator returned null for this row
   *
   * @throws IOException if there is an I/O error while allocating or writing to a segment
   */
  public AppenderatorDriverAddResult add(
      final InputRow row,
      final String sequenceName,
      final Supplier<Committer> committerSupplier
  ) throws IOException
  {
    Preconditions.checkNotNull(row, "row");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(committerSupplier, "committerSupplier");

    final SegmentIdentifier identifier = segmentTracker.getSegment(row, sequenceName);

    if (identifier != null) {
      try {
        final int numRowsInMemory = appenderator.add(identifier, row, wrapCommitterSupplier(committerSupplier));
        return AppenderatorDriverAddResult.ok(identifier, numRowsInMemory, appenderator.getTotalRowCount());
      }
      catch (SegmentNotWritableException e) {
        throw new ISE(e, "WTF?! Segment[%s] not writable when it should have been.", identifier);
      }
    } else {
      return AppenderatorDriverAddResult.fail();
    }
  }

  /**
   * Persist all data indexed through this driver so far. Blocks until complete.
   *
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier)}.
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
   * Register the segments in the given {@link SegmentsAndMetadata} to be handed off and execute a background task which
   * waits until the hand off completes.
   *
   * @param segmentsAndMetadata the result segments and metadata of
   *                            {@link #publish(TransactionalSegmentPublisher, Committer, Collection)}
   *
   * @return null if the input segmentsAndMetadata is null. Otherwise, a {@link ListenableFuture} for the submitted task
   *         which returns {@link SegmentsAndMetadata} containing the segments successfully handed off and the metadata
   *         of the caller of {@link SegmentTrackerMetadata}
   */
  public ListenableFuture<SegmentsAndMetadata> registerHandoff(SegmentsAndMetadata segmentsAndMetadata)
  {
    if (segmentsAndMetadata == null) {
      return Futures.immediateFuture(null);

    } else {
      final List<SegmentIdentifier> waitingSegmentIdList = segmentsAndMetadata.getSegments().stream()
                                                                              .map(SegmentIdentifier::fromDataSegment)
                                                                              .collect(Collectors.toList());

      if (waitingSegmentIdList.isEmpty()) {
        return Futures.immediateFuture(
            new SegmentsAndMetadata(
                segmentsAndMetadata.getSegments(),
                ((SegmentTrackerMetadata) segmentsAndMetadata.getCommitMetadata())
                    .getCallerMetadata()
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
                        log.info("All segments handed off.");
                        resultFuture.set(
                            new SegmentsAndMetadata(
                                segmentsAndMetadata.getSegments(),
                                ((SegmentTrackerMetadata) segmentsAndMetadata.getCommitMetadata())
                                    .getCallerMetadata()
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

  /**
   * Closes this driver. Does not close the underlying Appenderator; you should do that yourself.
   */
  @Override
  public void close()
  {
    publishExecutor.shutdownNow();
    handoffNotifier.close();
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   */
  public void moveSegmentOut(final String sequenceName, final List<SegmentIdentifier> identifiers)
  {
    segmentTracker.moveSegmentOut(sequenceName, identifiers);
  }

  /**
   * Publish all pending segments.
   *
   * @param publisher segment publisher
   * @param committer committer
   *
   * @return a {@link ListenableFuture} for the publish task which removes published {@code sequenceNames} from
   *         {@code activeSegments} and {@code publishPendingSegments}
   */
  public ListenableFuture<SegmentsAndMetadata> publishAll(
      final TransactionalSegmentPublisher publisher,
      final Committer committer
  )
  {
    final List<SegmentIdentifier> theSegments = segmentTracker.removePublished();
    return publish(publisher, wrapCommitter(committer), theSegments);
  }

  /**
   * Execute a task in background to publish all segments corresponding to the given sequence names.  The task
   * internally pushes the segments to the deep storage first, and then publishes the metadata to the metadata storage.
   *
   * @param publisher segment publisher
   * @param committer committer
   * @param sequenceNames a collection of sequence names to be published
   *
   * @return a {@link ListenableFuture} for the submitted task which removes published {@code sequenceNames} from
   *         {@code activeSegments} and {@code publishPendingSegments}
   */
  public ListenableFuture<SegmentsAndMetadata> publish(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    final List<SegmentIdentifier> theSegments = segmentTracker.removePublished(sequenceNames);

    return publish(publisher, wrapCommitter(committer), theSegments);
  }

  /**
   * Execute a task in background to publish the given segments.  The task blocks until complete.
   * Retries forever on transient failures, but may exit early on permanent failures.
   *
   * Should be called after all data has been added through {@link #add(InputRow, String, Supplier)}.
   *
   * @param publisher publisher to use for this set of segments
   * @param wrappedCommitter committer representing all data that has been added so far
   *
   * @return segments and metadata published if successful, or null if segments could not be handed off due to
   * transaction failure with commit metadata.
   */
  private ListenableFuture<SegmentsAndMetadata> publish(
      final TransactionalSegmentPublisher publisher,
      final WrappedCommitter wrappedCommitter,
      final List<SegmentIdentifier> segmentIdentifiers
  )
  {
    log.info("Pushing segments: [%s]", Joiner.on(", ").join(segmentIdentifiers));

    return Futures.transform(
        appenderator.push(segmentIdentifiers, wrappedCommitter),
        (Function<SegmentsAndMetadata, SegmentsAndMetadata>) segmentsAndMetadata -> {
          // Sanity check
          final Set<SegmentIdentifier> pushedSegments = segmentsAndMetadata.getSegments().stream()
                                                                           .map(SegmentIdentifier::fromDataSegment)
                                                                           .collect(Collectors.toSet());
          if (!pushedSegments.equals(Sets.newHashSet(segmentIdentifiers))) {
            throw new ISE(
                "WTF?! Pushed different segments than requested. Pushed[%s], requested[%s].",
                pushedSegments,
                segmentIdentifiers
            );
          }

          if (segmentsAndMetadata.getSegments().isEmpty()) {
            log.info("Nothing to publish, skipping publish step.");
          } else {
            log.info(
                "Publishing segments with commitMetadata[%s]: [%s]",
                segmentsAndMetadata.getCommitMetadata(),
                Joiner.on(", ").join(segmentsAndMetadata.getSegments())
            );

            try {
              final boolean published = publisher.publishSegments(
                  ImmutableSet.copyOf(segmentsAndMetadata.getSegments()),
                  ((SegmentTrackerMetadata) segmentsAndMetadata.getCommitMetadata()).getCallerMetadata()
              );

              if (published) {
                log.info("Published segments.");
              } else {
                log.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
                if (usedSegmentChecker.findUsedSegments(pushedSegments)
                                      .equals(Sets.newHashSet(segmentsAndMetadata.getSegments()))) {
                  log.info("Our segments really do exist, awaiting handoff.");
                } else {
                  log.warn("Our segments don't exist, giving up.");
                  return null;
                }
              }
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          return segmentsAndMetadata;
        },
        publishExecutor
    );
  }

  public ListenableFuture<SegmentsAndMetadata> publishAndRegisterHandoff(
      final TransactionalSegmentPublisher publisher,
      final Committer committer,
      final Collection<String> sequenceNames
  )
  {
    return Futures.transform(
        publish(publisher, committer, sequenceNames),
        this::registerHandoff
    );
  }

  private interface WrappedCommitter extends Committer
  {
  }

  private Supplier<Committer> wrapCommitterSupplier(final Supplier<Committer> committerSupplier)
  {
    return () -> wrapCommitter(committerSupplier.get());
  }

  private WrappedCommitter wrapCommitter(final Committer committer)
  {
    final SegmentTrackerMetadata wrappedMetadata = segmentTracker.wrapMetadata(committer.getMetadata());

    return new WrappedCommitter()
    {
      @Override
      public Object getMetadata()
      {
        return wrappedMetadata;
      }

      @Override
      public void run()
      {
        committer.run();
      }
    };
  }
}
