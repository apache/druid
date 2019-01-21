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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.realtime.appenderator.SegmentWithState.SegmentState;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A BaseAppenderatorDriver drives an Appenderator to index a finite stream of data. This class does not help you
 * index unbounded streams. All handoff is done at the end of indexing.
 * <p/>
 * This class helps with doing things that Appenderators don't, including deciding which segments to use (with a
 * SegmentAllocator), publishing segments to the metadata store (with a SegmentPublisher).
 * <p/>
 * This class has two child classes, i.e., {@link BatchAppenderatorDriver} and {@link StreamAppenderatorDriver},
 * which are for batch and streaming ingestion, respectively. This class provides some fundamental methods for making
 * the child classes' life easier like {@link #pushInBackground}, {@link #dropInBackground}, or
 * {@link #publishInBackground}. The child classes can use these methods to achieve their goal.
 * <p/>
 * Note that the commit metadata stored by this class via the underlying Appenderator is not the same metadata as
 * you pass in. It's wrapped in some extra metadata needed by the driver.
 */
public abstract class BaseAppenderatorDriver implements Closeable
{
  /**
   * Segments allocated for an intervval.
   * There should be at most a single active (appending) segment at any time.
   */
  static class SegmentsOfInterval
  {
    private final Interval interval;
    private final List<SegmentWithState> appendFinishedSegments = new ArrayList<>();

    @Nullable
    private SegmentWithState appendingSegment;

    SegmentsOfInterval(Interval interval)
    {
      this.interval = interval;
    }

    SegmentsOfInterval(
        Interval interval,
        @Nullable SegmentWithState appendingSegment,
        List<SegmentWithState> appendFinishedSegments
    )
    {
      this.interval = interval;
      this.appendingSegment = appendingSegment;
      this.appendFinishedSegments.addAll(appendFinishedSegments);

      if (appendingSegment != null) {
        Preconditions.checkArgument(
            appendingSegment.getState() == SegmentState.APPENDING,
            "appendingSegment[%s] is not in the APPENDING state",
            appendingSegment.getSegmentIdentifier()
        );
      }
      if (appendFinishedSegments
          .stream()
          .anyMatch(segmentWithState -> segmentWithState.getState() == SegmentState.APPENDING)) {
        throw new ISE("Some appendFinishedSegments[%s] is in the APPENDING state", appendFinishedSegments);
      }
    }

    void setAppendingSegment(SegmentWithState appendingSegment)
    {
      Preconditions.checkArgument(
          appendingSegment.getState() == SegmentState.APPENDING,
          "segment[%s] is not in the APPENDING state",
          appendingSegment.getSegmentIdentifier()
      );
      // There should be only one appending segment at any time
      Preconditions.checkState(
          this.appendingSegment == null,
          "WTF?! Current appendingSegment[%s] is not null. "
          + "Its state must be changed before setting a new appendingSegment[%s]",
          this.appendingSegment,
          appendingSegment
      );
      this.appendingSegment = appendingSegment;
    }

    void finishAppendingToCurrentActiveSegment(Consumer<SegmentWithState> stateTransitionFn)
    {
      Preconditions.checkNotNull(appendingSegment, "appendingSegment");
      stateTransitionFn.accept(appendingSegment);
      appendFinishedSegments.add(appendingSegment);
      appendingSegment = null;
    }

    Interval getInterval()
    {
      return interval;
    }

    SegmentWithState getAppendingSegment()
    {
      return appendingSegment;
    }

    List<SegmentWithState> getAllSegments()
    {
      final List<SegmentWithState> allSegments = new ArrayList<>(appendFinishedSegments.size() + 1);
      if (appendingSegment != null) {
        allSegments.add(appendingSegment);
      }
      allSegments.addAll(appendFinishedSegments);
      return allSegments;
    }
  }

  /**
   * Allocated segments for a sequence
   */
  static class SegmentsForSequence
  {
    // Interval Start millis -> List of Segments for this interval
    // there might be multiple segments for a start interval, for example one segment
    // can be in APPENDING state and others might be in PUBLISHING state
    private final NavigableMap<Long, SegmentsOfInterval> intervalToSegmentStates;

    // most recently allocated segment
    private String lastSegmentId;

    SegmentsForSequence()
    {
      this.intervalToSegmentStates = new TreeMap<>();
    }

    SegmentsForSequence(
        NavigableMap<Long, SegmentsOfInterval> intervalToSegmentStates,
        String lastSegmentId
    )
    {
      this.intervalToSegmentStates = intervalToSegmentStates;
      this.lastSegmentId = lastSegmentId;
    }

    void add(SegmentIdWithShardSpec identifier)
    {
      intervalToSegmentStates.computeIfAbsent(
          identifier.getInterval().getStartMillis(),
          k -> new SegmentsOfInterval(identifier.getInterval())
      ).setAppendingSegment(SegmentWithState.newSegment(identifier));
      lastSegmentId = identifier.toString();
    }

    Entry<Long, SegmentsOfInterval> floor(long timestamp)
    {
      return intervalToSegmentStates.floorEntry(timestamp);
    }

    SegmentsOfInterval get(long timestamp)
    {
      return intervalToSegmentStates.get(timestamp);
    }

    Stream<SegmentWithState> allSegmentStateStream()
    {
      return intervalToSegmentStates
          .values()
          .stream()
          .flatMap(segmentsOfInterval -> segmentsOfInterval.getAllSegments().stream());
    }

    Stream<SegmentsOfInterval> getAllSegmentsOfInterval()
    {
      return intervalToSegmentStates.values().stream();
    }
  }

  private static final Logger log = new Logger(BaseAppenderatorDriver.class);

  private final SegmentAllocator segmentAllocator;
  private final UsedSegmentChecker usedSegmentChecker;
  private final DataSegmentKiller dataSegmentKiller;

  protected final Appenderator appenderator;
  // sequenceName -> segmentsForSequence
  // This map should be locked with itself before accessing it.
  // Note: BatchAppenderatorDriver currently doesn't need to lock this map because it doesn't do anything concurrently.
  // However, it's desired to do some operations like indexing and pushing at the same time. Locking this map is also
  // required in BatchAppenderatorDriver once this feature is supported.
  protected final Map<String, SegmentsForSequence> segments = new TreeMap<>();
  protected final ListeningExecutorService executor;

  BaseAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      UsedSegmentChecker usedSegmentChecker,
      DataSegmentKiller dataSegmentKiller
  )
  {
    this.appenderator = Preconditions.checkNotNull(appenderator, "appenderator");
    this.segmentAllocator = Preconditions.checkNotNull(segmentAllocator, "segmentAllocator");
    this.usedSegmentChecker = Preconditions.checkNotNull(usedSegmentChecker, "usedSegmentChecker");
    this.dataSegmentKiller = Preconditions.checkNotNull(dataSegmentKiller, "dataSegmentKiller");
    this.executor = MoreExecutors.listeningDecorator(Execs.singleThreaded("publish-%d"));
  }

  @VisibleForTesting
  Map<String, SegmentsForSequence> getSegments()
  {
    return segments;
  }

  /**
   * Perform any initial setup and return currently persisted commit metadata.
   * <p>
   * Note that this method returns the same metadata you've passed in with your Committers, even though this class
   * stores extra metadata on disk.
   *
   * @return currently persisted commit metadata
   */
  @Nullable
  public abstract Object startJob();

  /**
   * Find a segment in the {@link SegmentState#APPENDING} state for the given timestamp and sequenceName.
   */
  private SegmentIdWithShardSpec getAppendableSegment(final DateTime timestamp, final String sequenceName)
  {
    synchronized (segments) {
      final SegmentsForSequence segmentsForSequence = segments.get(sequenceName);

      if (segmentsForSequence == null) {
        return null;
      }

      final Map.Entry<Long, SegmentsOfInterval> candidateEntry = segmentsForSequence.floor(
          timestamp.getMillis()
      );

      if (candidateEntry != null) {
        final SegmentsOfInterval segmentsOfInterval = candidateEntry.getValue();
        if (segmentsOfInterval.interval.contains(timestamp)) {
          return segmentsOfInterval.appendingSegment == null ?
                 null :
                 segmentsOfInterval.appendingSegment.getSegmentIdentifier();
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  /**
   * Return a segment usable for "timestamp". May return null if no segment can be allocated.
   *
   * @param row                     input row
   * @param sequenceName            sequenceName for potential segment allocation
   * @param skipSegmentLineageCheck if false, perform lineage validation using previousSegmentId for this sequence.
   *                                Should be set to false if replica tasks would index events in same order
   *
   * @return identifier, or null
   *
   * @throws IOException if an exception occurs while allocating a segment
   */
  private SegmentIdWithShardSpec getSegment(
      final InputRow row,
      final String sequenceName,
      final boolean skipSegmentLineageCheck
  ) throws IOException
  {
    synchronized (segments) {
      final DateTime timestamp = row.getTimestamp();
      final SegmentIdWithShardSpec existing = getAppendableSegment(timestamp, sequenceName);
      if (existing != null) {
        return existing;
      } else {
        // Allocate new segment.
        final SegmentsForSequence segmentsForSequence = segments.get(sequenceName);
        final SegmentIdWithShardSpec newSegment = segmentAllocator.allocate(
            row,
            sequenceName,
            segmentsForSequence == null ? null : segmentsForSequence.lastSegmentId,
            // send lastSegmentId irrespective of skipSegmentLineageCheck so that
            // unique constraint for sequence_name_prev_id_sha1 does not fail for
            // allocatePendingSegment in IndexerSQLMetadataStorageCoordinator
            skipSegmentLineageCheck
        );

        if (newSegment != null) {
          for (SegmentIdWithShardSpec identifier : appenderator.getSegments()) {
            if (identifier.equals(newSegment)) {
              throw new ISE(
                  "WTF?! Allocated segment[%s] which conflicts with existing segment[%s].",
                  newSegment,
                  identifier
              );
            }
          }

          log.info("New segment[%s] for row[%s] sequenceName[%s].", newSegment, row, sequenceName);
          addSegment(sequenceName, newSegment);
        } else {
          // Well, we tried.
          log.warn("Cannot allocate segment for timestamp[%s], sequenceName[%s]. ", timestamp, sequenceName);
        }

        return newSegment;
      }
    }
  }

  private void addSegment(String sequenceName, SegmentIdWithShardSpec identifier)
  {
    synchronized (segments) {
      segments.computeIfAbsent(sequenceName, k -> new SegmentsForSequence())
              .add(identifier);
    }
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
  protected AppenderatorDriverAddResult append(
      final InputRow row,
      final String sequenceName,
      @Nullable final Supplier<Committer> committerSupplier,
      final boolean skipSegmentLineageCheck,
      final boolean allowIncrementalPersists
  ) throws IOException
  {
    Preconditions.checkNotNull(row, "row");
    Preconditions.checkNotNull(sequenceName, "sequenceName");

    final SegmentIdWithShardSpec identifier = getSegment(row, sequenceName, skipSegmentLineageCheck);

    if (identifier != null) {
      try {
        final Appenderator.AppenderatorAddResult result = appenderator.add(
            identifier,
            row,
            committerSupplier == null ? null : wrapCommitterSupplier(committerSupplier),
            allowIncrementalPersists
        );
        return AppenderatorDriverAddResult.ok(
            identifier,
            result.getNumRowsInSegment(),
            appenderator.getTotalRowCount(),
            result.isPersistRequired(),
            result.getParseException()
        );
      }
      catch (SegmentNotWritableException e) {
        throw new ISE(e, "WTF?! Segment[%s] not writable when it should have been.", identifier);
      }
    } else {
      return AppenderatorDriverAddResult.fail();
    }
  }

  /**
   * Returns a stream of {@link SegmentWithState} for the given sequenceNames.
   */
  Stream<SegmentWithState> getSegmentWithStates(Collection<String> sequenceNames)
  {
    synchronized (segments) {
      return sequenceNames
          .stream()
          .map(segments::get)
          .filter(Objects::nonNull)
          .flatMap(segmentsForSequence -> segmentsForSequence.intervalToSegmentStates.values().stream())
          .flatMap(segmentsOfInterval -> segmentsOfInterval.getAllSegments().stream());
    }
  }

  Stream<SegmentWithState> getAppendingSegments(Collection<String> sequenceNames)
  {
    synchronized (segments) {
      return sequenceNames
          .stream()
          .map(segments::get)
          .filter(Objects::nonNull)
          .flatMap(segmentsForSequence -> segmentsForSequence.intervalToSegmentStates.values().stream())
          .map(segmentsOfInterval -> segmentsOfInterval.appendingSegment)
          .filter(Objects::nonNull);
    }
  }

  /**
   * Push the given segments in background.
   *
   * @param wrappedCommitter   should not be null if you want to persist intermediate states
   * @param segmentIdentifiers identifiers of the segments to be pushed
   * @param useUniquePath      true if the segment should be written to a path with a unique identifier
   *
   * @return a future for pushing segments
   */
  ListenableFuture<SegmentsAndMetadata> pushInBackground(
      @Nullable final WrappedCommitter wrappedCommitter,
      final Collection<SegmentIdWithShardSpec> segmentIdentifiers,
      final boolean useUniquePath
  )
  {
    log.info("Pushing segments in background: [%s]", Joiner.on(", ").join(segmentIdentifiers));

    return Futures.transform(
        appenderator.push(segmentIdentifiers, wrappedCommitter, useUniquePath),
        (Function<SegmentsAndMetadata, SegmentsAndMetadata>) segmentsAndMetadata -> {
          // Sanity check
          final Set<SegmentIdWithShardSpec> pushedSegments = segmentsAndMetadata.getSegments().stream()
                                                                                .map(
                                                                                    SegmentIdWithShardSpec::fromDataSegment)
                                                                                .collect(Collectors.toSet());
          if (!pushedSegments.equals(Sets.newHashSet(segmentIdentifiers))) {
            log.warn(
                "Removing segments from deep storage because sanity check failed: %s", segmentsAndMetadata.getSegments()
            );

            segmentsAndMetadata.getSegments().forEach(dataSegmentKiller::killQuietly);

            throw new ISE(
                "WTF?! Pushed different segments than requested. Pushed[%s], requested[%s].",
                pushedSegments,
                segmentIdentifiers
            );
          }

          return segmentsAndMetadata;
        },
        executor
    );
  }

  /**
   * Drop segments in background. The segments should be pushed (in batch ingestion) or published (in streaming
   * ingestion) before being dropped.
   *
   * @param segmentsAndMetadata result of pushing or publishing
   *
   * @return a future for dropping segments
   */
  ListenableFuture<SegmentsAndMetadata> dropInBackground(SegmentsAndMetadata segmentsAndMetadata)
  {
    log.info("Dropping segments[%s]", segmentsAndMetadata.getSegments());
    final ListenableFuture<?> dropFuture = Futures.allAsList(
        segmentsAndMetadata
            .getSegments()
            .stream()
            .map(segment -> appenderator.drop(SegmentIdWithShardSpec.fromDataSegment(segment)))
            .collect(Collectors.toList())
    );

    return Futures.transform(
        dropFuture,
        (Function<Object, SegmentsAndMetadata>) x -> {
          final Object metadata = segmentsAndMetadata.getCommitMetadata();
          return new SegmentsAndMetadata(
              segmentsAndMetadata.getSegments(),
              metadata == null ? null : ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
          );
        }
    );
  }

  /**
   * Publish segments in background. The segments should be dropped (in batch ingestion) or pushed (in streaming
   * ingestion) before being published.
   *
   * @param segmentsAndMetadata result of dropping or pushing
   * @param publisher           transactional segment publisher
   *
   * @return a future for publishing segments
   */
  ListenableFuture<SegmentsAndMetadata> publishInBackground(
      SegmentsAndMetadata segmentsAndMetadata,
      TransactionalSegmentPublisher publisher
  )
  {
    return executor.submit(
        () -> {
          if (segmentsAndMetadata.getSegments().isEmpty()) {
            log.info("Nothing to publish, skipping publish step.");
          } else {
            log.info(
                "Publishing segments with commitMetadata[%s]: [%s]",
                segmentsAndMetadata.getCommitMetadata(),
                Joiner.on(", ").join(segmentsAndMetadata.getSegments())
            );

            try {
              final Object metadata = segmentsAndMetadata.getCommitMetadata();
              final boolean published = publisher.publishSegments(
                  ImmutableSet.copyOf(segmentsAndMetadata.getSegments()),
                  metadata == null ? null : ((AppenderatorDriverMetadata) metadata).getCallerMetadata()
              ).isSuccess();

              if (published) {
                log.info("Published segments.");
              } else {
                log.info("Transaction failure while publishing segments, removing them from deep storage "
                         + "and checking if someone else beat us to publishing.");

                segmentsAndMetadata.getSegments().forEach(dataSegmentKiller::killQuietly);

                final Set<SegmentIdWithShardSpec> segmentsIdentifiers = segmentsAndMetadata
                    .getSegments()
                    .stream()
                    .map(SegmentIdWithShardSpec::fromDataSegment)
                    .collect(Collectors.toSet());

                if (usedSegmentChecker.findUsedSegments(segmentsIdentifiers)
                                      .equals(Sets.newHashSet(segmentsAndMetadata.getSegments()))) {
                  log.info("Our segments really do exist, awaiting handoff.");
                } else {
                  throw new ISE("Failed to publish segments.");
                }
              }
            }
            catch (Exception e) {
              // Must not remove segments here, we aren't sure if our transaction succeeded or not.
              log.warn(e, "Failed publish, not removing segments: %s", segmentsAndMetadata.getSegments());
              throw Throwables.propagate(e);
            }
          }

          return segmentsAndMetadata;
        }
    );
  }

  /**
   * Clears out all our state and also calls {@link Appenderator#clear()} on the underlying Appenderator.
   */
  public void clear() throws InterruptedException
  {
    synchronized (segments) {
      segments.clear();
    }
    appenderator.clear();
  }

  /**
   * Closes this driver. Does not close the underlying Appenderator; you should do that yourself.
   */
  @Override
  public void close()
  {
    executor.shutdownNow();
  }

  /**
   * Wrapped committer for BaseAppenderatorDriver. Used in only {@link StreamAppenderatorDriver} because batch ingestion
   * doesn't need committing intermediate states.
   */
  static class WrappedCommitter implements Committer
  {
    private final Committer delegate;
    private final AppenderatorDriverMetadata metadata;

    WrappedCommitter(Committer delegate, AppenderatorDriverMetadata metadata)
    {
      this.delegate = delegate;
      this.metadata = metadata;
    }

    @Override
    public Object getMetadata()
    {
      return metadata;
    }

    @Override
    public void run()
    {
      delegate.run();
    }
  }

  WrappedCommitter wrapCommitter(final Committer committer)
  {
    final AppenderatorDriverMetadata wrappedMetadata;
    final Map<String, SegmentsForSequence> snapshot;
    synchronized (segments) {
      snapshot = ImmutableMap.copyOf(segments);
    }

    wrappedMetadata = new AppenderatorDriverMetadata(
        ImmutableMap.copyOf(
            Maps.transformValues(
                snapshot,
                (Function<SegmentsForSequence, List<SegmentWithState>>) input -> ImmutableList.copyOf(
                    input.intervalToSegmentStates
                        .values()
                        .stream()
                        .flatMap(segmentsOfInterval -> segmentsOfInterval.getAllSegments().stream())
                        .collect(Collectors.toList())
                )
            )
        ),
        snapshot.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Entry::getKey,
                        e -> e.getValue().lastSegmentId
                    )
                ),
        committer.getMetadata()
    );

    return new WrappedCommitter(committer, wrappedMetadata);
  }

  private Supplier<Committer> wrapCommitterSupplier(final Supplier<Committer> committerSupplier)
  {
    return () -> wrapCommitter(committerSupplier.get());
  }
}
