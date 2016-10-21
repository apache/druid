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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * A FiniteAppenderatorDriver drives an Appenderator to index a finite stream of data. This class does not help you
 * index unbounded streams. All handoff is done at the end of indexing.
 *
 * This class helps with doing things that Appenderators don't, including deciding which segments to use (with a
 * SegmentAllocator), publishing segments to the metadata store (with a SegmentPublisher), and monitoring handoff (with
 * a SegmentHandoffNotifier).
 *
 * Note that the commit metadata stored by this class via the underlying Appenderator is not the same metadata as
 * you pass in. It's wrapped in some extra metadata needed by the driver.
 */
public class FiniteAppenderatorDriver implements Closeable
{
  private static final Logger log = new Logger(FiniteAppenderatorDriver.class);

  private final Appenderator appenderator;
  private final SegmentAllocator segmentAllocator;
  private final SegmentHandoffNotifier handoffNotifier;
  private final UsedSegmentChecker usedSegmentChecker;
  private final ObjectMapper objectMapper;
  private final int maxRowsPerSegment;
  private final long handoffConditionTimeout;

  // All access to "activeSegments" and "lastSegmentId" must be synchronized on "activeSegments".

  // sequenceName -> start of segment interval -> segment we're currently adding data to
  private final Map<String, NavigableMap<Long, SegmentIdentifier>> activeSegments = new TreeMap<>();

  // sequenceName -> most recently allocated segment
  private final Map<String, String> lastSegmentIds = Maps.newHashMap();

  // Notified when segments are dropped.
  private final Object handoffMonitor = new Object();

  /**
   * Create a driver.
   *
   * @param appenderator            appenderator
   * @param segmentAllocator        segment allocator
   * @param handoffNotifierFactory  handoff notifier factory
   * @param usedSegmentChecker      used segment checker
   * @param objectMapper            object mapper, used for serde of commit metadata
   * @param maxRowsPerSegment       maximum number of rows allowed in an entire segment (not a single persist)
   * @param handoffConditionTimeout maximum number of millis allowed for handoff (not counting push/publish), zero
   *                                means wait forever.
   */
  public FiniteAppenderatorDriver(
      Appenderator appenderator,
      SegmentAllocator segmentAllocator,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      UsedSegmentChecker usedSegmentChecker,
      ObjectMapper objectMapper,
      int maxRowsPerSegment,
      long handoffConditionTimeout
  )
  {
    this.appenderator = Preconditions.checkNotNull(appenderator, "appenderator");
    this.segmentAllocator = Preconditions.checkNotNull(segmentAllocator, "segmentAllocator");
    this.handoffNotifier = Preconditions.checkNotNull(handoffNotifierFactory, "handoffNotifierFactory")
                                        .createSegmentHandoffNotifier(appenderator.getDataSource());
    this.usedSegmentChecker = Preconditions.checkNotNull(usedSegmentChecker, "usedSegmentChecker");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.handoffConditionTimeout = handoffConditionTimeout;
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

    final FiniteAppenderatorDriverMetadata metadata = objectMapper.convertValue(
        appenderator.startJob(),
        FiniteAppenderatorDriverMetadata.class
    );

    log.info("Restored metadata[%s].", metadata);

    if (metadata != null) {
      synchronized (activeSegments) {
        for (Map.Entry<String, List<SegmentIdentifier>> entry : metadata.getActiveSegments().entrySet()) {
          final String sequenceName = entry.getKey();
          final TreeMap<Long, SegmentIdentifier> segmentMap = Maps.newTreeMap();

          lastSegmentIds.put(sequenceName, metadata.getLastSegmentIds().get(sequenceName));
          activeSegments.put(sequenceName, segmentMap);

          for (SegmentIdentifier identifier : entry.getValue()) {
            segmentMap.put(identifier.getInterval().getStartMillis(), identifier);
          }
        }
      }

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
    synchronized (activeSegments) {
      activeSegments.clear();
    }
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
  public SegmentIdentifier add(
      final InputRow row,
      final String sequenceName,
      final Supplier<Committer> committerSupplier
  ) throws IOException
  {
    Preconditions.checkNotNull(row, "row");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(committerSupplier, "committerSupplier");

    final SegmentIdentifier identifier = getSegment(row.getTimestamp(), sequenceName);

    if (identifier != null) {
      try {
        final int numRows = appenderator.add(identifier, row, wrapCommitterSupplier(committerSupplier));
        if (numRows >= maxRowsPerSegment) {
          moveSegmentOut(sequenceName, ImmutableList.of(identifier));
        }
      }
      catch (SegmentNotWritableException e) {
        throw new ISE(e, "WTF?! Segment[%s] not writable when it should have been.", identifier);
      }
    }

    return identifier;
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
   * Publish all data indexed through this driver so far, and waits for it to be handed off. Blocks until complete.
   * Retries forever on transient failures, but may exit early on permanent failures.
   *
   * Should be called after all data has been added and persisted through {@link #add(InputRow, String, Supplier)} and
   * {@link #persist(Committer)}.
   *
   * @param publisher publisher to use for this set of segments
   * @param committer committer representing all data that has been added so far
   *
   * @return segments and metadata published if successful, or null if segments could not be handed off due to
   * transaction failure with commit metadata.
   */
  public SegmentsAndMetadata finish(
      final TransactionalSegmentPublisher publisher,
      final Committer committer
  ) throws InterruptedException
  {
    final SegmentsAndMetadata segmentsAndMetadata = publishAll(publisher, wrapCommitter(committer));

    if (segmentsAndMetadata != null) {
      final long giveUpAt = handoffConditionTimeout > 0
                            ? System.currentTimeMillis() + handoffConditionTimeout
                            : 0;

      log.info("Awaiting handoff of segments: [%s]", Joiner.on(", ").join(appenderator.getSegments()));

      synchronized (handoffMonitor) {
        while (!appenderator.getSegments().isEmpty()) {

          if (giveUpAt == 0) {
            handoffMonitor.wait();
          } else {
            final long remaining = giveUpAt - System.currentTimeMillis();
            if (remaining > 0) {
              handoffMonitor.wait(remaining);
            } else {
              throw new ISE(
                  "Segment handoff wait timeout. Segments not yet handed off: [%s]",
                  Joiner.on(", ").join(appenderator.getSegments())
              );
            }
          }
        }
      }

      log.info("All segments handed off.");

      return new SegmentsAndMetadata(
          segmentsAndMetadata.getSegments(),
          ((FiniteAppenderatorDriverMetadata) segmentsAndMetadata.getCommitMetadata()).getCallerMetadata()
      );
    } else {
      return null;
    }
  }

  /**
   * Closes this driver. Does not close the underlying Appenderator; you should do that yourself.
   */
  @Override
  public void close()
  {
    handoffNotifier.close();
  }

  private SegmentIdentifier getActiveSegment(final DateTime timestamp, final String sequenceName)
  {
    synchronized (activeSegments) {
      final NavigableMap<Long, SegmentIdentifier> activeSegmentsForSequence = activeSegments.get(sequenceName);

      if (activeSegmentsForSequence == null) {
        return null;
      }

      final Map.Entry<Long, SegmentIdentifier> candidateEntry = activeSegmentsForSequence.floorEntry(timestamp.getMillis());
      if (candidateEntry != null && candidateEntry.getValue().getInterval().contains(timestamp)) {
        return candidateEntry.getValue();
      } else {
        return null;
      }
    }
  }

  /**
   * Return a segment usable for "timestamp". May return null if no segment can be allocated.
   *
   * @param timestamp    data timestamp
   * @param sequenceName sequenceName for potential segment allocation
   *
   * @return identifier, or null
   *
   * @throws IOException if an exception occurs while allocating a segment
   */
  private SegmentIdentifier getSegment(final DateTime timestamp, final String sequenceName) throws IOException
  {
    synchronized (activeSegments) {
      final SegmentIdentifier existing = getActiveSegment(timestamp, sequenceName);
      if (existing != null) {
        return existing;
      } else {
        // Allocate new segment.
        final NavigableMap<Long, SegmentIdentifier> activeSegmentsForSequence = activeSegments.get(sequenceName);
        final SegmentIdentifier newSegment = segmentAllocator.allocate(
            timestamp,
            sequenceName,
            lastSegmentIds.get(sequenceName)
        );

        if (newSegment != null) {
          final Long key = newSegment.getInterval().getStartMillis();

          for (SegmentIdentifier identifier : appenderator.getSegments()) {
            if (identifier.equals(newSegment)) {
              throw new ISE(
                  "WTF?! Allocated segment[%s] which conflicts with existing segment[%s].",
                  newSegment,
                  identifier
              );
            }
          }

          log.info("New segment[%s] for sequenceName[%s].", newSegment, sequenceName);
          if (activeSegmentsForSequence == null) {
            activeSegments.put(sequenceName, Maps.<Long, SegmentIdentifier>newTreeMap());
          }
          activeSegments.get(sequenceName).put(key, newSegment);
          lastSegmentIds.put(sequenceName, newSegment.getIdentifierAsString());
        } else {
          // Well, we tried.
          log.warn("Cannot allocate segment for timestamp[%s], sequenceName[%s]. ", timestamp, sequenceName);
        }

        return newSegment;
      }
    }
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   */
  private void moveSegmentOut(final String sequenceName, final List<SegmentIdentifier> identifiers)
  {
    synchronized (activeSegments) {
      final NavigableMap<Long, SegmentIdentifier> activeSegmentsForSequence = activeSegments.get(sequenceName);
      if (activeSegmentsForSequence == null) {
        throw new ISE("WTF?! Asked to remove segments for sequenceName[%s] which doesn't exist...", sequenceName);
      }

      for (final SegmentIdentifier identifier : identifiers) {
        log.info("Moving segment[%s] out of active list.", identifier);
        final long key = identifier.getInterval().getStartMillis();
        if (activeSegmentsForSequence.remove(key) != identifier) {
          throw new ISE("WTF?! Asked to remove segment[%s] that didn't exist...", identifier);
        }
      }
    }
  }

  /**
   * Push and publish all segments to the metadata store.
   *
   * @param publisher        segment publisher
   * @param wrappedCommitter wrapped committer (from wrapCommitter)
   *
   * @return published segments and metadata, or null if segments could not be published due to transaction failure
   * with commit metadata.
   */
  private SegmentsAndMetadata publishAll(
      final TransactionalSegmentPublisher publisher,
      final Committer wrappedCommitter
  ) throws InterruptedException
  {
    final List<SegmentIdentifier> theSegments = ImmutableList.copyOf(appenderator.getSegments());

    long nTry = 0;
    while (true) {
      try {
        log.info("Pushing segments: [%s]", Joiner.on(", ").join(theSegments));
        final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(theSegments, wrappedCommitter).get();

        // Sanity check
        if (!segmentsToIdentifiers(segmentsAndMetadata.getSegments()).equals(Sets.newHashSet(theSegments))) {
          throw new ISE(
              "WTF?! Pushed different segments than requested. Pushed[%s], requested[%s].",
              Joiner.on(", ").join(identifiersToStrings(segmentsToIdentifiers(segmentsAndMetadata.getSegments()))),
              Joiner.on(", ").join(identifiersToStrings(theSegments))
          );
        }

        log.info(
            "Publishing segments with commitMetadata[%s]: [%s]",
            segmentsAndMetadata.getCommitMetadata(),
            Joiner.on(", ").join(segmentsAndMetadata.getSegments())
        );

        if (segmentsAndMetadata.getSegments().isEmpty()) {
          log.info("Nothing to publish, skipping publish step.");
        } else {
          final boolean published = publisher.publishSegments(
              ImmutableSet.copyOf(segmentsAndMetadata.getSegments()),
              ((FiniteAppenderatorDriverMetadata) segmentsAndMetadata.getCommitMetadata()).getCallerMetadata()
          );

          if (published) {
            log.info("Published segments, awaiting handoff.");
          } else {
            log.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
            if (usedSegmentChecker.findUsedSegments(segmentsToIdentifiers(segmentsAndMetadata.getSegments()))
                                  .equals(Sets.newHashSet(segmentsAndMetadata.getSegments()))) {
              log.info("Our segments really do exist, awaiting handoff.");
            } else {
              log.warn("Our segments don't exist, giving up.");
              return null;
            }
          }
        }

        for (final DataSegment dataSegment : segmentsAndMetadata.getSegments()) {
          handoffNotifier.registerSegmentHandoffCallback(
              new SegmentDescriptor(
                  dataSegment.getInterval(),
                  dataSegment.getVersion(),
                  dataSegment.getShardSpec().getPartitionNum()
              ),
              MoreExecutors.sameThreadExecutor(),
              new Runnable()
              {
                @Override
                public void run()
                {
                  final SegmentIdentifier identifier = SegmentIdentifier.fromDataSegment(dataSegment);
                  log.info("Segment[%s] successfully handed off, dropping.", identifier);
                  final ListenableFuture<?> dropFuture = appenderator.drop(identifier);
                  Futures.addCallback(
                      dropFuture,
                      new FutureCallback<Object>()
                      {
                        @Override
                        public void onSuccess(Object result)
                        {
                          synchronized (handoffMonitor) {
                            handoffMonitor.notifyAll();
                          }
                        }

                        @Override
                        public void onFailure(Throwable e)
                        {
                          log.warn(e, "Failed to drop segment[%s]?!");
                          synchronized (handoffMonitor) {
                            handoffMonitor.notifyAll();
                          }
                        }
                      }
                  );
                }
              }
          );
        }

        return segmentsAndMetadata;
      }
      catch (InterruptedException e) {
        throw e;
      }
      catch (Exception e) {
        final long sleepMillis = computeNextRetrySleep(++nTry);
        log.warn(e, "Failed publishAll (try %d), retrying in %,dms.", nTry, sleepMillis);
        Thread.sleep(sleepMillis);
      }
    }
  }

  private Supplier<Committer> wrapCommitterSupplier(final Supplier<Committer> committerSupplier)
  {
    return new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        return wrapCommitter(committerSupplier.get());
      }
    };
  }

  private Committer wrapCommitter(final Committer committer)
  {
    synchronized (activeSegments) {
      final FiniteAppenderatorDriverMetadata wrappedMetadata = new FiniteAppenderatorDriverMetadata(
          ImmutableMap.copyOf(
              Maps.transformValues(
                  activeSegments,
                  new Function<NavigableMap<Long, SegmentIdentifier>, List<SegmentIdentifier>>()
                  {
                    @Override
                    public List<SegmentIdentifier> apply(NavigableMap<Long, SegmentIdentifier> input)
                    {
                      return ImmutableList.copyOf(input.values());
                    }
                  }
              )
          ),
          ImmutableMap.copyOf(lastSegmentIds),
          committer.getMetadata()
      );

      return new Committer()
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

  private static long computeNextRetrySleep(final long nTry)
  {
    final long baseSleepMillis = 1000;
    final long maxSleepMillis = 60000;
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * new Random().nextGaussian(), 0), 2);
    return (long) (Math.min(maxSleepMillis, baseSleepMillis * Math.pow(2, nTry)) * fuzzyMultiplier);
  }

  private static Set<SegmentIdentifier> segmentsToIdentifiers(final Iterable<DataSegment> segments)
  {
    return FluentIterable.from(segments)
                         .transform(
                             new Function<DataSegment, SegmentIdentifier>()
                             {
                               @Override
                               public SegmentIdentifier apply(DataSegment segment)
                               {
                                 return SegmentIdentifier.fromDataSegment(segment);
                               }
                             }
                         ).toSet();
  }

  private static Iterable<String> identifiersToStrings(final Iterable<SegmentIdentifier> identifiers)
  {
    return FluentIterable.from(identifiers)
                         .transform(
                             new Function<SegmentIdentifier, String>()
                             {
                               @Override
                               public String apply(SegmentIdentifier input)
                               {
                                 return input.getIdentifierAsString();
                               }
                             }
                         );
  }
}
