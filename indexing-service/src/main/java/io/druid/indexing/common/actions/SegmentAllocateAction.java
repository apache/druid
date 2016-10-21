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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.java.util.common.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Allocates a pending segment for a given timestamp. The preferredSegmentGranularity is used if there are no prior
 * segments for the given timestamp, or if the prior segments for the given timestamp are already at the
 * preferredSegmentGranularity. Otherwise, the prior segments will take precedence.
 * <p/>
 * This action implicitly acquires locks when it allocates segments. You do not have to acquire them beforehand,
 * although you *do* have to release them yourself.
 * <p/>
 * If this action cannot acquire an appropriate lock, or if it cannot expand an existing segment set, it returns null.
 */
public class SegmentAllocateAction implements TaskAction<SegmentIdentifier>
{
  private static final Logger log = new Logger(SegmentAllocateAction.class);

  // Prevent spinning forever in situations where the segment list just won't stop changing.
  private static final int MAX_ATTEMPTS = 90;

  private final String dataSource;
  private final DateTime timestamp;
  private final QueryGranularity queryGranularity;
  private final Granularity preferredSegmentGranularity;
  private final String sequenceName;
  private final String previousSegmentId;

  public static List<Granularity> granularitiesFinerThan(final Granularity gran0)
  {
    final DateTime epoch = new DateTime(0);
    final List<Granularity> retVal = Lists.newArrayList();
    for (Granularity gran : Granularity.values()) {
      if (gran.bucket(epoch).toDurationMillis() <= gran0.bucket(epoch).toDurationMillis()) {
        retVal.add(gran);
      }
    }
    Collections.sort(
        retVal,
        new Comparator<Granularity>()
        {
          @Override
          public int compare(Granularity g1, Granularity g2)
          {
            return Longs.compare(g2.bucket(epoch).toDurationMillis(), g1.bucket(epoch).toDurationMillis());
          }
        }
    );
    return retVal;
  }

  public SegmentAllocateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity,
      @JsonProperty("preferredSegmentGranularity") Granularity preferredSegmentGranularity,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("previousSegmentId") String previousSegmentId
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.queryGranularity = Preconditions.checkNotNull(queryGranularity, "queryGranularity");
    this.preferredSegmentGranularity = Preconditions.checkNotNull(
        preferredSegmentGranularity,
        "preferredSegmentGranularity"
    );
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
    this.previousSegmentId = previousSegmentId;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Granularity getPreferredSegmentGranularity()
  {
    return preferredSegmentGranularity;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  @Override
  public TypeReference<SegmentIdentifier> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdentifier>()
    {
    };
  }

  @Override
  public SegmentIdentifier perform(
      final Task task,
      final TaskActionToolbox toolbox
  ) throws IOException
  {
    int attempt = 0;
    while (true) {
      attempt++;

      if (!task.getDataSource().equals(dataSource)) {
        throw new IAE("Task dataSource must match action dataSource, [%s] != [%s].", task.getDataSource(), dataSource);
      }

      final IndexerMetadataStorageCoordinator msc = toolbox.getIndexerMetadataStorageCoordinator();

      // 1) if something overlaps our timestamp, use that
      // 2) otherwise try preferredSegmentGranularity & going progressively smaller

      final List<Interval> tryIntervals = Lists.newArrayList();

      final Interval rowInterval = new Interval(
          queryGranularity.truncate(timestamp.getMillis()),
          queryGranularity.next(queryGranularity.truncate(timestamp.getMillis()))
      );

      final Set<DataSegment> usedSegmentsForRow = ImmutableSet.copyOf(
          msc.getUsedSegmentsForInterval(dataSource, rowInterval)
      );

      if (usedSegmentsForRow.isEmpty()) {
        // No existing segments for this row, but there might still be nearby ones that conflict with our preferred
        // segment granularity. Try that first, and then progressively smaller ones if it fails.
        for (Granularity gran : granularitiesFinerThan(preferredSegmentGranularity)) {
          tryIntervals.add(gran.bucket(timestamp));
        }
      } else {
        // Existing segment(s) exist for this row; use the interval of the first one.
        tryIntervals.add(usedSegmentsForRow.iterator().next().getInterval());
      }

      for (final Interval tryInterval : tryIntervals) {
        if (tryInterval.contains(rowInterval)) {
          log.debug(
              "Trying to allocate pending segment for rowInterval[%s], segmentInterval[%s].",
              rowInterval,
              tryInterval
          );
          final TaskLock tryLock = toolbox.getTaskLockbox().tryLock(task, tryInterval).orNull();
          if (tryLock != null) {
            final SegmentIdentifier identifier = msc.allocatePendingSegment(
                dataSource,
                sequenceName,
                previousSegmentId,
                tryInterval,
                tryLock.getVersion()
            );
            if (identifier != null) {
              return identifier;
            } else {
              log.debug(
                  "Could not allocate pending segment for rowInterval[%s], segmentInterval[%s].",
                  rowInterval,
                  tryInterval
              );
            }
          } else {
            log.debug("Could not acquire lock for rowInterval[%s], segmentInterval[%s].", rowInterval, tryInterval);
          }
        }
      }

      // Could not allocate a pending segment. There's a chance that this is because someone else inserted a segment
      // overlapping with this row between when we called "mdc.getUsedSegmentsForInterval" and now. Check it again,
      // and if it's different, repeat.

      if (!ImmutableSet.copyOf(msc.getUsedSegmentsForInterval(dataSource, rowInterval)).equals(usedSegmentsForRow)) {
        if (attempt < MAX_ATTEMPTS) {
          final long shortRandomSleep = 50 + (long) (Math.random() * 450);
          log.debug(
              "Used segment set changed for rowInterval[%s]. Retrying segment allocation in %,dms (attempt = %,d).",
              rowInterval,
              shortRandomSleep,
              attempt
          );
          try {
            Thread.sleep(shortRandomSleep);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
          }
        } else {
          log.error(
              "Used segment set changed for rowInterval[%s]. Not trying again (attempt = %,d).",
              rowInterval,
              attempt
          );
          return null;
        }
      } else {
        return null;
      }
    }
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentAllocateAction{" +
           "dataSource='" + dataSource + '\'' +
           ", timestamp=" + timestamp +
           ", queryGranularity=" + queryGranularity +
           ", preferredSegmentGranularity=" + preferredSegmentGranularity +
           ", sequenceName='" + sequenceName + '\'' +
           ", previousSegmentId='" + previousSegmentId + '\'' +
           '}';
  }
}
