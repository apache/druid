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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Optional;

/**
 * {@link Segment} that is also a {@link ReferenceCountingSegment}, allowing query engines that operate directly on
 * segments to track references so that dropping a {@link Segment} can be done safely to ensure there are no in-flight
 * queries.
 *
 * Extensions can extend this class for populating {@link org.apache.druid.timeline.VersionedIntervalTimeline} with
 * a custom implementation through SegmentLoader.
 */
public class ReferenceCountingSegment extends ReferenceCountingCloseableObject<Segment>
    implements SegmentReference, Overshadowable<ReferenceCountingSegment>
{
  private final short startRootPartitionId;
  private final short endRootPartitionId;
  private final short minorVersion;
  private final short atomicUpdateGroupSize;

  public static ReferenceCountingSegment wrapRootGenerationSegment(Segment baseSegment)
  {
    return new ReferenceCountingSegment(
        Preconditions.checkNotNull(baseSegment, "baseSegment"),
        baseSegment.getId().getPartitionNum(),
        (baseSegment.getId().getPartitionNum() + 1),
        (short) 0,
        (short) 1
    );
  }

  public static ReferenceCountingSegment wrapSegment(
      Segment baseSegment,
      ShardSpec shardSpec
  )
  {
    return new ReferenceCountingSegment(
        baseSegment,
        shardSpec.getStartRootPartitionId(),
        shardSpec.getEndRootPartitionId(),
        shardSpec.getMinorVersion(),
        shardSpec.getAtomicUpdateGroupSize()
    );
  }

  protected ReferenceCountingSegment(
      Segment baseSegment,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      short atomicUpdateGroupSize
  )
  {
    super(baseSegment);
    this.startRootPartitionId = (short) startRootPartitionId;
    this.endRootPartitionId = (short) endRootPartitionId;
    this.minorVersion = minorVersion;
    this.atomicUpdateGroupSize = atomicUpdateGroupSize;
  }

  @Nullable
  public Segment getBaseSegment()
  {
    return !isClosed() ? baseObject : null;
  }

  @Override
  @Nullable
  public SegmentId getId()
  {
    return !isClosed() ? baseObject.getId() : null;
  }

  @Override
  @Nullable
  public Interval getDataInterval()
  {
    return !isClosed() ? baseObject.getDataInterval() : null;
  }

  @Override
  @Nullable
  public QueryableIndex asQueryableIndex()
  {
    return !isClosed() ? baseObject.asQueryableIndex() : null;
  }

  @Override
  @Nullable
  public StorageAdapter asStorageAdapter()
  {
    return !isClosed() ? baseObject.asStorageAdapter() : null;
  }

  @Override
  public boolean overshadows(ReferenceCountingSegment other)
  {
    if (baseObject.getId().getDataSource().equals(other.baseObject.getId().getDataSource())
        && baseObject.getId().getInterval().overlaps(other.baseObject.getId().getInterval())) {
      final int majorVersionCompare = baseObject.getId().getVersion().compareTo(other.baseObject.getId().getVersion());
      if (majorVersionCompare > 0) {
        return true;
      } else if (majorVersionCompare == 0) {
        return includeRootPartitions(other) && getMinorVersion() > other.getMinorVersion();
      }
    }
    return false;
  }

  private boolean includeRootPartitions(ReferenceCountingSegment other)
  {
    return startRootPartitionId <= other.startRootPartitionId
           && endRootPartitionId >= other.endRootPartitionId;
  }

  @Override
  public int getStartRootPartitionId()
  {
    return startRootPartitionId;
  }

  @Override
  public int getEndRootPartitionId()
  {
    return endRootPartitionId;
  }

  @Override
  public String getVersion()
  {
    return baseObject.getId().getVersion();
  }

  @Override
  public short getMinorVersion()
  {
    return minorVersion;
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return atomicUpdateGroupSize;
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return incrementReferenceAndDecrementOnceCloseable();
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    if (isClosed()) {
      return null;
    }
    return baseObject.as(clazz);
  }

  @Override
  public String asString()
  {
    return baseObject.asString();
  }
}
