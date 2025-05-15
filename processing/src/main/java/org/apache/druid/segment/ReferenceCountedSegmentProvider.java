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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.partition.ShardSpec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * {@link ReferenceCountedSegmentProvider} wraps a {@link Segment}, allowing query engines that operate directly on segments
 * to track references so that dropping a {@link Segment} can be done safely to ensure there are no in-flight queries.
 * <p>
 * Extensions can extend this class for populating {@link org.apache.druid.timeline.VersionedIntervalTimeline} with
 * a custom implementation through SegmentLoader.
 */
public class ReferenceCountedSegmentProvider extends ReferenceCountingCloseableObject<Segment>
    implements Overshadowable<ReferenceCountedSegmentProvider>, ReferenceCountedObjectProvider<Segment>
{
  private final short startRootPartitionId;
  private final short endRootPartitionId;
  private final short minorVersion;
  private final short atomicUpdateGroupSize;

  public static ReferenceCountedSegmentProvider wrapRootGenerationSegment(Segment baseSegment)
  {
    int partitionNum = baseSegment.getId() == null ? 0 : baseSegment.getId().getPartitionNum();
    return new ReferenceCountedSegmentProvider(
        Preconditions.checkNotNull(baseSegment, "baseSegment"),
        partitionNum,
        partitionNum + 1,
        (short) 0,
        (short) 1
    );
  }

  public static ReferenceCountedSegmentProvider wrapSegment(
      Segment baseSegment,
      ShardSpec shardSpec
  )
  {
    return new ReferenceCountedSegmentProvider(
        baseSegment,
        shardSpec.getStartRootPartitionId(),
        shardSpec.getEndRootPartitionId(),
        shardSpec.getMinorVersion(),
        shardSpec.getAtomicUpdateGroupSize()
    );
  }

  private ReferenceCountedSegmentProvider(
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
  public boolean overshadows(ReferenceCountedSegmentProvider other)
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
  public Optional<Segment> acquireReference()
  {
    final Optional<Closeable> reference = incrementReferenceAndDecrementOnceCloseable();
    return reference.map(ReferenceClosingSegment::new);
  }

  private boolean includeRootPartitions(ReferenceCountedSegmentProvider other)
  {
    return startRootPartitionId <= other.startRootPartitionId && endRootPartitionId >= other.endRootPartitionId;
  }

  /**
   * Wraps a {@link Segment} and closes reference to this segment, decrementing the counter instead of closing the
   * segment itself
   */
  public final class ReferenceClosingSegment extends WrappedSegment
  {
    private final Closeable referenceCloseable;
    private boolean isClosed;

    private ReferenceClosingSegment(Closeable referenceCloseable)
    {
      super(baseObject);
      this.referenceCloseable = referenceCloseable;
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return baseObject.as(clazz);
    }

    @Override
    public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
    {
      // a segment cannot directly have any policies, so use the enforcer directly
      policyEnforcer.validateOrElseThrow(this, null);
    }

    @Override
    public void close() throws IOException
    {
      if (isClosed) {
        return;
      }
      referenceCloseable.close();
      isClosed = true;
    }

    @VisibleForTesting
    public ReferenceCountedSegmentProvider getProvider()
    {
      return ReferenceCountedSegmentProvider.this;
    }
  }
}
