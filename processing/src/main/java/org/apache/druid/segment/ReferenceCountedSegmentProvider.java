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
import com.google.common.primitives.Shorts;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper object to assist in managing {@link Segment} storage lifecycle. Provides access to the {@link Segment} tracked
 * with reference counting, releasing the reference on close. Closing this provider closes the underlying
 * {@link Segment} once all references are returned, ensuring that a segment cannot be dropped while it is actively
 * being used.
 * <p>
 * This also implements {@link Overshadowable}, so the providers can be arranged in a
 * {@link org.apache.druid.timeline.TimelineLookup}.
 */
public class ReferenceCountedSegmentProvider extends ReferenceCountingCloseableObject<Segment>
    implements Overshadowable<ReferenceCountedSegmentProvider>, ReferenceCountedObjectProvider<Segment>
{
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

  /**
   * Returns a {@link ReferenceCountedObjectProvider<Segment>} that simply returns a wrapper around the supplied
   * {@link Segment} that prevents closing the segment, and does not provide any reference tracking functionality.
   * Useful for participating in the interface in cases where the lifecycle of the segment is managed externally.
   */
  public static ReferenceCountedObjectProvider<Segment> wrapUnmanaged(Segment unmanagedObject)
  {
    return () -> Optional.of(new WrappedSegment(unmanagedObject)
    {
      @Nullable
      @Override
      public <T> T as(@Nonnull Class<T> clazz)
      {
        return unmanagedObject.as(clazz);
      }

      @Override
      public void close()
      {
        // do not close it, the object lifecycle is managed externally
      }
    });
  }

  private final short startRootPartitionId;
  private final short endRootPartitionId;
  private final short minorVersion;
  private final short atomicUpdateGroupSize;

  private ReferenceCountedSegmentProvider(
      Segment baseSegment,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      short atomicUpdateGroupSize
  )
  {
    super(baseSegment);
    // ReferenceCountingSegment should not wrap another reference, because the inner reference would effectively
    // be ignored.
    if (baseSegment instanceof ReferenceClosingSegment) {
      throw DruidException.defensive(
          "Cannot use a ReferenceClosingSegment[%s] as baseSegment for a ReferenceCountedSegmentProvider",
          baseSegment.asString()
      );
    }
    this.startRootPartitionId = Shorts.checkedCast(startRootPartitionId);;
    this.endRootPartitionId = Shorts.checkedCast(endRootPartitionId);
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
   * Wraps a {@link Segment} and decrements reference to this segment of {@link ReferenceCountedSegmentProvider} on
   * close (instead of closing the segment itself which is managed through the provider)
   */
  public final class ReferenceClosingSegment implements Segment
  {
    private final Closeable referenceCloseable;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private ReferenceClosingSegment(Closeable referenceCloseable)
    {
      this.referenceCloseable = referenceCloseable;
    }

    @Nullable
    @Override
    public SegmentId getId()
    {
      return baseObject.getId();
    }

    @Override
    public Interval getDataInterval()
    {
      return baseObject.getDataInterval();
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      if (isClosed.get()) {
        throw DruidException.defensive(
            "Segment[%s] reference is already released, cannot get[%s]",
            baseObject.getId(),
            clazz
        );
      }
      return baseObject.as(clazz);
    }

    @Override
    public boolean isTombstone()
    {
      return baseObject.isTombstone();
    }

    @Override
    public String asString()
    {
      return baseObject.asString();
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
      if (isClosed.compareAndSet(false, true)) {
        referenceCloseable.close();
      } else {
        throw DruidException.defensive(
            "Segment[%s] reference is already released, cannot close again",
            baseObject.getId()
        );
      }
    }

    @VisibleForTesting
    public ReferenceCountedSegmentProvider getProvider()
    {
      return ReferenceCountedSegmentProvider.this;
    }
  }
}
