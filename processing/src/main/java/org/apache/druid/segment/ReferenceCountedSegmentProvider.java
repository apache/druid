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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.timeline.SegmentId;
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
 */
public class ReferenceCountedSegmentProvider extends ReferenceCountingCloseableObject<Segment>
    implements ReferenceCountedObjectProvider<Segment>
{
  public static ReferenceCountedSegmentProvider of(Segment segment)
  {
    return new ReferenceCountedSegmentProvider(segment);
  }

  /**
   * Make a wrapper for a {@link Segment} that does not close the underlying segment on close
   */
  public static Optional<Segment> unmanaged(Segment segment)
  {
    return Optional.of(new UnmanagedReference(segment));
  }

  public ReferenceCountedSegmentProvider(Segment baseSegment)
  {
    super(baseSegment);
    // ReferenceCountingSegment should not wrap another reference, because the inner reference would effectively
    // be ignored.
    if (baseSegment instanceof ReferenceClosingSegment) {
      throw DruidException.defensive(
          "Cannot use a ReferenceClosingSegment[%s] as baseSegment for a ReferenceCountedSegmentProvider",
          baseSegment.getDebugString()
      );
    }
  }

  @Nullable
  public Segment getBaseSegment()
  {
    return !isClosed() ? baseObject : null;
  }

  @Override
  public Optional<Segment> acquireReference()
  {
    final Optional<Closeable> reference = incrementReferenceAndDecrementOnceCloseable();
    return reference.map(ReferenceClosingSegment::new);
  }

  /**
   * Base type to mark {@link Segment} returned by {@link ReferenceCountedObjectProvider<Segment>} as 'leaf' segments,
   * to distinguish from other transformations which can be done on top of this segment, such as by
   * {@link SegmentMapFunction}
   */
  public abstract static class LeafReference implements Segment
  {
    protected final Segment baseSegment;

    public LeafReference(Segment baseSegment)
    {
      this.baseSegment = baseSegment;
    }

    @Nullable
    @Override
    public SegmentId getId()
    {
      return baseSegment.getId();
    }

    @Override
    public Interval getDataInterval()
    {
      return baseSegment.getDataInterval();
    }

    @Override
    public boolean isTombstone()
    {
      return baseSegment.isTombstone();
    }

    @Override
    public String getDebugString()
    {
      return baseSegment.getDebugString();
    }

    @Override
    public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
    {
      // a segment cannot directly have any policies, so use the enforcer directly
      policyEnforcer.validateOrElseThrow(baseSegment, null);
    }
  }


  /**
   * Wraps a {@link Segment} and decrements reference to this segment of {@link ReferenceCountedSegmentProvider} on
   * close (instead of closing the segment itself which is managed through the provider)
   */
  public final class ReferenceClosingSegment extends LeafReference
  {
    private final Closeable referenceCloseable;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private ReferenceClosingSegment(Closeable referenceCloseable)
    {
      super(baseObject);
      this.referenceCloseable = referenceCloseable;
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

  public static final class UnmanagedReference extends LeafReference
  {
    public UnmanagedReference(Segment delegate)
    {
      super(delegate);
    }

    @Nullable
    @Override
    public <T> T as(@Nonnull Class<T> clazz)
    {
      return baseSegment.as(clazz);
    }

    @Override
    public void close()
    {
      // close nothing, the lifecycle of the wrapped segment isn't managed by a provider
    }
  }
}
