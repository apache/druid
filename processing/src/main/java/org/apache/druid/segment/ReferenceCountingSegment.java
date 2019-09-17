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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReferenceCountingSegment allows to call {@link #close()} before some other "users", which called {@link
 * #increment()}, has not called {@link #decrement()} yet, and the wrapped {@link Segment} won't be actually closed
 * until that. So ReferenceCountingSegment implements something like automatic reference count-based resource
 * management.
 */
public class ReferenceCountingSegment extends AbstractSegment implements Overshadowable<ReferenceCountingSegment>
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private final Segment baseSegment;
  private final short startRootPartitionId;
  private final short endRootPartitionId;
  private final short minorVersion;
  private final short atomicUpdateGroupSize;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Phaser referents = new Phaser(1)
  {
    @Override
    protected boolean onAdvance(int phase, int registeredParties)
    {
      // Ensure that onAdvance() doesn't throw exception, otherwise termination won't happen
      if (registeredParties != 0) {
        log.error("registeredParties[%s] is not 0", registeredParties);
      }
      try {
        baseSegment.close();
      }
      catch (Exception e) {
        try {
          log.error(e, "Exception while closing segment[%s]", baseSegment.getId());
        }
        catch (Exception e2) {
          // ignore
        }
      }
      // Always terminate.
      return true;
    }
  };

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

  private ReferenceCountingSegment(
      Segment baseSegment,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      short atomicUpdateGroupSize
  )
  {
    this.baseSegment = baseSegment;
    this.startRootPartitionId = (short) startRootPartitionId;
    this.endRootPartitionId = (short) endRootPartitionId;
    this.minorVersion = minorVersion;
    this.atomicUpdateGroupSize = atomicUpdateGroupSize;
  }

  @Nullable
  public Segment getBaseSegment()
  {
    return !isClosed() ? baseSegment : null;
  }

  public int getNumReferences()
  {
    return Math.max(referents.getRegisteredParties() - 1, 0);
  }

  public boolean isClosed()
  {
    return referents.isTerminated();
  }

  @Override
  @Nullable
  public SegmentId getId()
  {
    return !isClosed() ? baseSegment.getId() : null;
  }

  @Override
  @Nullable
  public Interval getDataInterval()
  {
    return !isClosed() ? baseSegment.getDataInterval() : null;
  }

  @Override
  @Nullable
  public QueryableIndex asQueryableIndex()
  {
    return !isClosed() ? baseSegment.asQueryableIndex() : null;
  }

  @Override
  @Nullable
  public StorageAdapter asStorageAdapter()
  {
    return !isClosed() ? baseSegment.asStorageAdapter() : null;
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      referents.arriveAndDeregister();
    } else {
      log.warn("close() is called more than once on ReferenceCountingSegment");
    }
  }

  public boolean increment()
  {
    // Negative return from referents.register() means the Phaser is terminated.
    return referents.register() >= 0;
  }

  /**
   * Returns a {@link Closeable} which action is to call {@link #decrement()} only once. If close() is called on the
   * returned Closeable object for the second time, it won't call {@link #decrement()} again.
   */
  public Closeable decrementOnceCloseable()
  {
    AtomicBoolean decremented = new AtomicBoolean(false);
    return () -> {
      if (decremented.compareAndSet(false, true)) {
        decrement();
      } else {
        log.warn("close() is called more than once on ReferenceCountingSegment.decrementOnceCloseable()");
      }
    };
  }

  public void decrement()
  {
    referents.arriveAndDeregister();
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    return getBaseSegment().as(clazz);
  }

  @Override
  public boolean overshadows(ReferenceCountingSegment other)
  {
    if (baseSegment.getId().getDataSource().equals(other.baseSegment.getId().getDataSource())
        && baseSegment.getId().getInterval().overlaps(other.baseSegment.getId().getInterval())) {
      final int majorVersionCompare = baseSegment.getId().getVersion()
                                                 .compareTo(other.baseSegment.getId().getVersion());
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
    return baseSegment.getId().getVersion();
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
}
