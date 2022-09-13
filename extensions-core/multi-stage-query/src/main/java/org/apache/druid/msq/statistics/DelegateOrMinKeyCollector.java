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

package org.apache.druid.msq.statistics;

import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Delegates to some other kind of {@link KeyCollector} at first, until its {@link #downSample()} fails to downsample.
 * At that point, the delegate collector is nulled out and this collector starts tracking the min key instead.
 *
 * This is useful because it allows us to wrap any {@link KeyCollector} and enable downsampling to a single key, even
 * if the original collector does not support that. For example, {@link QuantilesSketchKeyCollector} cannot downsample
 * below K = 2, which retains more than one key.
 *
 * Created by {@link DelegateOrMinKeyCollectorFactory}.
 */
public class DelegateOrMinKeyCollector<TDelegate extends KeyCollector<TDelegate>>
    implements KeyCollector<DelegateOrMinKeyCollector<TDelegate>>
{
  private final Comparator<RowKey> comparator;

  // Null means we have been downsampled all the way to a single key.
  @Nullable
  private TDelegate delegate;

  @Nullable
  private RowKey minKey;

  DelegateOrMinKeyCollector(
      final Comparator<RowKey> comparator,
      @Nullable final TDelegate delegate,
      @Nullable final RowKey minKey
  )
  {
    this.comparator = comparator;
    this.delegate = delegate;
    this.minKey = minKey;

    if (delegate != null && minKey != null) {
      throw new ISE("Cannot have both 'delegate' and 'minKey'");
    }
  }

  public Optional<TDelegate> getDelegate()
  {
    return Optional.ofNullable(delegate);
  }

  @Override
  public void add(RowKey key, long weight)
  {
    if (delegate != null) {
      delegate.add(key, weight);
    } else if (minKey == null || comparator.compare(key, minKey) < 0) {
      minKey = key;
    }
  }

  @Override
  public void addAll(DelegateOrMinKeyCollector<TDelegate> other)
  {
    if (delegate != null) {
      if (other.delegate != null) {
        delegate.addAll(other.delegate);
      } else if (other.minKey != null) {
        delegate.add(other.minKey, 1);
      }
    } else if (!other.isEmpty()) {
      add(other.minKey(), 1);
    }
  }

  @Override
  public boolean isEmpty()
  {
    if (delegate != null) {
      return delegate.isEmpty();
    } else {
      return minKey == null;
    }
  }

  @Override
  public long estimatedTotalWeight()
  {
    if (delegate != null) {
      return delegate.estimatedTotalWeight();
    } else {
      return minKey != null ? 1 : 0;
    }
  }

  @Override
  public int estimatedRetainedKeys()
  {
    if (delegate != null) {
      return delegate.estimatedRetainedKeys();
    } else {
      return minKey != null ? 1 : 0;
    }
  }

  @Override
  public double estimatedRetainedBytes()
  {
    if (delegate != null) {
      return delegate.estimatedRetainedBytes();
    } else {
      return minKey != null ? minKey.getNumberOfBytes() : 0;
    }
  }

  @Override
  public boolean downSample()
  {
    if (delegate != null && !delegate.downSample()) {
      minKey = delegate.isEmpty() ? null : minKey();
      delegate = null;
    }

    return true;
  }

  @Override
  public RowKey minKey()
  {
    if (delegate != null) {
      return delegate.minKey();
    } else if (minKey != null) {
      return minKey;
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * Generates partitions using the delegate if it is valid (i.e., if it has not been discarded due to extreme
   * downsampling). If the delegate is not valid, then this method will always return a single partition.
   */
  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetWeight)
  {
    if (targetWeight <= 0) {
      throw new IAE("targetPartitionWeight must be positive, but was [%d]", targetWeight);
    } else if (delegate != null) {
      return delegate.generatePartitionsWithTargetWeight(targetWeight);
    } else if (minKey != null) {
      return new ClusterByPartitions(Collections.singletonList(new ClusterByPartition(minKey, null)));
    } else {
      return ClusterByPartitions.oneUniversalPartition();
    }
  }
}
