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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2LongSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A key collector that is used when aggregating. It tracks distinct keys. The key must be the aggregation key (i.e.
 * the GROUP BY columns.)
 */
public class DistinctKeyCollector implements KeyCollector<DistinctKeyCollector>
{
  static final int INITIAL_MAX_BYTES = 134_217_728;
  static final int SMALLEST_MAX_BYTES = 5000;
  private static final int MISSING_KEY_WEIGHT = 0;

  private final Comparator<RowKey> comparator;

  /**
   * Key -> weight first encountered for that key.
   *
   * Each retained key represents the chunk of keyspace that starts with that key (inclusive) and ends with the
   * next key (exclusive). The estimated number of keys in that chunk is pow(2, spaceReductionFactor).
   * The weight is treated as the average row weight for the entire keyspace represented by this key. This isn't
   * super precise, for two reasons:
   *
   * (1) nothing guarantees that each key is always added with the same weight;
   * (2) nothing guarantees that the weight of a retained key is going to be representative of the nonretained keys
   * that it represents.
   *
   * Item (1) is mitigated by the fact that row weight tends to be correlated with key. (This collector is used
   * when aggregating, and when aggregating, a big chunk of the row weight is driven by the aggregation key.)
   *
   * Item (2) is more likely to be an issue in real life. It is mitigated if the rows with "nearby" keys are likely to have
   * similar weight. That happens sometimes, but nothing guarantees it.
   *
   * The approach here is certainly more fragile than the one in QuantilesSketchKeyCollector, the other major key
   * collector type, which is based on a more solid statistical foundation.
   */
  private final Object2LongSortedMap<RowKey> retainedKeys;
  private int maxBytes;
  private int retainedBytes;

  /**
   * Each key is retained with probability 2^(-spaceReductionFactor). This value is incremented on calls to
   * {@link #downSample()}, since it is used to control the size of the {@link #retainedKeys} map as more keys
   * are added.
   */
  private int spaceReductionFactor;

  // Sum of all values of retainedKeys.
  private long totalWeightUnadjusted;

  DistinctKeyCollector(
      final Comparator<RowKey> comparator,
      final Object2LongSortedMap<RowKey> retainedKeys,
      final int spaceReductionFactor
  )
  {
    this.comparator = Preconditions.checkNotNull(comparator, "comparator");
    this.retainedKeys = Preconditions.checkNotNull(retainedKeys, "retainedKeys");
    this.retainedKeys.defaultReturnValue(MISSING_KEY_WEIGHT);
    this.maxBytes = INITIAL_MAX_BYTES;
    this.spaceReductionFactor = spaceReductionFactor;
    this.totalWeightUnadjusted = 0;

    final LongIterator weightIterator = retainedKeys.values().iterator();
    while (weightIterator.hasNext()) {
      totalWeightUnadjusted += weightIterator.nextLong();
    }
  }

  DistinctKeyCollector(final Comparator<RowKey> comparator)
  {
    this(comparator, new Object2LongRBTreeMap<>(comparator), 0);
  }

  @Override
  public void add(RowKey key, long weight)
  {
    if (weight <= 0) {
      throw new IAE("Weight must be positive");
    }

    final boolean isNewMin = retainedKeys.isEmpty() || comparator.compare(key, retainedKeys.firstKey()) < 0;

    if (isNewMin || isKeySelected(key)) {
      if (isNewMin && !retainedKeys.isEmpty() && !isKeySelected(retainedKeys.firstKey())) {
        // Old min should be kicked out.
        totalWeightUnadjusted -= retainedKeys.removeLong(retainedKeys.firstKey());
        retainedBytes -= retainedKeys.firstKey().getNumberOfBytes();
      }

      if (retainedKeys.putIfAbsent(key, weight) == MISSING_KEY_WEIGHT) {
        // We did add this key. (Previous value was zero, meaning absent.)
        totalWeightUnadjusted += weight;
        retainedBytes += key.getNumberOfBytes();
      }

      while (retainedBytes >= maxBytes) {
        increaseSpaceReductionFactorIfPossible();
      }
    }
  }

  @Override
  public void addAll(DistinctKeyCollector other)
  {
    while (!retainedKeys.isEmpty() && spaceReductionFactor < other.spaceReductionFactor) {
      increaseSpaceReductionFactorIfPossible();
    }

    if (retainedKeys.isEmpty()) {
      this.spaceReductionFactor = other.spaceReductionFactor;
    }

    for (final Object2LongMap.Entry<RowKey> otherEntry : other.retainedKeys.object2LongEntrySet()) {
      add(otherEntry.getKey(), otherEntry.getLongValue());
    }
  }

  @Override
  public boolean isEmpty()
  {
    return retainedKeys.isEmpty();
  }

  @Override
  public long estimatedTotalWeight()
  {
    assert totalWeightUnadjusted == retainedKeys.values().longStream().sum();
    return totalWeightUnadjusted << spaceReductionFactor;
  }

  @Override
  public int estimatedRetainedKeys()
  {
    return retainedKeys.size();
  }

  @Override
  public double estimatedRetainedBytes()
  {
    return retainedBytes;
  }

  @Override
  public RowKey minKey()
  {
    // Throws NoSuchElementException when empty, as required by minKey contract.
    return retainedKeys.firstKey();
  }

  @Override
  public boolean downSample()
  {
    if (retainedKeys.size() <= 1) {
      return true;
    }

    if (maxBytes <= SMALLEST_MAX_BYTES) {
      return false;
    }

    maxBytes /= 2;

    while (retainedBytes >= maxBytes) {
      if (!increaseSpaceReductionFactorIfPossible()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetPartitionWeight)
  {
    if (targetPartitionWeight <= 0) {
      throw new IAE("targetPartitionWeight must be positive, but was [%d]", targetPartitionWeight);
    } else if (retainedKeys.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final List<ClusterByPartition> partitions = new ArrayList<>();
    final ObjectBidirectionalIterator<Object2LongMap.Entry<RowKey>> iterator =
        retainedKeys.object2LongEntrySet().iterator();
    RowKey startKey = retainedKeys.firstKey();
    long partitionWeight = 0;

    while (iterator.hasNext()) {
      final Object2LongMap.Entry<RowKey> entry = iterator.next();
      final long keyWeight = entry.getLongValue() << spaceReductionFactor;
      final long partitionCountAfterKey = partitionWeight + keyWeight;

      if (partitionWeight > 0
          && partitionCountAfterKey > targetPartitionWeight
          && partitionCountAfterKey - targetPartitionWeight > targetPartitionWeight - partitionWeight) {
        // New partition *not* including the current key.
        partitions.add(new ClusterByPartition(startKey, entry.getKey()));
        startKey = entry.getKey();
        partitionWeight = keyWeight;
      } else {
        // Add to existing partition.
        partitionWeight = partitionCountAfterKey;
      }
    }

    // Add the last partition.
    partitions.add(new ClusterByPartition(startKey, null));

    return new ClusterByPartitions(partitions);
  }

  @JsonProperty("keys")
  Map<RowKey, Long> getRetainedKeys()
  {
    return retainedKeys;
  }

  @JsonProperty("maxBytes")
  int getMaxBytes()
  {
    return maxBytes;
  }

  @JsonProperty("spaceReductionFactor")
  int getSpaceReductionFactor()
  {
    return spaceReductionFactor;
  }

  /**
   * Returns whether a key would be selected by the current {@link #spaceReductionFactor}.
   */
  private boolean isKeySelected(final RowKey key)
  {
    return spaceReductionFactor == 0 || Long.numberOfTrailingZeros(key.longHashCode()) >= spaceReductionFactor;
  }

  /**
   * Increment {@link #spaceReductionFactor} and throw away keys from {@link #retainedKeys} as appropriate.
   * {@link #retainedKeys} must be nonempty.
   *
   * Returns true if the space reduction factor was increased, false otherwise.
   */
  private boolean increaseSpaceReductionFactorIfPossible()
  {
    if (spaceReductionFactor == Long.SIZE) {
      // This is the biggest possible spaceReductionFactor. It's unlikely to happen unless maxKeys is very low.
      return false;
    }

    if (retainedKeys.isEmpty()) {
      // Incorrect usage by code elsewhere in this class.
      throw new ISE("Cannot increase space reduction factor when keys are empty");
    }

    spaceReductionFactor++;

    final ObjectBidirectionalIterator<Object2LongMap.Entry<RowKey>> iterator =
        retainedKeys.object2LongEntrySet().iterator();

    // Never remove the first key.
    if (iterator.hasNext()) {
      iterator.next();
    }

    while (iterator.hasNext()) {
      final Object2LongMap.Entry<RowKey> entry = iterator.next();
      final RowKey key = entry.getKey();

      if (!isKeySelected(key)) {
        totalWeightUnadjusted -= entry.getLongValue();
        retainedBytes -= entry.getKey().getNumberOfBytes();
        iterator.remove();
      }
    }

    return true;
  }
}
