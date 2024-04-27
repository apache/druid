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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ClusterByStatisticsCollectorImpl implements ClusterByStatisticsCollector
{
  // Check if this can be done via binary search (accounting for the fuzziness of the datasketches)
  // for an objectively faster and more accurate solution instead of finding the best match with the following parameters
  private static final int MAX_COUNT_MAX_ITERATIONS = 500;
  private static final double MAX_COUNT_ITERATION_GROWTH_FACTOR = 1.05;
  private final Logger log = new Logger(ClusterByStatisticsCollectorImpl.class);

  private final ClusterBy clusterBy;
  private final RowKeyReader keyReader;
  private final KeyCollectorFactory<? extends KeyCollector<?>, ? extends KeyCollectorSnapshot> keyCollectorFactory;
  private final SortedMap<RowKey, BucketHolder> buckets;
  private final boolean checkHasMultipleValues;

  private final boolean[] hasMultipleValues;

  private final long maxRetainedBytes;
  private final int maxBuckets;
  private long totalRetainedBytes;

  private ClusterByStatisticsCollectorImpl(
      final ClusterBy clusterBy,
      final RowKeyReader keyReader,
      final KeyCollectorFactory<?, ?> keyCollectorFactory,
      final long maxRetainedBytes,
      final int maxBuckets,
      final boolean checkHasMultipleValues
  )
  {
    this.clusterBy = clusterBy;
    this.keyReader = keyReader;
    this.keyCollectorFactory = keyCollectorFactory;
    this.maxRetainedBytes = maxRetainedBytes;
    this.buckets = new TreeMap<>(clusterBy.bucketComparator());
    this.maxBuckets = maxBuckets;
    this.checkHasMultipleValues = checkHasMultipleValues;
    this.hasMultipleValues = checkHasMultipleValues ? new boolean[clusterBy.getColumns().size()] : null;

    if (maxBuckets > maxRetainedBytes) {
      throw new IAE("maxBuckets[%s] cannot be larger than maxRetainedBytes[%s]", maxBuckets, maxRetainedBytes);
    }
  }

  public static ClusterByStatisticsCollector create(
      final ClusterBy clusterBy,
      final RowSignature signature,
      final long maxRetainedBytes,
      final int maxBuckets,
      final boolean aggregate,
      final boolean checkHasMultipleValues
  )
  {
    final RowKeyReader keyReader = clusterBy.keyReader(signature);
    final KeyCollectorFactory<?, ?> keyCollectorFactory = KeyCollectors.makeStandardFactory(clusterBy, aggregate);

    return new ClusterByStatisticsCollectorImpl(
        clusterBy,
        keyReader,
        keyCollectorFactory,
        maxRetainedBytes,
        maxBuckets,
        checkHasMultipleValues
    );
  }

  @Override
  public ClusterBy getClusterBy()
  {
    return clusterBy;
  }

  @Override
  public ClusterByStatisticsCollector add(final RowKey key, final int weight)
  {
    if (checkHasMultipleValues) {
      for (int i = 0; i < clusterBy.getColumns().size(); i++) {
        hasMultipleValues[i] = hasMultipleValues[i] || keyReader.hasMultipleValues(key, i);
      }
    }

    final BucketHolder bucketHolder = getOrCreateBucketHolder(keyReader.trim(key, clusterBy.getBucketByCount()));

    bucketHolder.keyCollector.add(key, weight);

    totalRetainedBytes += bucketHolder.updateRetainedBytes();
    if (totalRetainedBytes > maxRetainedBytes) {
      log.debug("Downsampling ClusterByStatisticsCollector as totalRetainedBytes[%s] is greater than maxRetainedBytes[%s]", totalRetainedBytes, maxRetainedBytes);
      downSample();
    }

    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsCollector other)
  {
    if (other instanceof ClusterByStatisticsCollectorImpl) {
      ClusterByStatisticsCollectorImpl that = (ClusterByStatisticsCollectorImpl) other;

      // Add all key collectors from the other collector.
      for (Map.Entry<RowKey, BucketHolder> otherBucketEntry : that.buckets.entrySet()) {
        final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucketEntry.getKey());

        //noinspection rawtypes, unchecked
        ((KeyCollector) bucketHolder.keyCollector).addAll(otherBucketEntry.getValue().keyCollector);

        totalRetainedBytes += bucketHolder.updateRetainedBytes();
        if (totalRetainedBytes > maxRetainedBytes) {
          log.debug("Downsampling ClusterByStatisticsCollector as totalRetainedBytes[%s] is greater than maxRetainedBytes[%s]", totalRetainedBytes, maxRetainedBytes);
          downSample();
        }
      }

      if (checkHasMultipleValues) {
        for (int i = 0; i < clusterBy.getColumns().size(); i++) {
          hasMultipleValues[i] = hasMultipleValues[i] || that.hasMultipleValues[i];
        }
      }
    } else {
      addAll(other.snapshot());
    }

    return this;
  }

  @Override
  public ClusterByStatisticsCollector addAll(final ClusterByStatisticsSnapshot snapshot)
  {
    // Add all key collectors from the other collector.
    for (ClusterByStatisticsSnapshot.Bucket otherBucket : snapshot.getBuckets().values()) {
      //noinspection rawtypes, unchecked
      final KeyCollector<?> otherKeyCollector =
          ((KeyCollectorFactory) keyCollectorFactory).fromSnapshot(otherBucket.getKeyCollectorSnapshot());
      final BucketHolder bucketHolder = getOrCreateBucketHolder(otherBucket.getBucketKey());

      //noinspection rawtypes, unchecked
      ((KeyCollector) bucketHolder.keyCollector).addAll(otherKeyCollector);

      totalRetainedBytes += bucketHolder.updateRetainedBytes();
      if (totalRetainedBytes > maxRetainedBytes) {
        log.debug("Downsampling ClusterByStatisticsCollector as totalRetainedBytes[%s] is greater than maxRetainedBytes[%s]", totalRetainedBytes, maxRetainedBytes);
        downSample();
      }
    }

    if (checkHasMultipleValues) {
      for (int keyPosition : snapshot.getHasMultipleValues()) {
        hasMultipleValues[keyPosition] = true;
      }
    }

    return this;
  }

  @Override
  public long estimatedTotalWeight()
  {
    long count = 0L;
    for (final BucketHolder bucketHolder : buckets.values()) {
      count += bucketHolder.keyCollector.estimatedTotalWeight();
    }
    return count;
  }

  @Override
  public boolean hasMultipleValues(final int keyPosition)
  {
    if (checkHasMultipleValues) {
      if (keyPosition < 0 || keyPosition >= clusterBy.getColumns().size()) {
        throw new IAE("Invalid keyPosition [%d]", keyPosition);
      }

      return hasMultipleValues[keyPosition];
    } else {
      throw new ISE("hasMultipleValues not available for this collector");
    }
  }

  @Override
  public ClusterByStatisticsCollector clear()
  {
    buckets.clear();
    totalRetainedBytes = 0;
    return this;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithTargetWeight(final long targetWeight)
  {
    logSketches();

    if (targetWeight < 1) {
      throw new IAE("Target weight must be positive");
    }

    assertRetainedByteCountsAreTrackedCorrectly();

    if (buckets.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    }

    final List<ClusterByPartition> partitions = new ArrayList<>();

    for (final BucketHolder bucket : buckets.values()) {
      final List<ClusterByPartition> bucketPartitions =
          bucket.keyCollector.generatePartitionsWithTargetWeight(targetWeight).ranges();

      if (!partitions.isEmpty() && !bucketPartitions.isEmpty()) {
        // Stitch up final partition of previous bucket to match the first partition of this bucket.
        partitions.set(
            partitions.size() - 1,
            new ClusterByPartition(
                partitions.get(partitions.size() - 1).getStart(),
                bucketPartitions.get(0).getStart()
            )
        );
      }

      partitions.addAll(bucketPartitions);
    }

    final ClusterByPartitions retVal = new ClusterByPartitions(partitions);

    if (!retVal.allAbutting()) {
      // It's a bug if this happens.
      throw new ISE("Partitions are not all abutting");
    }

    return retVal;
  }

  @Override
  public ClusterByPartitions generatePartitionsWithMaxCount(final int maxNumPartitions)
  {
    logSketches();

    if (maxNumPartitions < 1) {
      throw new IAE("Must have at least one partition");
    } else if (buckets.isEmpty()) {
      return ClusterByPartitions.oneUniversalPartition();
    } else if (maxNumPartitions == 1 && clusterBy.getBucketByCount() == 0) {
      return new ClusterByPartitions(
          Collections.singletonList(
              new ClusterByPartition(
                  buckets.get(buckets.firstKey()).keyCollector.minKey(),
                  null
              )
          )
      );
    }

    long totalWeight = 0;

    for (final BucketHolder bucketHolder : buckets.values()) {
      totalWeight += bucketHolder.keyCollector.estimatedTotalWeight();
    }

    // Gradually increase targetPartitionSize until we get the right number of partitions.
    ClusterByPartitions ranges;
    long targetPartitionWeight = (long) Math.ceil((double) totalWeight / maxNumPartitions);
    int iterations = 0;

    do {
      if (iterations++ > MAX_COUNT_MAX_ITERATIONS) {
        // Could happen if there are a large number of partition-by keys, or if there are more buckets than
        // the max partition count.
        throw new ISE("Unable to compute partition ranges");
      }

      ranges = generatePartitionsWithTargetWeight(targetPartitionWeight);

      targetPartitionWeight = (long) Math.ceil(targetPartitionWeight * MAX_COUNT_ITERATION_GROWTH_FACTOR);
    } while (ranges.size() > maxNumPartitions);

    return ranges;
  }

  private void logSketches()
  {
    if (log.isDebugEnabled()) {
      // Log all sketches
      List<KeyCollector<?>> keyCollectors = buckets.values()
                                                   .stream()
                                                   .map(bucketHolder -> bucketHolder.keyCollector)
                                                   .sorted(Comparator.comparingInt(KeyCollector::sketchAccuracyFactor))
                                                   .collect(Collectors.toList());
      log.debug("KeyCollectors at partition generation: [%s]", keyCollectors);
    } else {
      // Log the 5 least accurate sketches
      List<KeyCollector<?>> limitedKeyCollectors = buckets.values()
                                                          .stream()
                                                          .map(bucketHolder -> bucketHolder.keyCollector)
                                                          .sorted(Comparator.comparingInt(KeyCollector::sketchAccuracyFactor))
                                                          .limit(5)
                                                          .collect(Collectors.toList());
      log.info("Most downsampled keyCollectors: [%s]", limitedKeyCollectors);
    }
  }

  @Override
  public ClusterByStatisticsSnapshot snapshot()
  {
    assertRetainedByteCountsAreTrackedCorrectly();

    final Map<Long, ClusterByStatisticsSnapshot.Bucket> bucketSnapshots = new HashMap<>();
    final RowKeyReader trimmedRowReader = keyReader.trimmedKeyReader(clusterBy.getBucketByCount());

    for (final Map.Entry<RowKey, BucketHolder> bucketEntry : buckets.entrySet()) {
      //noinspection rawtypes, unchecked
      final KeyCollectorSnapshot keyCollectorSnapshot =
          ((KeyCollectorFactory) keyCollectorFactory).toSnapshot(bucketEntry.getValue().keyCollector);
      Long bucketKey = Long.MIN_VALUE;

      // If there is a clustering on time, read the first field from each bucket and add it to the snapshots.
      if (clusterBy.getBucketByCount() == 1) {
        bucketKey = (Long) trimmedRowReader.read(bucketEntry.getKey(), 0);
      }
      bucketSnapshots.put(bucketKey, new ClusterByStatisticsSnapshot.Bucket(bucketEntry.getKey(), keyCollectorSnapshot, totalRetainedBytes));
    }

    final IntSet hasMultipleValuesSet;

    if (checkHasMultipleValues) {
      hasMultipleValuesSet = new IntRBTreeSet();

      for (int i = 0; i < hasMultipleValues.length; i++) {
        if (hasMultipleValues[i]) {
          hasMultipleValuesSet.add(i);
        }
      }
    } else {
      hasMultipleValuesSet = null;
    }

    return new ClusterByStatisticsSnapshot(bucketSnapshots, hasMultipleValuesSet);
  }

  @VisibleForTesting
  List<KeyCollector<?>> getKeyCollectors()
  {
    return buckets.values().stream().map(holder -> holder.keyCollector).collect(Collectors.toList());
  }

  private BucketHolder getOrCreateBucketHolder(final RowKey bucketKey)
  {
    final BucketHolder existingHolder = buckets.get(Preconditions.checkNotNull(bucketKey, "bucketKey"));

    if (existingHolder != null) {
      return existingHolder;
    } else if (buckets.size() < maxBuckets) {
      final BucketHolder newHolder = new BucketHolder(keyCollectorFactory.newKeyCollector());
      buckets.put(bucketKey, newHolder);
      return newHolder;
    } else {
      throw new TooManyBucketsException(maxBuckets);
    }
  }

  /**
   * Reduce the number of retained bytes by about half, if possible. May reduce by less than that, or keep the
   * number the same, if downsampling is not possible. (For example: downsampling is not possible if all buckets
   * have been downsampled all the way to one key each.)
   */
  void downSample()
  {
    long newTotalRetainedBytes = totalRetainedBytes;
    final long targetTotalRetainedBytes = totalRetainedBytes / 2;

    final List<Pair<Long, BucketHolder>> sortedHolders = new ArrayList<>(buckets.size());
    final RowKeyReader trimmedRowReader = keyReader.trimmedKeyReader(clusterBy.getBucketByCount());

    // Only consider holders with more than one retained key. Holders with a single retained key cannot be downsampled.
    for (final Map.Entry<RowKey, BucketHolder> entry : buckets.entrySet()) {
      BucketHolder bucketHolder = entry.getValue();
      if (bucketHolder != null && bucketHolder.keyCollector.estimatedRetainedKeys() > 1) {
        Long timeChunk = clusterBy.getBucketByCount() == 0 ? null : (Long) trimmedRowReader.read(entry.getKey(), 0);
        sortedHolders.add(Pair.of(timeChunk, bucketHolder));
      }
    }

    // Downsample least-dense buckets first. (They're less likely to need high resolution.)
    sortedHolders.sort(
        Comparator.comparing((Pair<Long, BucketHolder> pair) ->
                                 (double) pair.rhs.keyCollector.estimatedTotalWeight()
                                 / pair.rhs.keyCollector.estimatedRetainedKeys())
    );

    int i = 0;
    while (i < sortedHolders.size() && newTotalRetainedBytes > targetTotalRetainedBytes) {
      final Long timeChunk = sortedHolders.get(i).lhs;
      final BucketHolder bucketHolder = sortedHolders.get(i).rhs;

      // Ignore false return, because we wrap all collectors in DelegateOrMinKeyCollector and can be assured that
      // it will downsample all the way to one if needed. Can't do better than that.
      log.debug("Downsampling sketch for timeChunk [%s]: [%s]", timeChunk, bucketHolder.keyCollector);
      bucketHolder.keyCollector.downSample();
      newTotalRetainedBytes += bucketHolder.updateRetainedBytes();

      if (i == sortedHolders.size() - 1 || sortedHolders.get(i + 1).rhs.retainedBytes > bucketHolder.retainedBytes || bucketHolder.keyCollector.estimatedRetainedKeys() <= 1) {
        i++;
      }
    }

    totalRetainedBytes = newTotalRetainedBytes;
  }

  private void assertRetainedByteCountsAreTrackedCorrectly()
  {
    // Check cached value of retainedKeys in each holder.
    assert buckets.values()
                  .stream()
                  .allMatch(holder -> holder.retainedBytes == holder.keyCollector.estimatedRetainedBytes());

    // Check cached value of totalRetainedBytes.
    assert totalRetainedBytes ==
           buckets.values().stream().mapToDouble(holder -> holder.keyCollector.estimatedRetainedBytes()).sum();
  }

  private static class BucketHolder
  {
    private final KeyCollector<?> keyCollector;
    private double retainedBytes;

    public BucketHolder(final KeyCollector<?> keyCollector)
    {
      this.keyCollector = keyCollector;
      this.retainedBytes = keyCollector.estimatedRetainedBytes();
    }

    public double updateRetainedBytes()
    {
      final double newRetainedBytes = keyCollector.estimatedRetainedBytes();
      final double difference = newRetainedBytes - retainedBytes;
      retainedBytes = newRetainedBytes;
      return difference;
    }
  }
}
