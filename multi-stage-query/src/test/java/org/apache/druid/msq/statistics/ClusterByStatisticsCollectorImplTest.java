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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import org.apache.druid.error.ThrowableMatcher;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.statistics.serde.ClusterByStatisticsSnapshotSerde;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class ClusterByStatisticsCollectorImplTest extends InitializedNullHandlingTest
{
  private static final double PARTITION_SIZE_LEEWAY = 0.35;

  private static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("x", ColumnType.LONG)
                                                            .add("y", ColumnType.LONG)
                                                            .add("z", ColumnType.STRING)
                                                            .build();

  private static final ClusterBy CLUSTER_BY_X = new ClusterBy(
      ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING)),
      0
  );

  private static final ClusterBy CLUSTER_BY_XY_BUCKET_BY_X = new ClusterBy(
      ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING), new KeyColumn("y", KeyOrder.ASCENDING)),
      1
  );
  private static final ClusterBy CLUSTER_BY_XYZ_BUCKET_BY_X = new ClusterBy(
      ImmutableList.of(
          new KeyColumn("x", KeyOrder.ASCENDING),
          new KeyColumn("y", KeyOrder.ASCENDING),
          new KeyColumn("z", KeyOrder.ASCENDING)
      ),
      1
  );

  // These numbers are roughly 50x lower than authentic production numbers. (See StageDefinition.)
  private static final int MAX_BYTES = 150_000;
  private static final int MAX_BUCKETS = 200;

  @Test
  public void test_clusterByX_unique()
  {
    final long numRows = 150_000;
    final boolean aggregate = false;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final Iterable<RowKey> keys = () ->
        LongStream.range(0, numRows)
                  .mapToObj(n -> createKey(clusterBy, n))
                  .iterator();

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(1, trackedBuckets(collector), StringUtils.format("%s: tracked bucket count", testName));
          Assertions.assertEquals(numRows, trackedRows(collector), StringUtils.format("%s: tracked row count", testName));

          for (int targetPartitionWeight : new int[]{5111, 6543, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void test_clusterByX_everyKeyAppearsTwice()
  {
    final long numRows = 150_000;
    final boolean aggregate = false;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final List<RowKey> keys = new ArrayList<>();

    for (int i = 0; i < numRows / 2; i++) {
      keys.add(createKey(clusterBy, (long) i));
      keys.add(createKey(clusterBy, (long) i));
    }

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(1, trackedBuckets(collector), StringUtils.format("%s: tracked bucket count", testName));
          Assertions.assertEquals(numRows, trackedRows(collector), StringUtils.format("%s: tracked row count", testName));

          for (int targetPartitionWeight : new int[]{5111, 6543, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 10, 50}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void test_clusterByX_everyKeyAppearsTwice_withAggregation()
  {
    final long numRows = 150_000;
    final boolean aggregate = true;
    final ClusterBy clusterBy = CLUSTER_BY_X;
    final List<RowKey> keys = new ArrayList<>();
    final int duplicationFactor = 2;

    for (int i = 0; i < numRows / duplicationFactor; i++) {
      for (int j = 0; j < duplicationFactor; j++) {
        keys.add(createKey(clusterBy, (long) i));
      }
    }

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(1, trackedBuckets(collector), StringUtils.format("%s: tracked bucket count", testName));

          final double expectedNumRows = (double) numRows / duplicationFactor;
          Assertions.assertEquals(
              expectedNumRows,
              (double) trackedRows(collector),
              expectedNumRows * .05, // Acceptable estimation error
              StringUtils.format("%s: tracked row count", testName)
          );

          for (int targetPartitionWeight : new int[]{5111, 6543, (int) numRows + 10}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 5, 25}) {
            verifyPartitionsWithMaxCount(
                StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                collector,
                maxPartitionCount,
                sortedKeyWeights,
                aggregate
            );
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_threeX_uniqueY()
  {
    final int numBuckets = 3;
    final boolean aggregate = false;
    final long numRows = 150_000;
    final ClusterBy clusterBy = CLUSTER_BY_XY_BUCKET_BY_X;
    final List<RowKey> keys = new ArrayList<>((int) numRows);

    for (int i = 0; i < numRows; i++) {
      final Object[] key = new Object[2];
      key[0] = (long) (i % numBuckets);
      key[1] = (long) i;
      keys.add(createKey(clusterBy, key));
    }

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(numBuckets, trackedBuckets(collector), StringUtils.format("%s: bucket count", testName));
          Assertions.assertEquals(numRows, trackedRows(collector), StringUtils.format("%s: row count", testName));

          for (int targetPartitionWeight : new int[]{5111, 6543}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 2, 3, 10, 50}) {
            if (maxPartitionCount < numBuckets) {
              final IllegalStateException e = Assertions.assertThrows(
                  IllegalStateException.class,
                  () -> collector.generatePartitionsWithMaxCount(maxPartitionCount)
              );

              ThrowableMatcher.of(IllegalStateException.class)
                             .expectMessage(message -> message.startsWith("Unable to compute partition ranges"))
                             .assertThat(e);
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_maxX_uniqueY()
  {
    final int numBuckets = MAX_BUCKETS;
    final boolean aggregate = false;
    final long numRows = 150_000;
    final ClusterBy clusterBy = CLUSTER_BY_XY_BUCKET_BY_X;
    final List<RowKey> keys = new ArrayList<>((int) numRows);

    for (int i = 0; i < numRows; i++) {
      final Object[] key = new Object[2];
      key[0] = (long) (i % numBuckets);
      key[1] = (long) i;
      keys.add(createKey(clusterBy, key));
    }

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(numBuckets, trackedBuckets(collector), StringUtils.format("%s: bucket count", testName));

          for (int targetPartitionWeight : new int[]{1701, 2301}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 10, numBuckets - 1, numBuckets}) {
            if (maxPartitionCount < numBuckets) {
              // Cannot compute partitions ranges when maxPartitionCount < numBuckets, because there must be at
              // least one partition per bucket.
              final IllegalStateException e = Assertions.assertThrows(
                  IllegalStateException.class,
                  () -> verifyPartitionsWithMaxCount(
                      StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                      collector,
                      maxPartitionCount,
                      sortedKeyWeights,
                      aggregate
                  )
              );

              ThrowableMatcher.of(IllegalStateException.class)
                             .expectMessage(message -> message.startsWith("Unable to compute partition ranges"))
                             .assertThat(e);
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void test_clusterByXYbucketByX_maxX_lowCardinalityY_withAggregation()
  {
    final int numBuckets = MAX_BUCKETS;
    final boolean aggregate = true;
    final long numRows = 150_000;
    final ClusterBy clusterBy = CLUSTER_BY_XY_BUCKET_BY_X;
    final List<RowKey> keys = new ArrayList<>((int) numRows);

    for (int i = 0; i < numRows; i++) {
      final Object[] key = new Object[2];
      key[0] = (long) (i % numBuckets);
      key[1] = (long) (i % 5); // Only five different Ys
      keys.add(createKey(clusterBy, key));
    }

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        computeSortedKeyWeightsFromUnweightedKeys(keys, clusterBy.keyComparator(SIGNATURE));

    doTest(
        clusterBy,
        aggregate,
        keys,
        (testName, collector) -> {
          Assertions.assertEquals(numBuckets, trackedBuckets(collector), StringUtils.format("%s: bucket count", testName));

          // trackedRows will equal numBuckets, because the collectors have been downsampled so much
          Assertions.assertEquals(numBuckets, trackedRows(collector), StringUtils.format("%s: row count", testName));

          for (int targetPartitionWeight : new int[]{1701, 2301}) {
            verifyPartitionsWithTargetWeight(
                StringUtils.format("%s: generatePartitionsWithTargetWeight(%d)", testName, targetPartitionWeight),
                collector,
                targetPartitionWeight,
                sortedKeyWeights,
                aggregate
            );
          }

          for (int maxPartitionCount : new int[]{1, 10, numBuckets, numBuckets + 1}) {
            if (maxPartitionCount < numBuckets) {
              final IllegalStateException e = Assertions.assertThrows(
                  IllegalStateException.class,
                  () -> collector.generatePartitionsWithMaxCount(maxPartitionCount)
              );

              ThrowableMatcher.of(IllegalStateException.class)
                             .expectMessage(message -> message.startsWith("Unable to compute partition ranges"))
                             .assertThat(e);
            } else {
              verifyPartitionsWithMaxCount(
                  StringUtils.format("%s: generatePartitionsWithMaxCount(%d)", testName, maxPartitionCount),
                  collector,
                  maxPartitionCount,
                  sortedKeyWeights,
                  aggregate
              );
            }
          }

          verifySnapshotSerialization(testName, collector);
        }
    );
  }

  @Test
  public void testShouldDownsampleSingleBucket()
  {
    ClusterByStatisticsCollectorImpl clusterByStatisticsCollector =
        (ClusterByStatisticsCollectorImpl) ClusterByStatisticsCollectorImpl.create(
            CLUSTER_BY_XYZ_BUCKET_BY_X,
            SIGNATURE,
            FrameType.latestRowBased(),
            35000,
            500,
            false,
            false
        );

    clusterByStatisticsCollector.add(createKey(CLUSTER_BY_XYZ_BUCKET_BY_X, 2, 1, "value1"), 1);
    clusterByStatisticsCollector.add(createKey(CLUSTER_BY_XYZ_BUCKET_BY_X, 2, 3, "value2"), 1);
    clusterByStatisticsCollector.add(createKey(CLUSTER_BY_XYZ_BUCKET_BY_X, 1, 1, "Extremely long key string for unit test; Extremely long key string for unit test;"), 500);

    Assertions.assertTrue(clusterByStatisticsCollector.getTotalRetainedBytes() <= 35000);
  }

  @Test
  public void testBucketDownsampledToSingleKeyFinishesCorrectly()
  {
    ClusterByStatisticsCollectorImpl clusterByStatisticsCollector = makeCollector(CLUSTER_BY_XYZ_BUCKET_BY_X, false);

    clusterByStatisticsCollector.add(createKey(CLUSTER_BY_XYZ_BUCKET_BY_X, 1, 1, "Extremely long key string for unit test; Extremely long key string for unit test;"), 2);
    clusterByStatisticsCollector.add(createKey(CLUSTER_BY_XYZ_BUCKET_BY_X, 2, 1, "b"), 2);

    clusterByStatisticsCollector.downSample();
  }

  @Test
  public void testMoreBucketsThanKeysThrowsException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () ->
      ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          0,
          5,
          false,
          false));
  }

  @Test
  public void testTooManyBuckets()
  {
    Assertions.assertThrows(TooManyBucketsException.class, () -> {
      ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          5,
          0,
          false,
          false);
      clusterByStatisticsCollector.add(RowKey.empty(), 1);
    });
  }

  @Test
  public void testGeneratePartitionWithoutAddCreatesUniversalPartition()
  {
    ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
                                                                                                        RowSignature.empty(),
                                                                                                        FrameType.latestRowBased(),
                                                                                                        5,
                                                                                                        0,
                                                                                                        false,
                                                                                                        false);
    Assertions.assertEquals(ClusterByPartitions.oneUniversalPartition(), clusterByStatisticsCollector.generatePartitionsWithTargetWeight(10));
  }

  @Test
  public void testGeneratePartitionWithZeroTargetWeightThrowsException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          5,
          0,
          false, false);
      clusterByStatisticsCollector.generatePartitionsWithTargetWeight(0);
    });
  }

  @Test
  public void testGeneratePartitionWithZeroCountThrowsException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          5,
          0,
          false,
          false);
      clusterByStatisticsCollector.generatePartitionsWithMaxCount(0);
    });
  }

  @Test
  public void testHasMultipleValuesFalseThrowsException()
  {
    Assertions.assertThrows(IllegalStateException.class, () -> {
      ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          5,
          0,
          false,
          false);
      clusterByStatisticsCollector.hasMultipleValues(0);
    });
  }

  @Test
  public void testHasMultipleValuesInvalidKeyThrowsException()
  {
    Assertions.assertThrows(ISE.class, () -> {
      ClusterByStatisticsCollector clusterByStatisticsCollector = ClusterByStatisticsCollectorImpl.create(ClusterBy.none(),
          RowSignature.empty(),
          FrameType.latestRowBased(),
          5,
          0,
          false,
          false);
      clusterByStatisticsCollector.hasMultipleValues(-1);
    });
  }

  private void doTest(
      final ClusterBy clusterBy,
      final boolean aggregate,
      final Iterable<RowKey> keys,
      final BiConsumer<String, ClusterByStatisticsCollectorImpl> testFn
  )
  {
    final Comparator<RowKey> comparator = clusterBy.keyComparator(SIGNATURE);

    // Load into single collector, sorted order.
    final ClusterByStatisticsCollectorImpl sortedCollector = makeCollector(clusterBy, aggregate);
    final List<RowKey> sortedKeys = Lists.newArrayList(keys);
    sortedKeys.sort(comparator);
    sortedKeys.forEach(k -> sortedCollector.add(k, 1));
    testFn.accept("single collector, sorted order", sortedCollector);

    // Load into single collector, reverse sorted order.
    final ClusterByStatisticsCollectorImpl reverseSortedCollector = makeCollector(clusterBy, aggregate);
    final List<RowKey> reverseSortedKeys = Lists.newArrayList(keys);
    reverseSortedKeys.sort(comparator.reversed());
    reverseSortedKeys.forEach(k -> reverseSortedCollector.add(k, 1));
    testFn.accept("single collector, reverse sorted order", reverseSortedCollector);

    // Randomized load into single collector.
    final ClusterByStatisticsCollectorImpl randomizedCollector = makeCollector(clusterBy, aggregate);
    final List<RowKey> randomizedKeys = Lists.newArrayList(keys);
    Collections.shuffle(randomizedKeys, new Random(7 /* Consistent seed from run to run */));
    randomizedKeys.forEach(k -> randomizedCollector.add(k, 1));
    testFn.accept("single collector, random order", randomizedCollector);

    // Split randomized load into three collectors of the same size, followed by merge.
    final List<ClusterByStatisticsCollectorImpl> threeEqualSizedCollectors = new ArrayList<>();
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeEqualSizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<RowKey> iterator1 = randomizedKeys.iterator();
    for (int i = 0; iterator1.hasNext(); i++) {
      final RowKey key = iterator1.next();
      threeEqualSizedCollectors.get(i % threeEqualSizedCollectors.size()).add(key, 1);
    }

    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(1)); // Regular add
    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(2).snapshot()); // Snapshot add

    testFn.accept("three merged collectors, equal sizes", threeEqualSizedCollectors.get(0));

    // Split randomized load into three collectors of different sizes, followed by merge.
    final List<ClusterByStatisticsCollectorImpl> threeDifferentlySizedCollectors = new ArrayList<>();
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));
    threeDifferentlySizedCollectors.add(makeCollector(clusterBy, aggregate));

    final Iterator<RowKey> iterator2 = randomizedKeys.iterator();
    for (int i = 0; iterator2.hasNext(); i++) {
      final RowKey key = iterator2.next();

      if (i % 100 < 2) {
        // 2% of space
        threeDifferentlySizedCollectors.get(0).add(key, 1);
      } else if (i % 100 < 20) {
        // 18% of space
        threeDifferentlySizedCollectors.get(1).add(key, 1);
      } else {
        // 80% of space
        threeDifferentlySizedCollectors.get(2).add(key, 1);
      }
    }

    threeDifferentlySizedCollectors.get(0).addAll(threeDifferentlySizedCollectors.get(1)); // Big into small
    threeDifferentlySizedCollectors.get(2).addAll(threeDifferentlySizedCollectors.get(0)); // Small into big

    testFn.accept("three merged collectors, different sizes", threeDifferentlySizedCollectors.get(2));
  }

  private ClusterByStatisticsCollectorImpl makeCollector(final ClusterBy clusterBy, final boolean aggregate)
  {
    return (ClusterByStatisticsCollectorImpl) ClusterByStatisticsCollectorImpl.create(
        clusterBy,
        SIGNATURE,
        FrameType.latestRowBased(),
        MAX_BYTES,
        MAX_BUCKETS,
        aggregate,
        false
    );
  }

  private static void verifyPartitions(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate,
      final long expectedPartitionSize
  )
  {
    final RowKeyReader keyReader = clusterBy.keyReader(SIGNATURE, FrameType.latestRowBased());

    final int expectedNumberOfBuckets =
        sortedKeyWeights.keySet()
                        .stream()
                        .map(key -> keyReader.trim(key, clusterBy.getBucketByCount()))
                        .collect(Collectors.toSet())
                        .size();

    verifyNumberOfBuckets(testName, clusterBy, partitions, expectedNumberOfBuckets);
    verifyPartitionsRespectBucketBoundaries(testName, clusterBy, partitions, sortedKeyWeights);
    verifyPartitionsCoverKeySpace(
        testName,
        partitions,
        sortedKeyWeights.firstKey(),
        clusterBy.keyComparator(SIGNATURE)
    );
    verifyPartitionWeights(testName, clusterBy, partitions, sortedKeyWeights, aggregate, expectedPartitionSize);
  }

  private static RowKey createKey(final ClusterBy clusterBy, final Object... objects)
  {
    return KeyTestUtils.createKey(
        KeyTestUtils.createKeySignature(clusterBy.getColumns(), SIGNATURE),
        FrameType.latestRowBased(),
        objects
    );
  }

  private static void verifyPartitionsWithTargetWeight(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int targetPartitionWeight,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate
  )
  {
    verifyPartitions(
        testName,
        collector.getClusterBy(),
        collector.generatePartitionsWithTargetWeight(targetPartitionWeight),
        sortedKeyWeights,
        aggregate,
        targetPartitionWeight
    );
  }

  private static void verifyPartitionsWithMaxCount(
      final String testName,
      final ClusterByStatisticsCollector collector,
      final int maxPartitionCount,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate
  )
  {
    final ClusterByPartitions partitions = collector.generatePartitionsWithMaxCount(maxPartitionCount);

    verifyPartitions(
        testName,
        collector.getClusterBy(),
        partitions,
        sortedKeyWeights,
        aggregate,
        LongMath.divide(
            totalWeight(sortedKeyWeights, new ClusterByPartition(null, null), aggregate),
            maxPartitionCount,
            RoundingMode.CEILING
        )
    );

    MatcherAssert.assertThat(
        StringUtils.format("%s: number of partitions ≤ max", testName),
        partitions.size(),
        Matchers.lessThanOrEqualTo(maxPartitionCount)
    );
  }

  private static void verifyNumberOfBuckets(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final int expectedNumberOfBuckets
  )
  {
    final RowKeyReader keyReader = clusterBy.keyReader(SIGNATURE, FrameType.latestRowBased());

    Assertions.assertEquals(
        expectedNumberOfBuckets,
        partitions.ranges()
                  .stream()
                  .map(partition -> keyReader.trim(partition.getStart(), clusterBy.getBucketByCount()))
                  .distinct()
                  .count(),
        StringUtils.format("%s: number of buckets", testName)
    );
  }

  /**
   * Verify that:
   *
   * - Partitions are all abutting
   * - The start of the first partition matches the minimum key (if there are keys)
   * - The end of the last partition is null
   * - Each partition's end is after its start
   */
  static void verifyPartitionsCoverKeySpace(
      final String testName,
      final ClusterByPartitions partitions,
      final RowKey expectedMinKey,
      final Comparator<RowKey> comparator
  )
  {
    Assertions.assertTrue(partitions.allAbutting(), StringUtils.format("%s: partitions abutting", testName));

    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);

      // Check expected nullness of the start key.
      if (i == 0) {
        Assertions.assertEquals(
            expectedMinKey,
            partition.getStart(),
            StringUtils.format("%s: partition %d: start is min key", testName, i)
        );
      } else {
        Assertions.assertNotNull(
            partition.getStart(),
            StringUtils.format("%s: partition %d: start is nonnull", testName, i)
        );
      }

      // Check expected nullness of the end key.
      if (i == ranges.size() - 1) {
        Assertions.assertNull(
            partition.getEnd(),
            StringUtils.format("%s: partition %d (final): end is null", testName, i)
        );
      } else {
        Assertions.assertNotNull(
            partition.getEnd(),
            StringUtils.format("%s: partition %d: end is nonnull", testName, i)
        );
      }

      // Check that the ends are all after the starts.
      if (partition.getStart() != null && partition.getEnd() != null) {
        MatcherAssert.assertThat(
            StringUtils.format("%s: partition %d: start compareTo end", testName, i),
            comparator.compare(partition.getStart(), partition.getEnd()),
            Matchers.lessThan(0)
        );
      }
    }
  }

  /**
   * Verify that no partition spans more than one bucket.
   */
  private static void verifyPartitionsRespectBucketBoundaries(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights
  )
  {
    final RowKeyReader keyReader = clusterBy.keyReader(SIGNATURE, FrameType.latestRowBased());
    final List<ClusterByPartition> ranges = partitions.ranges();

    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);
      final RowKey firstBucketKey = keyReader.trim(partition.getStart(), clusterBy.getBucketByCount());
      final RowKey lastBucketKey = keyReader.trim(
          partition.getEnd() == null
          ? sortedKeyWeights.lastKey()
          : sortedKeyWeights.subMap(partition.getStart(), true, partition.getEnd(), false).lastKey(),
          clusterBy.getBucketByCount()
      );

      Assertions.assertEquals(
          firstBucketKey,
          lastBucketKey,
          StringUtils.format("%s: partition %d: first, last bucket key are equal", testName, i)
      );
    }
  }

  /**
   * Verify that partitions have "reasonable" sizes.
   */
  static void verifyPartitionWeights(
      final String testName,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitions,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights,
      final boolean aggregate,
      final long expectedPartitionSize
  )
  {
    final RowKeyReader keyReader = clusterBy.keyReader(SIGNATURE, FrameType.latestRowBased());
    final List<ClusterByPartition> ranges = partitions.ranges();

    // Compute actual number of rows per partition.
    final Map<RowKey, Long> rowsPerPartition = new HashMap<>();

    for (final ClusterByPartition partition : partitions) {
      rowsPerPartition.put(
          partition.getStart(),
          totalWeight(sortedKeyWeights, partition, aggregate)
      );
    }

    // Compare actual size to desired size.
    for (int i = 0; i < ranges.size(); i++) {
      final ClusterByPartition partition = ranges.get(i);
      final RowKey bucketKey = keyReader.trim(partition.getStart(), clusterBy.getBucketByCount());
      final long actualNumberOfRows = rowsPerPartition.get(partition.getStart());

      // Reasonable maximum number of rows per partition.
      MatcherAssert.assertThat(
          StringUtils.format("%s: partition #%d: number of rows", testName, i),
          actualNumberOfRows,
          Matchers.lessThanOrEqualTo((long) ((1 + PARTITION_SIZE_LEEWAY) * expectedPartitionSize))
      );

      // Reasonable minimum number of rows per partition, for all partitions except the last in a bucket.
      // Our algorithm allows the last partition of each bucket to be extra-small.
      final boolean isLastInBucket =
          i == partitions.size() - 1
          || !keyReader.trim(partitions.get(i + 1).getStart(), clusterBy.getBucketByCount()).equals(bucketKey);

      if (!isLastInBucket) {
        MatcherAssert.assertThat(
            StringUtils.format("%s: partition #%d: number of rows", testName, i),
            actualNumberOfRows,
            Matchers.greaterThanOrEqualTo((long) ((1 - PARTITION_SIZE_LEEWAY) * expectedPartitionSize))
        );
      }
    }
  }

  static NavigableMap<RowKey, List<Integer>> computeSortedKeyWeightsFromWeightedKeys(
      final Iterable<Pair<RowKey, Integer>> keys,
      final Comparator<RowKey> comparator
  )
  {
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights = new TreeMap<>(comparator);

    for (final Pair<RowKey, Integer> key : keys) {
      sortedKeyWeights.computeIfAbsent(key.lhs, k -> new ArrayList<>()).add(key.rhs);
    }

    return sortedKeyWeights;
  }

  static NavigableMap<RowKey, List<Integer>> computeSortedKeyWeightsFromUnweightedKeys(
      final Iterable<RowKey> keys,
      final Comparator<RowKey> comparator
  )
  {
    return computeSortedKeyWeightsFromWeightedKeys(
        Iterables.transform(keys, key -> Pair.of(key, 1)),
        comparator
    );
  }

  static long totalWeight(
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights,
      final ClusterByPartition partition,
      final boolean aggregate
  )
  {
    final NavigableMap<RowKey, List<Integer>> partitionWeights =
        sortedKeyWeights.subMap(
            partition.getStart() == null ? sortedKeyWeights.firstKey() : partition.getStart(),
            true,
            partition.getEnd() == null ? sortedKeyWeights.lastKey() : partition.getEnd(),
            partition.getEnd() == null
        );

    long retVal = 0;

    for (final Collection<Integer> weights : partitionWeights.values()) {
      if (aggregate) {
        retVal += Collections.max(weights);
      } else {
        for (int w : weights) {
          retVal += w;
        }
      }
    }

    return retVal;
  }

  private static long trackedBuckets(final ClusterByStatisticsCollectorImpl collector)
  {
    return collector.getKeyCollectors().size();
  }

  private static long trackedRows(final ClusterByStatisticsCollectorImpl collector)
  {
    long count = 0;
    for (final KeyCollector<?> keyCollector : collector.getKeyCollectors()) {
      count += keyCollector.estimatedTotalWeight();
    }
    return count;
  }

  private static void verifySnapshotSerialization(
      final String testName,
      final ClusterByStatisticsCollector collector
  )
  {
    // Verify jackson serialization
    try {
      final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
      final ClusterByStatisticsSnapshot snapshot = collector.snapshot();
      final ClusterByStatisticsSnapshot snapshot2 = jsonMapper.readValue(
          jsonMapper.writeValueAsString(snapshot),
          ClusterByStatisticsSnapshot.class
      );

      Assertions.assertEquals(snapshot, snapshot2, StringUtils.format("%s: snapshot is serializable", testName));

      // Verify octet stream serialization
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ClusterByStatisticsSnapshotSerde.serialize(byteArrayOutputStream, snapshot);

      final ClusterByStatisticsSnapshot snapshot3 = ClusterByStatisticsSnapshotSerde.deserialize(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      Assertions.assertEquals(snapshot, snapshot3, StringUtils.format("%s: snapshot is serializable", testName));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
