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

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2LongRBTreeMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class DistinctKeyCollectorTest
{
  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING)), 0);
  private final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
  private final Comparator<RowKey> comparator = clusterBy.keyComparator(signature);
  private final int numKeys = 100_000;

  @Test
  public void test_empty()
  {
    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        Collections.emptyList(),
        comparator,
        (testName, collector) -> {
          Assertions.assertTrue(collector.isEmpty());
          Assertions.assertThrows(NoSuchElementException.class, collector::minKey);
          Assertions.assertEquals(0, collector.estimatedTotalWeight(), testName);
          Assertions.assertEquals(
              ClusterByPartitions.oneUniversalPartition(),
              collector.generatePartitionsWithTargetWeight(1000)
          );
        }
    );
  }

  @Test
  public void test_sequentialKeys_unweighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.sequentialKeys(numKeys);

    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assertions.assertEquals(numKeys, collector.estimatedTotalWeight(), numKeys * 0.05);
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_single_key_addition()
  {
    DistinctKeyCollector distinctKeyCollector = new DistinctKeyCollector(
        clusterBy.keyComparator(signature),
        new Object2LongRBTreeMap<>(comparator),
        2
    );
    List<Pair<RowKey, Integer>> pairs = KeyCollectorTestUtils.sequentialKeys(2);

    distinctKeyCollector.add(pairs.get(1).lhs, pairs.get(1).rhs);
    distinctKeyCollector.downSample();
    distinctKeyCollector.add(pairs.get(0).lhs, pairs.get(0).rhs);
  }

  @Test
  public void test_uniformRandomKeys_unweighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assertions.assertEquals(
              sortedKeyWeights.size(),
              collector.estimatedTotalWeight(),
              sortedKeyWeights.size() * 0.05,
              testName
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_generateWithNegativeTargetWeight_throwsException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      DistinctKeyCollector distinctKeyCollector = DistinctKeyCollectorFactory.create(clusterBy, signature).newKeyCollector();
      distinctKeyCollector.generatePartitionsWithTargetWeight(-1);
    });
  }

  @Test
  public void test_uniformRandomKeys_unweighted_downSampledToOneKey()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final RowKey finalMinKey =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator).firstKey();

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          while (collector.downSample()) {
            // Intentionally empty loop body.
          }

          Assertions.assertTrue(DistinctKeyCollector.SMALLEST_MAX_BYTES >= collector.getMaxBytes());
          Assertions.assertTrue(
              (int) collector.estimatedRetainedBytes() <= DistinctKeyCollector.SMALLEST_MAX_BYTES,
              testName
          );

          // Don't use verifyCollector, since this collector is downsampled so aggressively that it can't possibly
          // hope to pass those tests. Grade on a curve.
          final ClusterByPartitions partitions = collector.generatePartitionsWithTargetWeight(10_000);
          ClusterByStatisticsCollectorImplTest.verifyPartitionsCoverKeySpace(
              testName,
              partitions,
              finalMinKey,
              comparator
          );
        }
    );
  }

  @Test
  public void test_nonUniformRandomKeys_unweighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.nonUniformRandomKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assertions.assertEquals(
              sortedKeyWeights.size(),
              collector.estimatedTotalWeight(),
              sortedKeyWeights.size() * 0.05
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_barbellWeighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomBarbellWeightedKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assertions.assertEquals(
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  true
              ),
              collector.estimatedTotalWeight(),
              sortedKeyWeights.size() * 0.05
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_inverseBarbellWeighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomInverseBarbellWeightedKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        DistinctKeyCollectorFactory.create(clusterBy, signature),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assertions.assertEquals(
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  true
              ),
              collector.estimatedTotalWeight(),
              sortedKeyWeights.size() * 0.05
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_addSnapshot_sameAsAddAllOfCollectorFromSnapshot()
  {
    final DistinctKeyCollectorFactory factory = DistinctKeyCollectorFactory.create(clusterBy, signature);
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);

    // Snapshots at different spaceReductionFactors, so merging them raises the merged collector's factor, and some
    // snapshots are merged into a collector with a higher factor than their own.
    final int[] spaceReductionFactors = {0, 3, 1, 5, 2};
    final List<DistinctKeySnapshot> snapshots = new ArrayList<>();
    for (int i = 0; i < spaceReductionFactors.length; i++) {
      final DistinctKeyCollector collector = factory.newKeyCollector();
      for (int j = i; j < keyWeights.size(); j += spaceReductionFactors.length) {
        collector.add(keyWeights.get(j).lhs, keyWeights.get(j).rhs);
      }
      while (collector.getSpaceReductionFactor() < spaceReductionFactors[i]) {
        Assertions.assertTrue(collector.downSample());
      }
      snapshots.add(factory.toSnapshot(collector));
    }

    final DistinctKeyCollector direct = factory.newKeyCollector();
    final DistinctKeyCollector fromSnapshots = factory.newKeyCollector();
    for (final DistinctKeySnapshot snapshot : snapshots) {
      factory.addSnapshot(direct, snapshot);
      fromSnapshots.addAll(factory.fromSnapshot(snapshot));

      Assertions.assertEquals(fromSnapshots.getRetainedKeys(), direct.getRetainedKeys());
      Assertions.assertEquals(fromSnapshots.getSpaceReductionFactor(), direct.getSpaceReductionFactor());
      Assertions.assertEquals(fromSnapshots.getMaxBytes(), direct.getMaxBytes());
      Assertions.assertEquals(fromSnapshots.estimatedTotalWeight(), direct.estimatedTotalWeight());
      Assertions.assertEquals(fromSnapshots.estimatedRetainedBytes(), direct.estimatedRetainedBytes());
      Assertions.assertEquals(retainedKeyBytes(direct), direct.estimatedRetainedBytes());
    }

    Assertions.assertEquals(5, direct.getSpaceReductionFactor());
  }

  @Test
  public void test_retainedBytes_afterAddAllIntoEmptyCollector()
  {
    final DistinctKeyCollectorFactory factory = DistinctKeyCollectorFactory.create(clusterBy, signature);
    final DistinctKeyCollector collector = factory.newKeyCollector();
    KeyCollectorTestUtils.uniformRandomKeys(numKeys).forEach(pair -> collector.add(pair.lhs, pair.rhs));

    final DistinctKeyCollector fromSnapshot = factory.fromSnapshot(factory.toSnapshot(collector));
    Assertions.assertEquals(collector.estimatedRetainedBytes(), fromSnapshot.estimatedRetainedBytes());

    final DistinctKeyCollector merged = factory.newKeyCollector();
    merged.addAll(fromSnapshot);
    Assertions.assertEquals(collector.estimatedRetainedBytes(), merged.estimatedRetainedBytes());

    // Downsampling subtracts the bytes of the keys it drops, which made the count negative when merging into an empty
    // collector did not count the bytes of the keys it copied.
    while (merged.getSpaceReductionFactor() < 3) {
      Assertions.assertTrue(merged.downSample());
    }
    Assertions.assertEquals(retainedKeyBytes(merged), merged.estimatedRetainedBytes());
  }

  private static void verifyCollector(
      final DistinctKeyCollector collector,
      final ClusterBy clusterBy,
      final Comparator<RowKey> comparator,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights
  )
  {
    Assertions.assertTrue((int) collector.estimatedRetainedBytes() < collector.getMaxBytes());
    Assertions.assertEquals(retainedKeyBytes(collector), collector.estimatedRetainedBytes());

    KeyCollectorTestUtils.verifyCollector(
        collector,
        clusterBy,
        comparator,
        sortedKeyWeights
    );
  }

  private static long retainedKeyBytes(final DistinctKeyCollector collector)
  {
    long bytes = 0;
    for (final RowKey key : collector.getRetainedKeys().keySet()) {
      bytes += key.estimatedObjectSizeBytes();
    }
    return bytes;
  }
}
