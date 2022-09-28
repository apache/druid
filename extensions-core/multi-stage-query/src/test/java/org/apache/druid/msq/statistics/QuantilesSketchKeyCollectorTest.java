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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class QuantilesSketchKeyCollectorTest
{
  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new SortColumn("x", false)), 0);
  private final Comparator<RowKey> comparator = clusterBy.keyComparator();
  private final int numKeys = 500_000;

  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void test_empty()
  {
    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        Collections.emptyList(),
        comparator,
        (testName, collector) -> {
          Assert.assertTrue(collector.isEmpty());
          Assert.assertThrows(NoSuchElementException.class, collector::minKey);
          Assert.assertEquals(testName, 0, collector.estimatedTotalWeight());
          Assert.assertEquals(
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
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(testName, numKeys, collector.estimatedTotalWeight());
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_unweighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(testName, numKeys, collector.estimatedTotalWeight());
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void test_uniformRandomKeys_unweighted_downSampledToSmallestSize()
  {
    final List<Pair<RowKey, Integer>> keyWeights = KeyCollectorTestUtils.uniformRandomKeys(numKeys);
    final RowKey finalMinKey =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator).firstKey();

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        keyWeights,
        comparator,
        (testName, collector) -> {
          while (collector.downSample()) {
            // Intentionally empty loop body.
          }

          Assert.assertEquals(testName, 2, collector.getSketch().getK());
          Assert.assertEquals(testName, 14, collector.estimatedRetainedKeys());

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
  public void test_uniformRandomKeys_barbellWeighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomBarbellWeightedKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(
              testName,
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  false
              ),
              collector.estimatedTotalWeight()
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  @Test
  public void testAverageKeyLength()
  {
    final QuantilesSketchKeyCollector collector = QuantilesSketchKeyCollectorFactory.create(clusterBy).newKeyCollector();

    final QuantilesSketchKeyCollector other = QuantilesSketchKeyCollectorFactory.create(clusterBy).newKeyCollector();

    RowSignature smallKeySignature = KeyTestUtils.createKeySignature(
        new ClusterBy(ImmutableList.of(new SortColumn("x", false)), 0).getColumns(),
        RowSignature.builder().add("x", ColumnType.LONG).build()
    );
    RowKey smallKey = KeyTestUtils.createKey(smallKeySignature, 1L);

    RowSignature largeKeySignature = KeyTestUtils.createKeySignature(
        new ClusterBy(
            ImmutableList.of(
                new SortColumn("x", false),
                new SortColumn("y", false),
                new SortColumn("z", false)
            ),
            0).getColumns(),
        RowSignature.builder()
                    .add("x", ColumnType.LONG)
                    .add("y", ColumnType.LONG)
                    .add("z", ColumnType.LONG)
                    .build()
    );
    RowKey largeKey = KeyTestUtils.createKey(largeKeySignature, 1L, 2L, 3L);


    collector.add(smallKey, 3);
    Assert.assertEquals(smallKey.getNumberOfBytes(), collector.getAverageKeyLength(), 0);

    other.add(largeKey, 5);
    Assert.assertEquals(largeKey.getNumberOfBytes(), other.getAverageKeyLength(), 0);

    collector.addAll(other);
    Assert.assertEquals((smallKey.getNumberOfBytes() * 3 + largeKey.getNumberOfBytes() * 5) / 8.0, collector.getAverageKeyLength(), 0);
  }

  @Test
  public void test_uniformRandomKeys_inverseBarbellWeighted()
  {
    final List<Pair<RowKey, Integer>> keyWeights =
        KeyCollectorTestUtils.uniformRandomInverseBarbellWeightedKeys(numKeys);
    final NavigableMap<RowKey, List<Integer>> sortedKeyWeights =
        ClusterByStatisticsCollectorImplTest.computeSortedKeyWeightsFromWeightedKeys(keyWeights, comparator);

    KeyCollectorTestUtils.doTest(
        QuantilesSketchKeyCollectorFactory.create(clusterBy),
        keyWeights,
        comparator,
        (testName, collector) -> {
          Assert.assertEquals(
              testName,
              ClusterByStatisticsCollectorImplTest.totalWeight(
                  sortedKeyWeights,
                  new ClusterByPartition(null, null),
                  false
              ),
              collector.estimatedTotalWeight()
          );
          verifyCollector(collector, clusterBy, comparator, sortedKeyWeights);
        }
    );
  }

  private static void verifyCollector(
      final QuantilesSketchKeyCollector collector,
      final ClusterBy clusterBy,
      final Comparator<RowKey> comparator,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights
  )
  {
    KeyCollectorTestUtils.verifyCollector(
        collector,
        clusterBy,
        comparator,
        sortedKeyWeights
    );
  }
}
