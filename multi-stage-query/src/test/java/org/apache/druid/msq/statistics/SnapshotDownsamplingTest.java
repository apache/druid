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
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2LongRBTreeMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link KeyCollectorSnapshot#downsampleTo}, which workers use to drop the keys the controller would drop anyway.
 */
public class SnapshotDownsamplingTest
{
  private static final int KEYS_PER_WORKER = 3_000;

  private final ClusterBy clusterBy = new ClusterBy(ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING)), 0);
  private final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
  private final Comparator<RowKey> comparator = clusterBy.keyComparator(signature);
  private final DistinctKeyCollectorFactory factory = DistinctKeyCollectorFactory.create(clusterBy, signature);

  @Test
  public void test_downsample_keepsFirstKeyAndSelectedKeys()
  {
    final List<Pair<RowKey, Integer>> keys = KeyCollectorTestUtils.uniformRandomKeys(KEYS_PER_WORKER);
    final DistinctKeySnapshot snapshot = factory.toSnapshot(collectorFrom(keys, 0));
    final DistinctKeySnapshot downsampled = snapshot.downsampleTo(3);

    Assertions.assertEquals(3, downsampled.getSpaceReductionFactor());
    Assertions.assertTrue(downsampled.getKeys().size() < snapshot.getKeys().size());
    Assertions.assertEquals(snapshot.getKeys().get(0), downsampled.getKeys().get(0));

    for (int i = 1; i < downsampled.getKeys().size(); i++) {
      Assertions.assertTrue(Long.numberOfTrailingZeros(downsampled.getKeys().get(i).lhs.longHashCode()) >= 3);
    }

    // Same keys as a collector that was at the lower factor from the start.
    Assertions.assertEquals(
        factory.toSnapshot(collectorFrom(keys, 3)),
        downsampled
    );
  }

  @Test
  public void test_downsample_noOpAtOrBelowOwnFactor()
  {
    final DistinctKeySnapshot snapshot = factory.toSnapshot(workerCollector(4));

    Assertions.assertSame(snapshot, snapshot.downsampleTo(0));
    Assertions.assertSame(snapshot, snapshot.downsampleTo(4));
  }

  @Test
  public void test_mergeOfDownsampledSnapshots_matchesMergeOfFullSnapshots()
  {
    // Workers with different factors, as workers with different numbers of segments or memory would have.
    final int[] workerFactors = {0, 1, 1, 2, 5, 0, 3};
    final List<DistinctKeySnapshot> snapshots = new ArrayList<>();
    int maxFactor = 0;
    for (final int workerFactor : workerFactors) {
      snapshots.add(factory.toSnapshot(workerCollector(workerFactor)));
      maxFactor = Math.max(maxFactor, workerFactor);
    }

    final DistinctKeyCollector fromFull = factory.newKeyCollector();
    final DistinctKeyCollector fromDownsampled = factory.newKeyCollector();
    long fullKeys = 0;
    long downsampledKeys = 0;
    for (final DistinctKeySnapshot snapshot : snapshots) {
      final DistinctKeySnapshot downsampled = snapshot.downsampleTo(maxFactor);
      fullKeys += snapshot.getKeys().size();
      downsampledKeys += downsampled.getKeys().size();
      fromFull.addAll(factory.fromSnapshot(snapshot));
      fromDownsampled.addAll(factory.fromSnapshot(downsampled));
    }

    Assertions.assertEquals(fromFull.getSpaceReductionFactor(), fromDownsampled.getSpaceReductionFactor());
    Assertions.assertEquals(toMap(fromFull), toMap(fromDownsampled));
    Assertions.assertEquals(fromFull.estimatedTotalWeight(), fromDownsampled.estimatedTotalWeight());
    Assertions.assertEquals(fromFull.minKey(), fromDownsampled.minKey());

    // Most of what the workers would have sent is dropped.
    Assertions.assertTrue(
        downsampledKeys < fullKeys / 4,
        "downsampled[" + downsampledKeys + "] full[" + fullKeys + "]"
    );
  }

  @Test
  public void test_delegateOrMin_downsamplesDelegateAndLeavesMinKeyAlone()
  {
    final DistinctKeySnapshot distinct = factory.toSnapshot(workerCollector(1));
    final DelegateOrMinKeyCollectorSnapshot<DistinctKeySnapshot> withDelegate =
        new DelegateOrMinKeyCollectorSnapshot<>(distinct, null);

    Assertions.assertEquals(1, withDelegate.spaceReductionFactorOrZero());
    final KeyCollectorSnapshot downsampled = withDelegate.downsampleTo(4);
    Assertions.assertEquals(4, downsampled.spaceReductionFactorOrZero());
    Assertions.assertEquals(distinct.downsampleTo(4), ((DelegateOrMinKeyCollectorSnapshot<?>) downsampled).getSnapshot());

    final RowKey minKey = distinct.getKeys().get(0).lhs;
    final DelegateOrMinKeyCollectorSnapshot<DistinctKeySnapshot> minOnly =
        new DelegateOrMinKeyCollectorSnapshot<>(null, minKey);
    Assertions.assertEquals(0, minOnly.spaceReductionFactorOrZero());
    Assertions.assertSame(minOnly, minOnly.downsampleTo(4));
  }

  @Test
  public void test_clusterByStatisticsSnapshot_partialKeyStatisticsAndDownsample()
  {
    final DistinctKeySnapshot low = factory.toSnapshot(workerCollector(1));
    final DistinctKeySnapshot high = factory.toSnapshot(workerCollector(3));
    final RowKey bucketKey = RowKey.empty();

    final ClusterByStatisticsSnapshot snapshot = new ClusterByStatisticsSnapshot(
        ImmutableMap.of(
            0L, new ClusterByStatisticsSnapshot.Bucket(bucketKey, new DelegateOrMinKeyCollectorSnapshot<>(low, null), 1),
            1L, new ClusterByStatisticsSnapshot.Bucket(bucketKey, new DelegateOrMinKeyCollectorSnapshot<>(high, null), 1)
        ),
        null
    );

    Assertions.assertEquals(3, snapshot.partialKeyStatistics().getSpaceReductionFactor());

    final ClusterByStatisticsSnapshot downsampled = snapshot.downsampleTo(5);
    Assertions.assertEquals(5, downsampled.partialKeyStatistics().getSpaceReductionFactor());
    Assertions.assertEquals(snapshot.getBuckets().keySet(), downsampled.getBuckets().keySet());
    Assertions.assertSame(snapshot, snapshot.downsampleTo(0));
  }

  @Test
  public void test_quantilesSnapshot_isLeftAlone()
  {
    final QuantilesSketchKeyCollectorFactory quantilesFactory =
        QuantilesSketchKeyCollectorFactory.create(clusterBy, signature);
    final QuantilesSketchKeyCollector collector = quantilesFactory.newKeyCollector();
    for (final Pair<RowKey, Integer> key : KeyCollectorTestUtils.sequentialKeys(100)) {
      collector.add(key.lhs, key.rhs);
    }
    final QuantilesSketchKeyCollectorSnapshot snapshot = quantilesFactory.toSnapshot(collector);

    Assertions.assertEquals(0, snapshot.spaceReductionFactorOrZero());
    Assertions.assertSame(snapshot, snapshot.downsampleTo(5));
  }

  /**
   * Collector that starts at the given factor and then takes random keys, like a worker's collector that downsampled
   * to fit its memory.
   */
  private DistinctKeyCollector workerCollector(final int spaceReductionFactor)
  {
    return collectorFrom(KeyCollectorTestUtils.uniformRandomKeys(KEYS_PER_WORKER), spaceReductionFactor);
  }

  private DistinctKeyCollector collectorFrom(final List<Pair<RowKey, Integer>> keys, final int spaceReductionFactor)
  {
    final DistinctKeyCollector collector = new DistinctKeyCollector(
        comparator,
        new Object2LongRBTreeMap<>(comparator),
        spaceReductionFactor
    );
    for (final Pair<RowKey, Integer> key : keys) {
      collector.add(key.lhs, key.rhs);
    }
    return collector;
  }

  private static Map<RowKey, Long> toMap(final DistinctKeyCollector collector)
  {
    return new HashMap<>(collector.getRetainedKeys());
  }
}
