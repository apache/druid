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
import com.google.common.collect.Lists;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.function.BiConsumer;

public class KeyCollectorTestUtils
{
  private KeyCollectorTestUtils()
  {
    // No instantiation.
  }

  static <TCollector extends KeyCollector<TCollector>, TSnapshot extends KeyCollectorSnapshot> void doTest(
      final KeyCollectorFactory<TCollector, TSnapshot> keyCollectorFactory,
      final Iterable<Pair<RowKey, Integer>> keys,
      final Comparator<RowKey> comparator,
      final BiConsumer<String, TCollector> testFn
  )
  {
    // Load into single collector, sorted order.
    final TCollector sortedCollector = keyCollectorFactory.newKeyCollector();
    final List<Pair<RowKey, Integer>> sortedKeys = Lists.newArrayList(keys);
    sortedKeys.sort(Comparator.comparing(pair -> pair.lhs, comparator));
    sortedKeys.forEach(pair -> sortedCollector.add(pair.lhs, pair.rhs));
    testFn.accept("single collector, sorted order", sortedCollector);

    // Load into single collector, reverse sorted order.
    final TCollector reverseSortedCollector = keyCollectorFactory.newKeyCollector();
    Lists.reverse(sortedKeys).forEach(key -> reverseSortedCollector.add(key.lhs, key.rhs));
    testFn.accept("single collector, reverse sorted order", reverseSortedCollector);

    // Randomized load into single collector.
    final TCollector randomizedCollector = keyCollectorFactory.newKeyCollector();
    final List<Pair<RowKey, Integer>> randomizedKeys = Lists.newArrayList(keys);
    Collections.shuffle(randomizedKeys, new Random(7 /* Consistent seed from run to run */));
    randomizedKeys.forEach(pair -> randomizedCollector.add(pair.lhs, pair.rhs));
    testFn.accept("single collector, random order", randomizedCollector);

    // Split randomized load into three collectors of the same size, followed by merge.
    final List<TCollector> threeEqualSizedCollectors = new ArrayList<>();
    threeEqualSizedCollectors.add(keyCollectorFactory.newKeyCollector());
    threeEqualSizedCollectors.add(keyCollectorFactory.newKeyCollector());
    threeEqualSizedCollectors.add(keyCollectorFactory.newKeyCollector());

    final Iterator<Pair<RowKey, Integer>> iterator1 = randomizedKeys.iterator();
    for (int i = 0; iterator1.hasNext(); i++) {
      final Pair<RowKey, Integer> key = iterator1.next();
      threeEqualSizedCollectors.get(i % threeEqualSizedCollectors.size()).add(key.lhs, key.rhs);
    }

    // Regular add
    threeEqualSizedCollectors.get(0).addAll(threeEqualSizedCollectors.get(1));

    // Snapshot add
    threeEqualSizedCollectors.get(0).addAll(
        keyCollectorFactory.fromSnapshot(keyCollectorFactory.toSnapshot(threeEqualSizedCollectors.get(2)))
    );

    testFn.accept("three merged collectors, equal sizes", threeEqualSizedCollectors.get(0));

    // Split randomized load into three collectors of different sizes, followed by merge.
    final List<TCollector> threeDifferentlySizedCollectors = new ArrayList<>();
    threeDifferentlySizedCollectors.add(keyCollectorFactory.newKeyCollector());
    threeDifferentlySizedCollectors.add(keyCollectorFactory.newKeyCollector());
    threeDifferentlySizedCollectors.add(keyCollectorFactory.newKeyCollector());

    boolean didDownsampleLargeCollector = false;
    final Iterator<Pair<RowKey, Integer>> iterator2 = randomizedKeys.iterator();
    for (int i = 0; iterator2.hasNext(); i++) {
      final Pair<RowKey, Integer> key = iterator2.next();

      if (i % 100 < 2) {
        // 2% of space
        threeDifferentlySizedCollectors.get(0).add(key.lhs, key.rhs);
      } else if (i % 100 < 20) {
        // 18% of space
        threeDifferentlySizedCollectors.get(1).add(key.lhs, key.rhs);
      } else {
        // 80% of space
        threeDifferentlySizedCollectors.get(2).add(key.lhs, key.rhs);

        // Downsample once during the add process.
        if (!didDownsampleLargeCollector) {
          threeDifferentlySizedCollectors.get(2).downSample();
          didDownsampleLargeCollector = true;
        }
      }
    }

    // Downsample medium, large collectors (so: two total times for the large one).
    threeDifferentlySizedCollectors.get(1).downSample();
    threeDifferentlySizedCollectors.get(2).downSample();

    threeDifferentlySizedCollectors.get(0).addAll(threeDifferentlySizedCollectors.get(1)); // Big into small
    threeDifferentlySizedCollectors.get(2).addAll(threeDifferentlySizedCollectors.get(0)); // Small into big

    testFn.accept(
        "three merged collectors, different sizes, with downsampling",
        threeDifferentlySizedCollectors.get(2)
    );
  }

  static void verifyCollector(
      final KeyCollector<?> collector,
      final ClusterBy clusterBy,
      final Comparator<RowKey> comparator,
      final NavigableMap<RowKey, List<Integer>> sortedKeyWeights
  )
  {
    Assert.assertEquals(sortedKeyWeights.isEmpty() ? null : sortedKeyWeights.firstKey(), collector.minKey());

    for (int targetWeight : new int[]{10_000, 50_000, 300_000}) {
      final ClusterByPartitions partitions = collector.generatePartitionsWithTargetWeight(targetWeight);
      final String testName = StringUtils.format("target weight = %,d", targetWeight);

      ClusterByStatisticsCollectorImplTest.verifyPartitionsCoverKeySpace(
          testName,
          partitions,
          sortedKeyWeights.firstKey(),
          comparator
      );

      ClusterByStatisticsCollectorImplTest.verifyPartitionWeights(
          testName,
          clusterBy,
          partitions,
          sortedKeyWeights,
          collector instanceof DistinctKeyCollector,
          targetWeight
      );
    }
  }

  /**
   * Generates sequential keys from the range {@code [0, numKeys)}.
   */
  static List<Pair<RowKey, Integer>> sequentialKeys(final int numKeys)
  {
    final List<Pair<RowKey, Integer>> keyWeights = new ArrayList<>();

    for (int i = 0; i < numKeys; i++) {
      final RowKey key = createSingleLongKey((long) i);
      keyWeights.add(Pair.of(key, 1));
    }

    return keyWeights;
  }

  /**
   * Generates a certain number of keys drawn from a uniform random distribution on {@code [0, numKeys)}.
   */
  static List<Pair<RowKey, Integer>> uniformRandomKeys(final int numKeys)
  {
    final List<Pair<RowKey, Integer>> keyWeights = new ArrayList<>();

    // Fixed seed; "random" but deterministic.
    final Random random = new Random(0);

    for (int i = 0; i < numKeys; i++) {
      final long keyNumber = random.nextInt(numKeys);
      final RowKey key = createSingleLongKey(keyNumber); // Uniformly random
      keyWeights.add(Pair.of(key, 1));
    }

    return keyWeights;
  }

  /**
   * Generates a certain number of keys drawn from a uniform random distribution on {@code [0, numKeys)}. A contiguous
   * 10% of the keyspace on either end is "heavyweight" and the rest of the keyspace is "lightweight".
   */
  static List<Pair<RowKey, Integer>> uniformRandomBarbellWeightedKeys(final int numKeys)
  {
    final int firstTenPercent = numKeys / 10;
    final int lastTenPercent = numKeys * 9 / 10;

    final List<Pair<RowKey, Integer>> keyWeights = new ArrayList<>();

    // Fixed seed; "random" but deterministic.
    final Random random = new Random(0);

    for (int i = 0; i < numKeys; i++) {
      final long keyNumber = random.nextInt(numKeys);
      final RowKey key = createSingleLongKey(keyNumber); // Uniformly random
      final int weight = keyNumber < firstTenPercent && keyNumber >= lastTenPercent ? 3 : 1;
      keyWeights.add(Pair.of(key, weight));
    }

    return keyWeights;
  }

  /**
   * Generates a certain number of keys drawn from a uniform random distribution on {@code [0, numKeys)}. A contiguous
   * 10% of the keyspace on either end is "lightweight" and the rest of the keyspace is "heavyweight".
   */
  static List<Pair<RowKey, Integer>> uniformRandomInverseBarbellWeightedKeys(final int numKeys)
  {
    final int firstTenPercent = numKeys / 10;
    final int lastTenPercent = numKeys * 9 / 10;

    final List<Pair<RowKey, Integer>> keyWeights = new ArrayList<>();

    // Fixed seed; "random" but deterministic.
    final Random random = new Random(0);

    for (int i = 0; i < numKeys; i++) {
      final long keyNumber = random.nextInt(numKeys);
      final RowKey key = createSingleLongKey(keyNumber); // Uniformly random
      final int weight = keyNumber >= firstTenPercent && keyNumber < lastTenPercent ? 3 : 1;
      keyWeights.add(Pair.of(key, weight));
    }

    return keyWeights;
  }

  /**
   * Generates a certain number of keys drawn from a nonuniform random distribution on
   * {@code [0, numKeys) ∪ {100, 201, 302, 403}}. The keys 100, 201, 302, and 403 are much more likely to occur than
   * any other keys.
   */
  static List<Pair<RowKey, Integer>> nonUniformRandomKeys(final int numKeys)
  {
    final List<Pair<RowKey, Integer>> keyWeights = new ArrayList<>();

    // Fixed seed; "random" but deterministic.
    final Random random = new Random(0);

    for (int i = 0; i < numKeys; i++) {
      final long randomNumber = random.nextInt(numKeys * 10);
      final long keyNumber;

      if (randomNumber < numKeys) {
        keyNumber = randomNumber;
      } else if (randomNumber < numKeys * 5L) {
        keyNumber = 100;
      } else if (randomNumber < numKeys * 8L) {
        keyNumber = 201;
      } else if (randomNumber < numKeys * 9L) {
        keyNumber = 302;
      } else {
        keyNumber = 403;
      }

      final RowKey key = createSingleLongKey(keyNumber);
      keyWeights.add(Pair.of(key, 1));
    }

    return keyWeights;
  }

  private static RowKey createSingleLongKey(final long n)
  {
    final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
    final List<SortColumn> sortColumns = ImmutableList.of(new SortColumn("x", false));
    final RowSignature keySignature = KeyTestUtils.createKeySignature(sortColumns, signature);
    return KeyTestUtils.createKey(keySignature, n);
  }
}
