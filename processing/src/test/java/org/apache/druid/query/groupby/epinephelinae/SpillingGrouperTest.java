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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpillingGrouperTest extends InitializedNullHandlingTest
{
  private static final AggregatorFactory[] AGGREGATOR_FACTORIES = new AggregatorFactory[]{
      new LongSumAggregatorFactory("valueSum", "value"),
      new CountAggregatorFactory("count")
  };
  private static final int KEY_SIZE = new IntKeySerde().keySize();
  private static final float MAX_LOAD_FACTOR = 0.75f;
  private static final int INITIAL_BUCKETS = 4;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testNoSpilling() throws IOException
  {
    final File storageDir = temporaryFolder.newFolder();
    //  Only 3 keys with a 10,000-byte buffer. Everything fits in memory
    try (SpillingGrouper<IntKey> grouper = makeGrouper(10000, storageDir, 1024 * 1024, 100)) {
      for (int i = 0; i < 3; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      assertResultsCorrect(grouper, 3, 1);
      Assert.assertEquals(0, storageDir.listFiles().length);
    }
  }

  @Test
  public void testSpillAndIterateSorted() throws IOException
  {
    final File storageDir = temporaryFolder.newFolder();
    final int numKeys = 100;
    // 100 unique keys force many spills since buffer is only 50 bytes. With iterator(true), results should be sorted ascending by key.
    try (SpillingGrouper<IntKey> grouper = makeGrouper(50, storageDir, 1024 * 1024, 100)) {
      for (int i = 0; i < numKeys; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      try (CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true)) {
        Assert.assertTrue("spilling should have occurred", storageDir.listFiles().length > 0);
        int prevKey = -1;
        int count = 0;
        while (iterator.hasNext()) {
          Grouper.Entry<IntKey> entry = iterator.next();
          Assert.assertTrue(
              "keys should be sorted ascending",
              entry.getKey().intValue() > prevKey
          );
          prevKey = entry.getKey().intValue();
          Assert.assertEquals(1L, entry.getValues()[0]);
          Assert.assertEquals(1L, entry.getValues()[1]);
          count++;
        }
        Assert.assertEquals(numKeys, count);
      }
    }
  }

  @Test
  public void testSpillAndIterateUnsorted() throws IOException
  {
    final File storageDir = temporaryFolder.newFolder();
    final int numKeys = 100;
    // 100 unique keys force many spills since buffer is only 50 bytes. With iterator(false), results may be in any order, but all keys should be present with correct values.
    try (SpillingGrouper<IntKey> grouper = makeGrouper(50, storageDir, 1024 * 1024, 100)) {
      for (int i = 0; i < numKeys; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      assertResultsCorrect(grouper, numKeys, 1);
      Assert.assertTrue("spilling should have occurred", storageDir.listFiles().length > 0);
    }
  }

  @Test
  public void testAggregatesDuplicateKeys() throws IOException
  {
    // SpillingGrouper doesn't combine across spills — duplicate keys from different spill files
    // appear as separate entries in the sorted iterator. Verify that the total aggregate values
    // per key sum to the expected amount across all entries.
    final File storageDir = temporaryFolder.newFolder();
    final int numKeys = 20;
    final int duplicates = 5;
    try (SpillingGrouper<IntKey> grouper = makeGrouper(50, storageDir, 1024 * 1024, 100)) {
      for (int round = 0; round < duplicates; round++) {
        for (int i = 0; i < numKeys; i++) {
          Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
        }
      }

      int totalEntries = 0;
      final Map<Integer, Long> totalCounts = new HashMap<>();
      try (CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true)) {
        Assert.assertTrue("spilling should have occurred", storageDir.listFiles().length > 0);
        while (iterator.hasNext()) {
          Grouper.Entry<IntKey> entry = iterator.next();
          totalCounts.merge(entry.getKey().intValue(), (Long) entry.getValues()[1], Long::sum);
          totalEntries++;
        }
      }
      Assert.assertTrue(
          "duplicate keys should exist across spills, so total entries (" + totalEntries
          + ") should exceed unique key count (" + numKeys + ")",
          totalEntries > numKeys
      );
      Assert.assertEquals(numKeys, totalCounts.size());
      for (Map.Entry<Integer, Long> e : totalCounts.entrySet()) {
        Assert.assertEquals(
            "total count for key " + e.getKey(),
            (long) duplicates,
            (long) e.getValue()
        );
      }
    }
  }

  @Test
  public void testSmallSpillsAreBatched() throws IOException
  {
    final File storageDir = temporaryFolder.newFolder();
    final int bufferSize = 50;
    final int numKeys = 100;

    int maxUsableEntries = computeMaxUsableEntries(bufferSize);
    Assert.assertEquals(
        "buffer should hold at most 1 entry, guaranteeing a spill on every key",
        1,
        maxUsableEntries
    );

    try (SpillingGrouper<IntKey> grouper = makeGrouper(bufferSize, storageDir, 1024 * 1024, 100)) {
      for (int i = 0; i < numKeys; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      assertResultsCorrect(grouper, numKeys, 1);

      File[] files = storageDir.listFiles();
      Assert.assertNotNull(files);
      Assert.assertEquals(
          "all spills are tiny and should batch into a single data + dictionary file pair",
          2,
          files.length
      );
    }
  }

  @Test
  public void testDiskQuotaReclaimedWhenSmallSpillsDeleted() throws IOException
  {
    final File storageDir = temporaryFolder.newFolder();
    final LimitedTemporaryStorage temporaryStorage =
        new LimitedTemporaryStorage(storageDir, 1024 * 1024, 100, new GroupByStatsProvider.PerQueryStats());
    final int bufferSize = 50;
    final int numKeys = 100;

    int maxUsableEntries = computeMaxUsableEntries(bufferSize);
    Assert.assertEquals(
        "buffer should hold at most 1 entry, guaranteeing a spill on every key",
        1,
        maxUsableEntries
    );

    try (SpillingGrouper<IntKey> grouper = makeGrouper(bufferSize, temporaryStorage)) {
      for (int i = 0; i < numKeys; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      // Before iterator(): small spills were created and deleted during batching, so the
      // temporary storage should have reclaimed their bytes. Only the final merged file(s)
      // from flushPendingRunsToDisk() should remain on disk.
      long sizeBeforeIterator = temporaryStorage.currentSize();
      int fileCountBeforeIterator = temporaryStorage.currentFileCount();

      // With a 50-byte buffer and 100 keys, many individual spills occur. Batching deletes
      // each small temp file immediately, so the file count should be much less than numKeys.
      Assert.assertTrue(
          "file count (" + fileCountBeforeIterator + ") should be much less than numKeys (" + numKeys
          + ") because small spill files are deleted after being read into memory",
          fileCountBeforeIterator < numKeys
      );

      // The tracked bytes should reflect only the files still on disk, not the deleted ones.
      long actualDiskBytes = 0;
      File[] diskFiles = storageDir.listFiles();
      Assert.assertNotNull(diskFiles);
      for (File f : diskFiles) {
        actualDiskBytes += f.length();
      }
      Assert.assertEquals(
          "tracked bytes should match actual bytes on disk",
          actualDiskBytes,
          sizeBeforeIterator
      );

      // Calling iterator() flushes remaining pending runs; verify results are still correct.
      assertResultsCorrect(grouper, numKeys, 1);

      // After iterator, check that the final state is also consistent.
      long sizeAfterIterator = temporaryStorage.currentSize();
      long actualDiskBytesAfter = 0;
      File[] diskFilesAfter = storageDir.listFiles();
      Assert.assertNotNull(diskFilesAfter);
      for (File f : diskFilesAfter) {
        actualDiskBytesAfter += f.length();
      }
      Assert.assertEquals(
          "tracked bytes should match actual bytes on disk after iterator",
          actualDiskBytesAfter,
          sizeAfterIterator
      );
    }
  }

  @Test
  public void testResetClearsPendingState() throws IOException
  {
    try (SpillingGrouper<IntKey> grouper = makeGrouper(50, temporaryFolder.newFolder(), 1024 * 1024, 100)) {
      for (int i = 0; i < 50; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      grouper.reset();

      for (int i = 1000; i < 1010; i++) {
        Assert.assertTrue(grouper.aggregate(new IntKey(i)).isOk());
      }

      try (CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true)) {
        int count = 0;
        while (iterator.hasNext()) {
          Grouper.Entry<IntKey> entry = iterator.next();
          Assert.assertTrue(
              "keys should be >= 1000 after reset",
              entry.getKey().intValue() >= 1000
          );
          count++;
        }
        Assert.assertEquals(10, count);
      }
    }
  }

  @Test
  public void testDiskFull() throws IOException
  {
    try (SpillingGrouper<IntKey> grouper = makeGrouper(50, temporaryFolder.newFolder(), 10, 100)) {
      AggregateResult lastResult = AggregateResult.ok();
      for (int i = 0; i < 10000 && lastResult.isOk(); i++) {
        lastResult = grouper.aggregate(new IntKey(i));
      }

      Assert.assertFalse("should have hit disk full", lastResult.isOk());
      Assert.assertTrue(
          "reason should mention disk space",
          lastResult.getReason().contains("Not enough disk space")
      );
    }
  }

  @Test
  public void testMaxSpillFileCount() throws IOException
  {
    // With batching, small spill files are created and immediately deleted, so the file count
    // stays low. The limit is hit when flushPendingRunsToDisk() creates the merged data file
    // (succeeds as file #1) then tries to create the dictionary file (fails because
    // maxFileCount=1 is already reached). Need enough keys to accumulate >= 1MB of pending bytes.
    //
    // Without batching, the file limit would be hit on the 2nd spill — only a handful of keys
    // would succeed. With batching, thousands of keys are processed before the flush triggers
    // the limit. We assert keysAggregated > maxUsableEntries * 2 to prove batching was active.
    final int bufferSize = 50;
    final int maxUsableEntries = computeMaxUsableEntries(bufferSize);
    try (SpillingGrouper<IntKey> grouper = makeGrouper(bufferSize, temporaryFolder.newFolder(), 10 * 1024 * 1024, 1)) {
      AggregateResult lastResult = AggregateResult.ok();
      int keysAggregated = 0;
      for (int i = 0; i < 200_000 && lastResult.isOk(); i++) {
        lastResult = grouper.aggregate(new IntKey(i));
        if (lastResult.isOk()) {
          keysAggregated++;
        }
      }

      Assert.assertFalse("should have hit max file count", lastResult.isOk());
      Assert.assertTrue(
          "reason should mention spill file count",
          lastResult.getReason().contains("Maximum number of spill files")
      );
      Assert.assertTrue(
          "batching should allow many keys (" + keysAggregated + ") before hitting file limit;"
          + " without batching only ~" + (maxUsableEntries * 2) + " would succeed",
          keysAggregated > maxUsableEntries * 2
      );
    }
  }

  private SpillingGrouper<IntKey> makeGrouper(
      int bufferSize,
      File storageDir,
      long maxStorageBytes,
      int maxFileCount
  )
  {
    return makeGrouper(
        bufferSize,
        new LimitedTemporaryStorage(storageDir, maxStorageBytes, maxFileCount, new GroupByStatsProvider.PerQueryStats())
    );
  }

  private SpillingGrouper<IntKey> makeGrouper(
      int bufferSize,
      LimitedTemporaryStorage temporaryStorage
  )
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("value", 1L)));

    final SpillingGrouper<IntKey> grouper = new SpillingGrouper<>(
        Suppliers.ofInstance(ByteBuffer.allocate(bufferSize)),
        new IntKeySerdeFactory(),
        columnSelectorFactory,
        AGGREGATOR_FACTORIES,
        Integer.MAX_VALUE,
        MAX_LOAD_FACTOR,
        INITIAL_BUCKETS,
        temporaryStorage,
        new ObjectMapper(),
        true,
        null,
        false,
        bufferSize,
        1024 * 1024L,
        new GroupByStatsProvider.PerQueryStats()
    );
    grouper.init();
    return grouper;
  }

  private void assertResultsCorrect(
      SpillingGrouper<IntKey> grouper,
      int expectedKeys,
      long expectedCountPerKey
  ) throws IOException
  {
    final Map<Integer, Object[]> results = new HashMap<>();
    try (CloseableIterator<Grouper.Entry<IntKey>> iterator = grouper.iterator(true)) {
      while (iterator.hasNext()) {
        Grouper.Entry<IntKey> entry = iterator.next();
        int key = entry.getKey().intValue();
        Object[] valuesCopy = new Object[entry.getValues().length];
        System.arraycopy(entry.getValues(), 0, valuesCopy, 0, valuesCopy.length);
        Assert.assertNull("duplicate key in results: " + key, results.put(key, valuesCopy));
      }
    }
    Assert.assertEquals(expectedKeys, results.size());
    for (Map.Entry<Integer, Object[]> e : results.entrySet()) {
      Assert.assertEquals(
          "valueSum for key " + e.getKey(),
          expectedCountPerKey,
          e.getValue()[0]
      );
      Assert.assertEquals(
          "count for key " + e.getKey(),
          expectedCountPerKey,
          e.getValue()[1]
      );
    }
  }

  private static int computeMaxUsableEntries(int bufferSize)
  {
    int aggSize = 0;
    for (AggregatorFactory factory : AGGREGATOR_FACTORIES) {
      aggSize += factory.getMaxIntermediateSizeWithNulls();
    }
    int bucketSizeWithHash = Integer.BYTES + KEY_SIZE + aggSize;
    int maxBuckets = Math.min(bufferSize / bucketSizeWithHash, INITIAL_BUCKETS);
    return (int) (maxBuckets * MAX_LOAD_FACTOR);
  }

  static class IntKeySerdeFactory implements Grouper.KeySerdeFactory<IntKey>
  {
    @Override
    public long getMaxDictionarySize()
    {
      return 0;
    }

    @Override
    public Grouper.KeySerde<IntKey> factorize()
    {
      return new IntKeySerde();
    }

    @Override
    public Grouper.KeySerde<IntKey> factorizeWithDictionary(List<String> dictionary)
    {
      return factorize();
    }

    @Override
    public IntKey copyKey(IntKey key)
    {
      return new IntKey(key.intValue());
    }

    @Override
    public Comparator<Grouper.Entry<IntKey>> objectComparator(boolean forceDefaultOrder)
    {
      return Comparator.comparingInt(o -> o.getKey().intValue());
    }
  }
}
