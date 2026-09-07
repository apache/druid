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

package org.apache.druid.benchmark;

import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.groupby.epinephelinae.ByteBufferHashTable;
import org.apache.druid.query.groupby.epinephelinae.Groupers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for ByteBufferHashTable.findBucket() method.
 * Measures lookup latency with various key sizes, simulating GROUP BY workloads
 * with a mix of existing key lookups and new key insertions.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(ByteBufferHashTableBenchmark.ITERATIONS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
@State(Scope.Benchmark)
public class ByteBufferHashTableBenchmark
{
  public static final int ITERATIONS = 10000;

  private static final float MAX_LOAD_FACTOR = 0.7f;
  private static final int NUM_BUCKETS = 16384;
  // Percentage of lookups that will be for non-existent keys (simulates new group creation)
  private static final double NEW_KEY_RATIO = 0.1;
  // Size of aggregation values per bucket (e.g., 8 bytes for a long sum, 16 for sum+count, etc.)
  private static final int VALUE_SIZE = 16;

  @Param({"8", "16", "32", "64", "128"})
  public int keySize;

  private BenchmarkHashTable hashTable;
  private ByteBuffer[] lookupKeys;
  private int[] lookupKeyHashes;

  // Direct buffers to be freed in tearDown
  private ByteBuffer tableBuffer;
  private ByteBuffer insertedKeysBuffer;
  private ByteBuffer lookupKeysBuffer;

  @Setup(Level.Trial)
  public void setup()
  {
    int bucketSize = Integer.BYTES + keySize + VALUE_SIZE; // hash + key + aggregation values
    tableBuffer = ByteBuffer.allocateDirect(NUM_BUCKETS * bucketSize);

    hashTable = new BenchmarkHashTable(
        MAX_LOAD_FACTOR,
        NUM_BUCKETS,
        bucketSize,
        tableBuffer,
        keySize
    );
    hashTable.reset();

    Random random = new Random(42);
    int numEntries = (int) (NUM_BUCKETS * MAX_LOAD_FACTOR);

    // Allocate direct buffer for inserted keys
    insertedKeysBuffer = ByteBuffer.allocateDirect(numEntries * keySize);
    int[] insertedKeyHashes = new int[numEntries];

    // Insert entries into the hash table
    for (int i = 0; i < numEntries; i++) {
      byte[] keyBytes = new byte[keySize];
      random.nextBytes(keyBytes);

      // Store key in direct buffer
      int keyOffset = i * keySize;
      insertedKeysBuffer.position(keyOffset);
      insertedKeysBuffer.put(keyBytes);

      // Create a slice for this key
      insertedKeysBuffer.position(keyOffset);
      insertedKeysBuffer.limit(keyOffset + keySize);
      ByteBuffer keyBuffer = insertedKeysBuffer.slice();
      insertedKeysBuffer.clear(); // Reset limit for next iteration

      int keyHash = Groupers.smear(keyBuffer.getInt(0)) & Groupers.USED_FLAG_MASK;
      insertedKeyHashes[i] = keyHash;

      int bucket = hashTable.findBucket0(true, keyBuffer, keyHash);
      if (bucket >= 0) {
        // Reset position before initBucket since initializeNewBucketKey uses relative put
        keyBuffer.position(0);
        hashTable.initBucket(bucket, keyBuffer, keyHash);
      }
    }

    // Prepare lookup keys - mix of existing keys and new keys (not in table)
    // Allocate a single direct buffer for all lookup keys
    lookupKeysBuffer = ByteBuffer.allocateDirect(ITERATIONS * keySize);
    lookupKeys = new ByteBuffer[ITERATIONS];
    lookupKeyHashes = new int[ITERATIONS];

    int newKeyCount = (int) (ITERATIONS * NEW_KEY_RATIO);

    // Generate new keys that don't exist in the table
    Random newKeyRandom = new Random(12345);
    for (int i = 0; i < newKeyCount; i++) {
      byte[] keyBytes = new byte[keySize];
      newKeyRandom.nextBytes(keyBytes);

      int keyOffset = i * keySize;
      lookupKeysBuffer.position(keyOffset);
      lookupKeysBuffer.put(keyBytes);

      // Create a slice for this key
      lookupKeysBuffer.position(keyOffset);
      lookupKeysBuffer.limit(keyOffset + keySize);
      lookupKeys[i] = lookupKeysBuffer.slice();
      lookupKeysBuffer.clear();

      lookupKeyHashes[i] = Groupers.smear(lookupKeys[i].getInt(0)) & Groupers.USED_FLAG_MASK;
    }

    // Fill the rest with existing keys (copy from insertedKeysBuffer)
    for (int i = newKeyCount; i < ITERATIONS; i++) {
      int idx = ThreadLocalRandom.current().nextInt(numEntries);

      // Copy key data from inserted keys buffer
      int srcOffset = idx * keySize;
      int dstOffset = i * keySize;

      for (int j = 0; j < keySize; j++) {
        lookupKeysBuffer.put(dstOffset + j, insertedKeysBuffer.get(srcOffset + j));
      }

      // Create a slice for this key
      lookupKeysBuffer.position(dstOffset);
      lookupKeysBuffer.limit(dstOffset + keySize);
      lookupKeys[i] = lookupKeysBuffer.slice();
      lookupKeysBuffer.clear();

      lookupKeyHashes[i] = insertedKeyHashes[idx];
    }

    // Shuffle to mix new and existing keys
    for (int i = ITERATIONS - 1; i > 0; i--) {
      int j = ThreadLocalRandom.current().nextInt(i + 1);
      ByteBuffer tempKey = lookupKeys[i];
      lookupKeys[i] = lookupKeys[j];
      lookupKeys[j] = tempKey;
      int tempHash = lookupKeyHashes[i];
      lookupKeyHashes[i] = lookupKeyHashes[j];
      lookupKeyHashes[j] = tempHash;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    if (tableBuffer != null) {
      ByteBufferUtils.free(tableBuffer);
      tableBuffer = null;
    }
    if (insertedKeysBuffer != null) {
      ByteBufferUtils.free(insertedKeysBuffer);
      insertedKeysBuffer = null;
    }
    if (lookupKeysBuffer != null) {
      ByteBufferUtils.free(lookupKeysBuffer);
      lookupKeysBuffer = null;
    }
  }

  @Benchmark
  public int findBucket()
  {
    int result = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      // allowNewBucket=true simulates GROUP BY where new groups can be created
      result ^= hashTable.findBucket0(true, lookupKeys[i], lookupKeyHashes[i]);
    }
    return result;
  }

  /**
   * Test harness that exposes protected findBucket() method for benchmarking.
   */
  private static class BenchmarkHashTable extends ByteBufferHashTable
  {
    BenchmarkHashTable(
        float maxLoadFactor,
        int initialBuckets,
        int bucketSizeWithHash,
        ByteBuffer buffer,
        int keySize
    )
    {
      super(maxLoadFactor, initialBuckets, bucketSizeWithHash, buffer, keySize, Integer.MAX_VALUE, null);
    }

    int findBucket0(boolean allowNewBucket, ByteBuffer keyBuffer, int keyHash)
    {
      return findBucket(allowNewBucket, maxBuckets, tableBuffer, keyBuffer, keyHash);
    }

    void initBucket(int bucket, ByteBuffer keyBuffer, int keyHash)
    {
      initializeNewBucketKey(bucket, keyBuffer, keyHash);
    }
  }
}
