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

package org.apache.druid.server.router;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Distributes objects across a set of node keys using consistent hashing.
 * See https://en.wikipedia.org/wiki/Consistent_hashing
 * Not thread-safe.
 */
public class ConsistentHasher
{
  // Determined through tests to provide reasonably equal balancing on a test set of 5-10 brokers
  private static final int REPLICATION_FACTOR = 128;
  private static final HashFunction DEFAULT_HASH_FN = Hashing.murmur3_128();

  private final Long2ObjectRBTreeMap<String> nodeKeySlots = new Long2ObjectRBTreeMap<>();

  {
    nodeKeySlots.defaultReturnValue(null);
  }

  private final HashFunction hashFn;
  private final Map<String, long[]> nodeKeyHashes = new HashMap<>();
  private Set<String> previousKeys = new HashSet<>();

  public ConsistentHasher(final HashFunction hashFunction)
  {
    this.hashFn = hashFunction == null ? DEFAULT_HASH_FN : hashFunction;
  }

  public void updateKeys(Set<String> currentKeys)
  {
    Set<String> added = new HashSet<>(currentKeys);
    added.removeAll(previousKeys);

    Set<String> removed = new HashSet<>(previousKeys);
    removed.removeAll(currentKeys);

    for (String key : added) {
      addKey(key);
    }

    for (String key : removed) {
      removeKey(key);
    }

    // store a copy in case the input was immutable
    previousKeys = new HashSet<>(currentKeys);
  }

  public String findKey(byte[] obj)
  {
    if (nodeKeySlots.size() == 0) {
      return null;
    }

    long objHash = hashFn.hashBytes(obj).asLong();

    Long2ObjectSortedMap<String> subMap = nodeKeySlots.tailMap(objHash);
    if (subMap.isEmpty()) {
      return nodeKeySlots.long2ObjectEntrySet().first().getValue();
    }

    Long2ObjectMap.Entry<String> firstEntry = subMap.long2ObjectEntrySet().first();
    return firstEntry.getValue();
  }

  private void addKey(String key)
  {
    if (nodeKeyHashes.containsKey(key)) {
      return;
    }

    addNodeKeyHashes(key);
    addNodeKeySlots(key);
  }

  private void removeKey(String key)
  {
    if (!nodeKeyHashes.containsKey(key)) {
      return;
    }

    removeNodeKeySlots(key);
    removeNodeKeyHashes(key);
  }

  private void addNodeKeyHashes(String key)
  {
    long[] hashes = new long[REPLICATION_FACTOR];
    for (int i = 0; i < REPLICATION_FACTOR; i++) {
      String vnode = key + "-" + i;
      hashes[i] = hashFn.hashString(vnode, StandardCharsets.UTF_8).asLong();
    }

    nodeKeyHashes.put(key, hashes);
  }

  private void addNodeKeySlots(String key)
  {
    long[] hashes = nodeKeyHashes.get(key);
    for (long hash : hashes) {
      nodeKeySlots.put(hash, key);
    }
  }

  private void removeNodeKeyHashes(String key)
  {
    nodeKeyHashes.remove(key);
  }

  private void removeNodeKeySlots(String key)
  {
    long[] hashes = nodeKeyHashes.get(key);
    for (long hash : hashes) {
      nodeKeySlots.remove(hash);
    }
  }
}
