/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.router;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;

import java.util.List;
import java.util.Set;

/**
 * Distributes objects across a set of node keys using rendezvous hashing
 * See https://en.wikipedia.org/wiki/Rendezvous_hashing
 */
public class RendezvousHasher
{
  private static final HashFunction HASH_FN = Hashing.murmur3_128();

  public static Funnel STRING_FUNNEL = Funnels.stringFunnel(Charsets.UTF_8);

  public String chooseNode(Set<String> nodeIds, String key)
  {
    return chooseNode(nodeIds, key, STRING_FUNNEL);
  }

  public <KeyType> String chooseNode(Set<String> nodeIds, KeyType key, Funnel<KeyType> funnel)
  {
    if (nodeIds.isEmpty()) {
      return null;
    }

    Long2ObjectRBTreeMap<String> weights = new Long2ObjectRBTreeMap<>();
    weights.defaultReturnValue(null);

    for (String nodeId : nodeIds) {
      HashCode keyHash = HASH_FN.hashObject(key, funnel);
      HashCode nodeHash = HASH_FN.hashObject(nodeId, STRING_FUNNEL);
      List<HashCode> hashes = Lists.newArrayList(nodeHash, keyHash);
      HashCode combinedHash = Hashing.combineOrdered(hashes);
      weights.put(combinedHash.asLong(), nodeId);
    }

    return weights.get(weights.lastLongKey());
  }
}
