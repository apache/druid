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

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Distributes objects across a set of node keys using rendezvous hashing
 * See https://en.wikipedia.org/wiki/Rendezvous_hashing
 */
public class RendezvousHasher
{
  private static final HashFunction HASH_FN = Hashing.murmur3_128();

  public String chooseNode(Set<String> nodeIds, byte[] key)
  {
    if (nodeIds.isEmpty()) {
      return null;
    }

    long maxHash = Long.MIN_VALUE;
    String maxNode = null;

    for (String nodeId : nodeIds) {
      long combinedHash = HASH_FN.newHasher()
                                 .putString(nodeId, StandardCharsets.UTF_8)
                                 .putBytes(key)
                                 .hash().asLong();
      if (maxNode == null) {
        maxHash = combinedHash;
        maxNode = nodeId;
      } else if (combinedHash > maxHash) {
        maxHash = combinedHash;
        maxNode = nodeId;
      }
    }

    return maxNode;
  }
}
