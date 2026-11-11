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

package org.apache.druid.k8s.config;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remembers what a delegate read, so that a node is asked about once per process.
 * <p>
 * Druid builds a {@link org.apache.druid.metadata.DynamicConfigProvider} for every use and a task builds several
 * consumers, so without this each of them would call the API server again. A failed read is not remembered, so a
 * transient error is retried by the next caller.
 */
class CachingNodeLabelReader implements NodeLabelReader
{
  private final NodeLabelReader delegate;
  private final ConcurrentHashMap<String, Map<String, String>> labelsByNode = new ConcurrentHashMap<>();

  CachingNodeLabelReader(NodeLabelReader delegate)
  {
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public Map<String, String> readLabels(String nodeName)
  {
    final Map<String, String> cached = labelsByNode.get(nodeName);
    if (cached != null) {
      return cached;
    }

    // Not computeIfAbsent: the delegate blocks on the network, and a mapping function must not hold the map's bin
    // lock for that long. Two callers racing here cost one extra request and agree on the result.
    final Map<String, String> read = delegate.readLabels(nodeName);
    if (read == null) {
      return null;
    }
    labelsByNode.put(nodeName, read);
    return read;
  }
}
