/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.common.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@JsonTypeName("onHeap")
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private final ConcurrentMap<String, ConcurrentMap<String, String>> mapMap = new ConcurrentHashMap<>();

  @JsonCreator
  public OnHeapNamespaceExtractionCacheManager(
      @JacksonInject Lifecycle lifecycle
  )
  {
    super(lifecycle);
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespace)
  {
    ConcurrentMap<String, String> map = mapMap.get(namespace);
    if (map == null) {
      mapMap.putIfAbsent(namespace, new ConcurrentHashMap<String, String>(32));
      map = mapMap.get(namespace);
    }
    return map;
  }

  @Override
  public void delete(final String ns)
  {
    super.delete(ns);
    mapMap.remove(ns);
  }

  @Override
  public Collection<String> getKnownNamespaces()
  {
    return mapMap.keySet();
  }
}
