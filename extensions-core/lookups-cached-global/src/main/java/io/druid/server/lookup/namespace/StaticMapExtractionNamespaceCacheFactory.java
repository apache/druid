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

package io.druid.server.lookup.namespace;

import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.StaticMapExtractionNamespace;

import java.util.Map;
import java.util.UUID;

public class StaticMapExtractionNamespaceCacheFactory
    implements ExtractionNamespaceCacheFactory<StaticMapExtractionNamespace>
{
  private final String version = UUID.randomUUID().toString();

  @Override
  public String populateCache(
      final String id,
      final StaticMapExtractionNamespace extractionNamespace,
      final String lastVersion,
      final Map<String, String> swap
  )
  {
    if (lastVersion != null) {
      // Throwing AssertionError, because NamespaceExtractionCacheManager doesn't suppress Errors and will stop trying
      // to update the cache periodically.
      throw new AssertionError(
          "StaticMapExtractionNamespaceCacheFactory could only be configured for a namespace which is scheduled " +
          "to be updated once, not periodically. Last version: `" + lastVersion + "`");
    }
    swap.putAll(extractionNamespace.getMap());
    return version;
  }

  String getVersion()
  {
    return version;
  }
}
