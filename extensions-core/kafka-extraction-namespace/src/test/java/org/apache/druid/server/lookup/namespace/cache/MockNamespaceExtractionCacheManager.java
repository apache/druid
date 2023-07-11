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

package org.apache.druid.server.lookup.namespace.cache;

import org.easymock.EasyMock;

import java.util.concurrent.ConcurrentHashMap;

public class MockNamespaceExtractionCacheManager
{
  public static NamespaceExtractionCacheManager createMockNamespaceExtractionCacheManager()
  {
    final NamespaceExtractionCacheManager cacheManager = EasyMock.createStrictMock(NamespaceExtractionCacheManager.class);
    final CacheHandler cacheHandler = new CacheHandler(cacheManager, new ConcurrentHashMap<>(), "cache1");

    EasyMock.expect(cacheManager.createCache())
            .andReturn(cacheHandler)
            .once();

    // disposeCache is package private, requiring us to have this methoe
    // in the same package as NamespaceExtractionCacheManager
    cacheManager.disposeCache(cacheHandler);
    EasyMock.expectLastCall().once();
    return cacheManager;
  }
}
