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

import com.google.common.collect.ImmutableMap;
import io.druid.query.lookup.namespace.StaticMapExtractionNamespace;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class StaticMapExtractionNamespaceCacheFactoryTest
{
  private static final Map<String, String> MAP = ImmutableMap.<String, String>builder().put("foo", "bar").build();

  @Test
  public void testSimplePopulator() throws Exception
  {
    final StaticMapExtractionNamespaceCacheFactory factory = new StaticMapExtractionNamespaceCacheFactory();
    final StaticMapExtractionNamespace namespace = new StaticMapExtractionNamespace(MAP);
    final Map<String, String> cache = new HashMap<>();
    Assert.assertEquals(factory.getVersion(), factory.getCachePopulator(null, namespace, null, cache).call());
    Assert.assertEquals(MAP, cache);
    Assert.assertNull(factory.getCachePopulator(null, namespace, factory.getVersion(), cache).call());
  }
}
