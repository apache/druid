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

package io.druid.server.lookup.namespace;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.CacheGenerator;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.StaticMapExtractionNamespace;
import io.druid.server.lookup.namespace.cache.CacheScheduler;
import io.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import io.druid.server.metrics.NoopServiceEmitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class StaticMapCacheGeneratorTest
{
  private static final Map<String, String> MAP = ImmutableMap.<String, String>builder().put("foo", "bar").build();

  private Lifecycle lifecycle;
  private CacheScheduler scheduler;

  @Before
  public void setup() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
    NoopServiceEmitter noopServiceEmitter = new NoopServiceEmitter();
    scheduler = new CacheScheduler(
        noopServiceEmitter,
        Collections.<Class<? extends ExtractionNamespace>, CacheGenerator<?>>emptyMap(),
        new OnHeapNamespaceExtractionCacheManager(lifecycle, noopServiceEmitter)
    );
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  @Test
  public void testSimpleGenerator() throws Exception
  {
    final StaticMapCacheGenerator factory = new StaticMapCacheGenerator();
    final StaticMapExtractionNamespace namespace = new StaticMapExtractionNamespace(MAP);
    CacheScheduler.VersionedCache versionedCache = factory.generateCache(namespace, null, null, scheduler);
    Assert.assertNotNull(versionedCache);
    Assert.assertEquals(factory.getVersion(), versionedCache.getVersion());
    Assert.assertEquals(MAP, versionedCache.getCache());

  }

  @Test(expected = AssertionError.class)
  public void testNonNullLastVersionCausesAssertionError()
  {
    final StaticMapCacheGenerator factory = new StaticMapCacheGenerator();
    final StaticMapExtractionNamespace namespace = new StaticMapExtractionNamespace(MAP);
    factory.generateCache(namespace, null, factory.getVersion(), scheduler);
  }
}
