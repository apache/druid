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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.query.lookup.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.lookup.namespace.cache.CacheScheduler;
import io.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import io.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Map;

/**
 *
 */
public class NamespacedExtractorModuleTest
{
  private static final ObjectMapper mapper = URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper());
  private CacheScheduler scheduler;
  private Lifecycle lifecycle;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> factoryMap =
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>of(
            URIExtractionNamespace.class,
            new URIExtractionNamespaceCacheFactory(
                ImmutableMap.<String, SearchableVersionedDataFinder>of(
                    "file",
                    new LocalFileTimestampVersionFinder()
                )
            ),
            JDBCExtractionNamespace.class, new JDBCExtractionNamespaceCacheFactory()
        );
    lifecycle = new Lifecycle();
    lifecycle.start();
    NoopServiceEmitter noopServiceEmitter = new NoopServiceEmitter();
    scheduler = new CacheScheduler(
        noopServiceEmitter,
        factoryMap,
        new OnHeapNamespaceExtractionCacheManager(lifecycle, noopServiceEmitter)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    lifecycle.stop();
  }

  @Test
  public void testNewTask() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespaceCacheFactory factory = new URIExtractionNamespaceCacheFactory(
        ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
    );
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    CacheScheduler.VersionedCache versionedCache = factory.populateCache(namespace, null, null, scheduler);
    Assert.assertNotNull(versionedCache);
    Map<String, String> map = versionedCache.getCache();
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test
  public void testListNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())),
        new Period(0),
        null
    );
    try (CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 1_000)) {
      Assert.assertNotNull(entry);
      entry.awaitTotalUpdates(1);
      Assert.assertEquals(1, scheduler.getActiveEntries());
    }
  }

  @Test//(timeout = 10_000)
  public void testDeleteNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    try (CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 1_000)) {
      Assert.assertNotNull(entry);
    }
  }

  @Test
  public void testNewUpdate() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    Assert.assertEquals(0, scheduler.getActiveEntries());
    try (CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 10_000)) {
      Assert.assertNotNull(entry);
      entry.awaitTotalUpdates(1);
      Assert.assertEquals(1, scheduler.getActiveEntries());
    }
  }
}
