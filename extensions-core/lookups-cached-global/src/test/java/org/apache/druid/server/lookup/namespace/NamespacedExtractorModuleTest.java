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

package org.apache.druid.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.ExtractionNamespace;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespaceTest;
import org.apache.druid.segment.loading.LocalFileTimestampVersionFinder;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.apache.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *
 */
public class NamespacedExtractorModuleTest
{
  private static final ObjectMapper MAPPER = UriExtractionNamespaceTest.registerTypes(new DefaultObjectMapper());
  private CacheScheduler scheduler;
  private Lifecycle lifecycle;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    final Map<Class<? extends ExtractionNamespace>, CacheGenerator<?>> factoryMap =
        ImmutableMap.of(
            UriExtractionNamespace.class,
            new UriCacheGenerator(
                ImmutableMap.of(
                    "file",
                    new LocalFileTimestampVersionFinder()
                )
            ),
            JdbcExtractionNamespace.class, new JdbcCacheGenerator()
        );
    lifecycle = new Lifecycle();
    lifecycle.start();
    NoopServiceEmitter noopServiceEmitter = new NoopServiceEmitter();
    scheduler = new CacheScheduler(
        noopServiceEmitter,
        factoryMap,
        new OnHeapNamespaceExtractionCacheManager(lifecycle, noopServiceEmitter, new NamespaceExtractionConfig())
    );
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  @Test
  public void testNewTask() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (Writer out = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      out.write(MAPPER.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final UriCacheGenerator factory = new UriCacheGenerator(
        ImmutableMap.of("file", new LocalFileTimestampVersionFinder())
    );
    final UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    CacheScheduler.VersionedCache versionedCache = factory.generateCache(namespace, null, null, scheduler);
    Assert.assertNotNull(versionedCache);
    Map<String, String> map = versionedCache.getCache();
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test
  public void testListNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (Writer out = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      out.write(MAPPER.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(UriExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())),
        new Period(0),
        null
    );
    try (CacheScheduler.Entry entry = scheduler.scheduleAndWait(namespace, 1_000)) {
      Assert.assertNotNull(entry);
      entry.awaitTotalUpdates(1);
      Assert.assertEquals(1, scheduler.getActiveEntries());
    }
  }

  @Test//(timeout = 60_000L)
  public void testDeleteNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (Writer out = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      out.write(MAPPER.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
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
    try (Writer out = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      out.write(MAPPER.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final UriExtractionNamespace namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
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
