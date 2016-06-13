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

package io.druid.server.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractorModuleTest
{
  private static final ObjectMapper mapper = URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper());
  private static NamespaceExtractionCacheManager cacheManager;
  private static Lifecycle lifecycle;
  private static ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpStatic() throws Exception
  {
    final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> factoryMap =
        ImmutableMap.<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>>of(
            URIExtractionNamespace.class,
            new URIExtractionNamespaceFunctionFactory(
                ImmutableMap.<String, SearchableVersionedDataFinder>of(
                    "file",
                    new LocalFileTimestampVersionFinder()
                )
            ),
            JDBCExtractionNamespace.class, new JDBCExtractionNamespaceFunctionFactory()
        );
    lifecycle = new Lifecycle();
    cacheManager = new OnHeapNamespaceExtractionCacheManager(
        lifecycle,
        new ConcurrentHashMap<String, Function<String, String>>(),
        new ConcurrentHashMap<String, Function<String, List<String>>>(),
        new NoopServiceEmitter(), factoryMap
    );
    fnCache.clear();
  }

  @AfterClass
  public static void tearDownStatic() throws Exception
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
    final URIExtractionNamespaceFunctionFactory factory = new URIExtractionNamespaceFunctionFactory(
        ImmutableMap.<String, SearchableVersionedDataFinder>of("file", new LocalFileTimestampVersionFinder())
    );
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    Map<String, String> map = new HashMap<>();
    factory.getCachePopulator(namespace, null, map).call();
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test(timeout = 1_000)
  public void testListNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())),
        new Period(0),
        null
    );
    cacheManager.scheduleOrUpdate(ImmutableList.<ExtractionNamespace>of(namespace));
    Collection<String> strings = cacheManager.getKnownNamespaces();
    Assert.assertArrayEquals(new String[]{"ns"}, strings.toArray(new String[strings.size()]));
    while (!Arrays.equals(cacheManager.getKnownNamespaces().toArray(), new Object[]{"ns"})) {
      Thread.sleep(1);
    }
  }

  private static boolean noNamespaces(NamespaceExtractionCacheManager manager)
  {
    return manager.getKnownNamespaces().isEmpty();
  }

  @Test//(timeout = 10_000)
  public void testDeleteNamespaces() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    cacheManager.delete("ns");
    while (!noNamespaces(cacheManager)) {
      Thread.sleep(1);
    }
  }

  @Test(timeout = 10_000)
  public void testNewUpdate() throws Exception
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    Assert.assertTrue(noNamespaces(cacheManager));
    cacheManager.scheduleOrUpdate(ImmutableList.<ExtractionNamespace>of(namespace));
    while (!Arrays.equals(cacheManager.getKnownNamespaces().toArray(), new Object[]{"ns"})) {
      Thread.sleep(1);
    }
  }
}
