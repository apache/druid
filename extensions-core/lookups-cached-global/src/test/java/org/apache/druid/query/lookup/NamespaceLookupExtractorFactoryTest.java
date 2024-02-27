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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.namespace.ExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;

import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NamespaceLookupExtractorFactoryTest
{
  static {
    NullHandling.initializeForTests();
  }

  private final ObjectMapper mapper = new DefaultObjectMapper();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CacheScheduler scheduler = mock(CacheScheduler.class);
  private final CacheScheduler.Entry entry = mock(CacheScheduler.Entry.class);
  private final CacheScheduler.VersionedCache versionedCache =
      mock(CacheScheduler.VersionedCache.class);


  @Before
  public void setUp()
  {
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId,
              DeserializationContext ctxt,
              BeanProperty forProperty,
              Object beanInstance
          )
          {
            if (CacheScheduler.class.getName().equals(valueId)) {
              return scheduler;
            } else if (ObjectMapper.class.getName().equals(valueId)) {
              return mapper;
            } else {
              return null;
            }
          }
        }
    );
  }

  @Test
  public void testSimpleSerde() throws Exception
  {
    final UriExtractionNamespace uriExtractionNamespace = new UriExtractionNamespace(
        temporaryFolder.newFolder().toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(mapper),
        Period.millis(0),
        null,
        null
    );
    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        uriExtractionNamespace,
        scheduler
    );
    Assert.assertEquals(
        uriExtractionNamespace,
        mapper.readValue(
            mapper.writeValueAsString(namespaceLookupExtractorFactory),
            NamespaceLookupExtractorFactory.class
        ).getExtractionNamespace()
    );
  }

  @Test
  public void testMissingSpec()
  {
    Assert.assertThrows(
        "extractionNamespace should be specified", NullPointerException.class,
        () -> new NamespaceLookupExtractorFactory(null, null)
    );
  }

  @Test
  public void testSimpleStartStop() throws Exception
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    expectScheduleAndWaitOnce(extractionNamespace);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());

    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verify(entry).close();
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testStartReturnsImmediately()
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    when(scheduler.schedule(extractionNamespace)).thenReturn(entry);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        0,
        false,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());

    verify(scheduler).schedule(any());
    verify(entry).close();
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testStartReturnsImmediatelyAndFails() throws InterruptedException
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    when(scheduler.scheduleAndWait(extractionNamespace, 1L))
        .thenReturn(null);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        1,
        false,
        scheduler
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());

    verify(scheduler).scheduleAndWait(extractionNamespace, 1L);
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testSimpleStartStopStop() throws Exception
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    expectScheduleAndWaitOnce(extractionNamespace);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());

    verify(entry).close();
    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testSimpleStartStart() throws Exception
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    expectScheduleAndWaitOnce(extractionNamespace);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }


  @Test
  public void testSimpleStartGetStop() throws Exception
  {
    final ExtractionNamespace extractionNamespace = () -> 0;
    expectScheduleAndWaitOnce(extractionNamespace);
    when(entry.getCacheState()).thenReturn(versionedCache);
    when(versionedCache.asLookupExtractor(ArgumentMatchers.eq(false), ArgumentMatchers.any()))
        .thenReturn(new MapLookupExtractor(new HashMap<>(), false));
    when(versionedCache.getVersion()).thenReturn("0");

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    final LookupExtractor extractor = namespaceLookupExtractorFactory.get();
    Assert.assertNull(extractor.apply("foo"));
    Assert.assertTrue(namespaceLookupExtractorFactory.close());

    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verify(entry).getCacheState();
    verify(entry).close();
    verify(versionedCache).getVersion();
    verify(versionedCache, atLeastOnce()).asLookupExtractor(ArgumentMatchers.eq(false), ArgumentMatchers.any());
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }


  @Test
  public void testSimpleStartRacyGetDuringDelete() throws Exception
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    expectScheduleAndWaitOnce(extractionNamespace);
    when(entry.getCacheState()).thenReturn(CacheScheduler.NoCache.ENTRY_CLOSED);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertThrows(ISE.class, () -> namespaceLookupExtractorFactory.get());

    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verify(entry).getCacheState();
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testAwaitInitializationOnCacheNotInitialized() throws Exception
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }

      @Override
      public long getLoadTimeoutMills()
      {
        return 1;
      }
    };
    expectScheduleAndWaitOnce(extractionNamespace);
    when(entry.getCacheState()).thenReturn(CacheScheduler.NoCache.CACHE_NOT_INITIALIZED);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    namespaceLookupExtractorFactory.awaitInitialization();
    Assert.assertThrows(ISE.class, () -> namespaceLookupExtractorFactory.get());
    verify(scheduler).scheduleAndWait(extractionNamespace, 60000L);
    verify(entry, times(2)).getCacheState();
    verify(entry).awaitTotalUpdatesWithTimeout(1, 1);
    Thread.sleep(10);
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  private void expectScheduleAndWaitOnce(ExtractionNamespace extractionNamespace)
  {
    try {
      when(scheduler.scheduleAndWait(
          extractionNamespace,
          60000L
      )).thenReturn(entry);
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }


  @Test
  public void testStartFailsToSchedule() throws Exception
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    try {
      when(scheduler.scheduleAndWait(
          extractionNamespace,
          60000L
      )).thenReturn(null);
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    // true because it never fully started
    Assert.assertTrue(namespaceLookupExtractorFactory.close());

    verify(scheduler).scheduleAndWait(
        extractionNamespace,
        60000L
    );
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  @Test
  public void testReplaces()
  {
    final ExtractionNamespace
        en1 = mock(ExtractionNamespace.class),
        en2 = mock(ExtractionNamespace.class);

    final NamespaceLookupExtractorFactory f1 = new NamespaceLookupExtractorFactory(
        en1,
        scheduler
    );
    final NamespaceLookupExtractorFactory f2 = new NamespaceLookupExtractorFactory(en2, scheduler);
    final NamespaceLookupExtractorFactory f1b = new NamespaceLookupExtractorFactory(
        en1,
        scheduler
    );
    Assert.assertTrue(f1.replaces(f2));
    Assert.assertTrue(f2.replaces(f1));
    Assert.assertFalse(f1.replaces(f1b));
    Assert.assertFalse(f1b.replaces(f1));
    Assert.assertFalse(f1.replaces(f1));
    Assert.assertTrue(f1.replaces(mock(LookupExtractorFactory.class)));

    verifyNoInteractions(en1, en2);
  }

  @Test(expected = ISE.class)
  public void testMustBeStarted()
  {
    final ExtractionNamespace extractionNamespace = () -> 0;

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );

    namespaceLookupExtractorFactory.get();
  }

  // Note this does NOT catch problems with returning factories as failed in error messages.
  @Test
  public void testSerDe() throws Exception
  {
    final Injector injector = makeInjector();
    final ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
    final String str = "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"uri\", \"uriPrefix\": \"s3://bucket/prefix/\", \"fileRegex\": \"foo.*\\\\.gz\", \"namespaceParseSpec\": { \"format\": \"customJson\", \"keyFieldName\": \"someKey\", \"valueFieldName\": \"someVal\" }, \"pollPeriod\": \"PT5M\", \"maxHeapPercentage\": 10 } } }";
    final LookupExtractorFactory factory = mapper.readValue(str, LookupExtractorFactory.class);
    Assert.assertTrue(factory instanceof NamespaceLookupExtractorFactory);
    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = (NamespaceLookupExtractorFactory) factory;
    Assert.assertNotNull(mapper.writeValueAsString(factory));
    Assert.assertFalse(factory.replaces(mapper.readValue(
        mapper.writeValueAsString(factory),
        LookupExtractorFactory.class
    )));
    Assert.assertEquals(
        UriExtractionNamespace.class,
        namespaceLookupExtractorFactory.getExtractionNamespace().getClass()
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.replaces(mapper.readValue(str, LookupExtractorFactory.class)));
    final Map<String, Object> map = new HashMap<>(mapper.readValue(
        str,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    ));
    map.put("firstCacheTimeout", "1");
    Assert.assertTrue(namespaceLookupExtractorFactory.replaces(mapper.convertValue(map, LookupExtractorFactory.class)));
  }

  @Test
  public void testSimpleIntrospectionHandler() throws Exception
  {
    final Injector injector = makeInjector();
    final ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
    final String str = "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"staticMap\", \"map\": {\"foo\":\"bar\"} }, \"firstCacheTimeout\":10000 }";
    final LookupExtractorFactory lookupExtractorFactory = mapper.readValue(str, LookupExtractorFactory.class);
    Assert.assertTrue(lookupExtractorFactory.start());
    try {
      final LookupIntrospectHandler handler = lookupExtractorFactory.getIntrospectHandler();
      Assert.assertNotNull(handler);
      final Class<? extends LookupIntrospectHandler> clazz = handler.getClass();
      Assert.assertNotNull(clazz.getMethod("getVersion").invoke(handler));
      Assert.assertEquals(ImmutableSet.of("foo"), ((Response) clazz.getMethod("getKeys").invoke(handler)).getEntity());
      Assert.assertEquals(
          ImmutableList.of("bar"),
          ImmutableList.copyOf((Collection) ((Response) clazz.getMethod("getValues").invoke(handler)).getEntity())
      );
      Assert.assertEquals(
          ImmutableMap.builder().put("foo", "bar").build(),
          ((Response) clazz.getMethod("getMap").invoke(handler)).getEntity()
      );
    }
    finally {
      Assert.assertTrue(lookupExtractorFactory.close());
    }
  }

  @Test
  public void testSingletonCacheScheduler() throws Exception
  {
    final Injector injector = makeInjector();
    final ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerSubtypes(NamespaceLookupExtractorFactory.class);
    final String str1 = "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"staticMap\", \"map\": {\"foo\":\"bar\"} }, \"firstCacheTimeout\":10000 }";
    final NamespaceLookupExtractorFactory factory1 =
        (NamespaceLookupExtractorFactory) mapper.readValue(str1, LookupExtractorFactory.class);
    final String str2 = "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"uri\", \"uriPrefix\": \"s3://bucket/prefix/\", \"fileRegex\": \"foo.*\\\\.gz\", \"namespaceParseSpec\": { \"format\": \"customJson\", \"keyFieldName\": \"someKey\", \"valueFieldName\": \"someVal\" }, \"pollPeriod\": \"PT5M\" } } }";
    final NamespaceLookupExtractorFactory factory2 =
        (NamespaceLookupExtractorFactory) mapper.readValue(str2, LookupExtractorFactory.class);

    Assert.assertSame(factory1.getCacheScheduler(), factory2.getCacheScheduler());
  }

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("test-inject", null, false, null, null, true, false)
            )
        )
    );
  }

  @Test
  public void testExceptionalIntrospectionHandler() throws Exception
  {
    final ExtractionNamespace extractionNamespace = mock(ExtractionNamespace.class);
    when(scheduler.scheduleAndWait(eq(extractionNamespace), anyLong())).thenReturn(entry);

    final LookupExtractorFactory lookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(lookupExtractorFactory.start());

    final LookupIntrospectHandler handler = lookupExtractorFactory.getIntrospectHandler();
    Assert.assertNotNull(handler);
    final Class<? extends LookupIntrospectHandler> clazz = handler.getClass();

    verify(scheduler).scheduleAndWait(eq(extractionNamespace), anyLong());
    verifyNoMoreInteractions(scheduler, entry, versionedCache);

    reset(scheduler, entry, versionedCache);

    when(entry.getCacheState()).thenReturn(CacheScheduler.NoCache.CACHE_NOT_INITIALIZED);

    final Response response = (Response) clazz.getMethod("getVersion").invoke(handler);
    Assert.assertEquals(404, response.getStatus());

    verify(entry).getCacheState();
    validateNotFound("getKeys", handler, clazz);
    validateNotFound("getValues", handler, clazz);
    validateNotFound("getMap", handler, clazz);
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }

  private void validateNotFound(
      String method,
      LookupIntrospectHandler handler,
      Class<? extends LookupIntrospectHandler> clazz
  ) throws Exception
  {
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
    reset(scheduler, entry, versionedCache);

    when(entry.getCacheState()).thenReturn(versionedCache);
    when(entry.getCache()).thenReturn(new HashMap<String, String>());
    when(versionedCache.getCache()).thenReturn(new HashMap<>());
    when(versionedCache.getVersion()).thenThrow(new ISE("some exception"));

    final Response response = (Response) clazz.getMethod(method).invoke(handler);
    Assert.assertEquals(404, response.getStatus());

    verify(entry).getCacheState();
    verify(entry, atMostOnce()).getCache();
    verify(versionedCache, atMostOnce()).getCache();
    verify(versionedCache).getVersion();
    verifyNoMoreInteractions(scheduler, entry, versionedCache);
  }
}
