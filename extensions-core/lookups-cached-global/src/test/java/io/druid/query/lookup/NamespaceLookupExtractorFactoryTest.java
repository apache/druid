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

package io.druid.query.lookup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.UriExtractionNamespace;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.lookup.namespace.cache.CacheScheduler;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    NamespaceExtractionCacheManager.class,
    CacheScheduler.class,
    CacheScheduler.VersionedCache.class,
    CacheScheduler.Entry.class
})
@PowerMockIgnore("javax.net.ssl.*")
public class NamespaceLookupExtractorFactoryTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final CacheScheduler scheduler = PowerMock.createStrictMock(CacheScheduler.class);
  private final CacheScheduler.Entry entry = PowerMock.createStrictMock(CacheScheduler.Entry.class);
  private final CacheScheduler.VersionedCache versionedCache =
      PowerMock.createStrictMock(CacheScheduler.VersionedCache.class);

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
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
    expectedException.expectMessage("extractionNamespace should be specified");
    new NamespaceLookupExtractorFactory(null, null);
  }

  @Test
  public void testSimpleStartStop()
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
    expectEntryCloseOnce();
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    mockVerify();
  }

  @Test
  public void testStartReturnsImmediately()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(scheduler.schedule(EasyMock.eq(extractionNamespace))).andReturn(entry).once();
    expectEntryCloseOnce();
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        0,
        false,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    mockVerify();
  }

  private void expectEntryCloseOnce()
  {
    entry.close();
    EasyMock.expectLastCall().once();
  }

  @Test
  public void testStartReturnsImmediatelyAndFails() throws InterruptedException
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(scheduler.scheduleAndWait(EasyMock.eq(extractionNamespace), EasyMock.eq(1L)))
            .andReturn(null).once();
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        1,
        false,
        scheduler
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    mockVerify();
  }

  @Test
  public void testSimpleStartStopStop()
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
    expectEntryCloseOnce();
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    mockVerify();
  }

  @Test
  public void testSimpleStartStart()
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
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    mockVerify();
  }


  @Test
  public void testSimpleStartGetStop()
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
    expectEntryGetCacheStateOnce(versionedCache);
    expectEmptyCache();
    expectVersionOnce("0");
    expectEntryCloseOnce();
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    final LookupExtractor extractor = namespaceLookupExtractorFactory.get();
    Assert.assertNull(extractor.apply("foo"));
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    mockVerify();
  }

  private void expectEmptyCache()
  {
    EasyMock.expect(entry.getCache()).andReturn(new HashMap<String, String>()).anyTimes();
    EasyMock.expect(versionedCache.getCache()).andReturn(new HashMap<String, String>()).anyTimes();
  }


  @Test
  public void testSimpleStartRacyGetDuringDelete()
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
    expectEntryGetCacheStateOnce(CacheScheduler.NoCache.ENTRY_CLOSED);

    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    try {
      namespaceLookupExtractorFactory.get();
      Assert.fail("Should have thrown ISE");
    }
    catch (ISE ise) {
      // NOOP
    }

    mockVerify();
  }

  private void expectEntryGetCacheStateOnce(final CacheScheduler.CacheState versionedCache)
  {
    EasyMock.expect(entry.getCacheState()).andReturn(versionedCache).once();
  }

  private IExpectationSetters<String> expectVersionOnce(String version)
  {
    return EasyMock.expect(versionedCache.getVersion()).andReturn(version).once();
  }

  private void expectFooBarCache()
  {
    EasyMock.expect(versionedCache.getCache()).andReturn(new HashMap<>(ImmutableMap.of("foo", "bar"))).once();
  }

  private void expectScheduleAndWaitOnce(ExtractionNamespace extractionNamespace)
  {
    try {
      EasyMock.expect(scheduler.scheduleAndWait(
          EasyMock.eq(extractionNamespace),
          EasyMock.eq(60000L)
      )).andReturn(entry).once();
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }


  @Test
  public void testStartFailsToSchedule()
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
      EasyMock.expect(scheduler.scheduleAndWait(
          EasyMock.eq(extractionNamespace),
          EasyMock.eq(60000L)
      )).andReturn(null).once();
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }
    mockReplay();

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    // true because it never fully started
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    mockVerify();
  }

  @Test
  public void testReplaces()
  {
    final ExtractionNamespace en1 = PowerMock.createStrictMock(ExtractionNamespace.class), en2 = PowerMock.createStrictMock(
        ExtractionNamespace.class);
    PowerMock.replay(en1, en2);
    final NamespaceLookupExtractorFactory f1 = new NamespaceLookupExtractorFactory(
        en1,
        scheduler
    ), f2 = new NamespaceLookupExtractorFactory(en2, scheduler), f1b = new NamespaceLookupExtractorFactory(
        en1,
        scheduler
    );
    Assert.assertTrue(f1.replaces(f2));
    Assert.assertTrue(f2.replaces(f1));
    Assert.assertFalse(f1.replaces(f1b));
    Assert.assertFalse(f1b.replaces(f1));
    Assert.assertFalse(f1.replaces(f1));
    Assert.assertTrue(f1.replaces(EasyMock.createNiceMock(LookupExtractorFactory.class)));
    PowerMock.verify(en1, en2);
  }

  @Test(expected = ISE.class)
  public void testMustBeStarted()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };

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
    final String str = "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"uri\", \"uriPrefix\": \"s3://bucket/prefix/\", \"fileRegex\": \"foo.*\\\\.gz\", \"namespaceParseSpec\": { \"format\": \"customJson\", \"keyFieldName\": \"someKey\", \"valueFieldName\": \"someVal\" }, \"pollPeriod\": \"PT5M\" } } }";
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
    final Map<String, Object> map = new HashMap<>(mapper.<Map<String, Object>>readValue(
        str,
        new TypeReference<Map<String, Object>>()
        {
        }
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
          ImmutableSet.of("bar"),
          ((Response) clazz.getMethod("getValues").invoke(handler)).getEntity()
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
    Assert.assertTrue(factory1.getCacheScheduler() == factory2.getCacheScheduler());
  }

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null, null, new ServerConfig())
                );
              }
            }
        )
    );
  }

  @Test
  public void testExceptionalIntrospectionHandler() throws Exception
  {
    final ExtractionNamespace extractionNamespace = PowerMock.createStrictMock(ExtractionNamespace.class);
    EasyMock.expect(scheduler.scheduleAndWait(EasyMock.eq(extractionNamespace), EasyMock.anyLong()))
            .andReturn(entry)
            .once();
    mockReplay();
    final LookupExtractorFactory lookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        scheduler
    );
    Assert.assertTrue(lookupExtractorFactory.start());

    final LookupIntrospectHandler handler = lookupExtractorFactory.getIntrospectHandler();
    Assert.assertNotNull(handler);
    final Class<? extends LookupIntrospectHandler> clazz = handler.getClass();

    mockVerify();
    mockReset();
    EasyMock.expect(entry.getCacheState()).andReturn(CacheScheduler.NoCache.CACHE_NOT_INITIALIZED).once();
    mockReplay();

    final Response response = (Response) clazz.getMethod("getVersion").invoke(handler);
    Assert.assertEquals(404, response.getStatus());

    validateNotFound("getKeys", handler, clazz);
    validateNotFound("getValues", handler, clazz);
    validateNotFound("getMap", handler, clazz);
    mockVerify();
  }

  private void mockReplay()
  {
    PowerMock.replay(scheduler, entry, versionedCache);
  }

  private void mockReset()
  {
    PowerMock.reset(scheduler, entry, versionedCache);
  }

  private void mockVerify()
  {
    PowerMock.verify(scheduler, entry, versionedCache);
  }

  private void validateNotFound(
      String method,
      LookupIntrospectHandler handler,
      Class<? extends LookupIntrospectHandler> clazz
  ) throws Exception
  {
    mockVerify();
    mockReset();
    expectEntryGetCacheStateOnce(versionedCache);
    expectEmptyCache();
    EasyMock.expect(versionedCache.getVersion()).andThrow(new ISE("some exception")).once();
    mockReplay();
    final Response response = (Response) clazz.getMethod(method).invoke(handler);
    Assert.assertEquals(404, response.getStatus());
  }
}
