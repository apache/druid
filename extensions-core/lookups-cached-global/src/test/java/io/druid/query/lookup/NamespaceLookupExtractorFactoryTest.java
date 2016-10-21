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
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.server.DruidNode;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceLookupExtractorFactoryTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final NamespaceExtractionCacheManager cacheManager = EasyMock.createStrictMock(NamespaceExtractionCacheManager.class);

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
            if ("io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager".equals(valueId)) {
              return cacheManager;
            }
            return null;
          }
        }
    );
  }

  @Test
  public void testSimpleSerde() throws Exception
  {
    final URIExtractionNamespace uriExtractionNamespace = new URIExtractionNamespace(
        temporaryFolder.newFolder().toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper),

        Period.millis(0),
        null
    );
    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        uriExtractionNamespace,
        cacheManager
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
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(
        cacheManager.checkedDelete(EasyMock.anyString())
    ).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
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
    EasyMock.expect(cacheManager.scheduleOrUpdate(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace)
    )).andReturn(true).once();
    EasyMock.expect(
        cacheManager.checkedDelete(EasyMock.anyString())
    ).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        0,
        false,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testStartReturnsImmediatelyAndFails()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(cacheManager.scheduleOrUpdate(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace)
    )).andReturn(false).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        0,
        false,
        cacheManager
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    EasyMock.verify(cacheManager);
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
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(
        cacheManager.checkedDelete(EasyMock.anyString())
    ).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
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
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    EasyMock.verify(cacheManager);
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
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("0").once();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("0").once();
    EasyMock.expect(
        cacheManager.checkedDelete(EasyMock.anyString())
    ).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    final LookupExtractor extractor = namespaceLookupExtractorFactory.get();
    Assert.assertNull(extractor.apply("foo"));
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
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
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("0").once();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn(null).once();
    EasyMock.expect(cacheManager.delete(EasyMock.anyString())).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    try {
      namespaceLookupExtractorFactory.get();
      Assert.fail("Should have thrown ISE");
    }
    catch (ISE ise) {
      // NOOP
    }

    EasyMock.verify(cacheManager);
  }


  @Test
  public void testSimpleStartRacyGetDuringUpdate()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("0").once();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>(ImmutableMap.of("foo", "bar")))
            .once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("1").once();

    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("2").once();
    EasyMock.expect(cacheManager.getCacheMap(EasyMock.anyString()))
            .andReturn(new ConcurrentHashMap<String, String>())
            .once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn("2").once();
    EasyMock.expect(cacheManager.checkedDelete(EasyMock.anyString())).andReturn(true).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    final LookupExtractor extractor = namespaceLookupExtractorFactory.get();
    Assert.assertNull(extractor.apply("foo"));
    Assert.assertNotNull(extractor.getCacheKey());
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
  }


  @Test
  public void testSimpleStartRacyGetAfterDelete()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(true).once();
    EasyMock.expect(cacheManager.getVersion(EasyMock.anyString())).andReturn(null).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertTrue(namespaceLookupExtractorFactory.start());
    try {
      namespaceLookupExtractorFactory.get();
      Assert.fail("Should have thrown ISE");
    }
    catch (ISE ise) {
      // NOOP
    }

    EasyMock.verify(cacheManager);
  }


  @Test
  public void testSartFailsToSchedule()
  {
    final ExtractionNamespace extractionNamespace = new ExtractionNamespace()
    {
      @Override
      public long getPollMs()
      {
        return 0;
      }
    };
    EasyMock.expect(cacheManager.scheduleAndWait(
        EasyMock.anyString(),
        EasyMock.eq(extractionNamespace),
        EasyMock.eq(60000L)
    )).andReturn(false).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    // true because it never fully started
    Assert.assertTrue(namespaceLookupExtractorFactory.close());
    EasyMock.verify(cacheManager);
  }

  @Test
  public void testReplaces()
  {
    final ExtractionNamespace en1 = EasyMock.createStrictMock(ExtractionNamespace.class), en2 = EasyMock.createStrictMock(
        ExtractionNamespace.class);
    EasyMock.replay(en1, en2);
    final NamespaceLookupExtractorFactory f1 = new NamespaceLookupExtractorFactory(
        en1,
        cacheManager
    ), f2 = new NamespaceLookupExtractorFactory(en2, cacheManager), f1b = new NamespaceLookupExtractorFactory(
        en1,
        cacheManager
    );
    Assert.assertTrue(f1.replaces(f2));
    Assert.assertTrue(f2.replaces(f1));
    Assert.assertFalse(f1.replaces(f1b));
    Assert.assertFalse(f1b.replaces(f1));
    Assert.assertFalse(f1.replaces(f1));
    Assert.assertTrue(f1.replaces(EasyMock.createNiceMock(LookupExtractorFactory.class)));
    EasyMock.verify(en1, en2);
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
        cacheManager
    );

    namespaceLookupExtractorFactory.get();
  }

  // Note this does NOT catch problems with returning factories as failed in error messages.
  @Test
  public void testSerDe() throws Exception
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                );
              }
            }
        )
    );
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
        URIExtractionNamespace.class,
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
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                );
              }
            }
        )
    );
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
  public void testExceptionalIntrospectionHandler() throws Exception
  {
    final NamespaceExtractionCacheManager manager = EasyMock.createStrictMock(NamespaceExtractionCacheManager.class);
    final ExtractionNamespace extractionNamespace = EasyMock.createStrictMock(ExtractionNamespace.class);
    EasyMock.expect(manager.scheduleAndWait(EasyMock.anyString(), EasyMock.eq(extractionNamespace), EasyMock.anyLong()))
            .andReturn(true)
            .once();
    EasyMock.replay(manager);
    final LookupExtractorFactory lookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        manager
    );
    Assert.assertTrue(lookupExtractorFactory.start());

    final LookupIntrospectHandler handler = lookupExtractorFactory.getIntrospectHandler();
    Assert.assertNotNull(handler);
    final Class<? extends LookupIntrospectHandler> clazz = handler.getClass();

    synchronized (manager) {
      EasyMock.verify(manager);
      EasyMock.reset(manager);
      EasyMock.expect(manager.getVersion(EasyMock.anyString())).andReturn(null).once();
      EasyMock.replay(manager);
    }
    final Response response = (Response) clazz.getMethod("getVersion").invoke(handler);
    Assert.assertEquals(404, response.getStatus());


    validateCode(
        new ISE("some exception"),
        404,
        "getKeys",
        handler,
        manager,
        clazz
    );

    validateCode(
        new ISE("some exception"),
        404,
        "getValues",
        handler,
        manager,
        clazz
    );

    validateCode(
        new ISE("some exception"),
        404,
        "getMap",
        handler,
        manager,
        clazz
    );

    EasyMock.verify(manager);
  }

  void validateCode(
      Throwable thrown,
      int expectedCode,
      String method,
      LookupIntrospectHandler handler,
      NamespaceExtractionCacheManager manager,
      Class<? extends LookupIntrospectHandler> clazz
  ) throws Exception
  {
    synchronized (manager) {
      EasyMock.verify(manager);
      EasyMock.reset(manager);
      EasyMock.expect(manager.getVersion(EasyMock.anyString())).andThrow(thrown).once();
      EasyMock.replay(manager);
    }
    final Response response = (Response) clazz.getMethod(method).invoke(handler);
    Assert.assertEquals(expectedCode, response.getStatus());
  }
}
