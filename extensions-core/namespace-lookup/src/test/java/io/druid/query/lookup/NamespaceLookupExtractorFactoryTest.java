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

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.ISE;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.server.DruidNode;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import java.util.concurrent.ConcurrentHashMap;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

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
    EasyMock.expect(
        cacheManager.checkedDelete(EasyMock.anyString())
    ).andReturn(false).once();
    EasyMock.replay(cacheManager);

    final NamespaceLookupExtractorFactory namespaceLookupExtractorFactory = new NamespaceLookupExtractorFactory(
        extractionNamespace,
        cacheManager
    );
    Assert.assertFalse(namespaceLookupExtractorFactory.start());
    Assert.assertFalse(namespaceLookupExtractorFactory.close());
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
    final LookupExtractorFactory factory = mapper.readValue(
        "{ \"type\": \"cachedNamespace\", \"extractionNamespace\": { \"type\": \"uri\", \"uriPrefix\": \"s3://bucket/prefix/\", \"fileRegex\": \"foo.*\\\\.gz\", \"namespaceParseSpec\": { \"format\": \"customJson\", \"keyFieldName\": \"someKey\", \"valueFieldName\": \"someVal\" }, \"pollPeriod\": \"PT5M\" } } }",
        LookupExtractorFactory.class
    );
    Assert.assertTrue(factory instanceof NamespaceLookupExtractorFactory);
    Assert.assertNotNull(mapper.writeValueAsString(factory));
  }
}
