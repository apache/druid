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

package org.apache.druid.indextable.loader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indextable.loader.config.IndexedTableConfig;
import org.apache.druid.indextable.loader.config.IndexedTableLoaderConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

@RunWith(MockitoJUnitRunner.class)
public class IndexedTableLoaderDruidModuleTest
{
  private Properties properties;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private ObjectMapper mapper;
  @Mock
  private ObjectReader reader;
  @Mock
  private Map<String, IndexedTableConfig> loadMap;

  private IndexedTableLoaderDruidModule target;
  private Injector injector;

  @Before
  public void setUp() throws IOException
  {
    properties = new Properties();
    properties.setProperty("druid.indexedTable.loader.numThreads", "5");
    properties.setProperty("druid.indexedTable.loader.configFilePath", "/path/to/config");

    Mockito.when(mapper.readerFor(ArgumentMatchers.<TypeReference<ImmutableMap<String, IndexedTableConfig>>>any()))
           .thenReturn(reader);
    Mockito.when(reader.readValue(ArgumentMatchers.any(File.class))).thenReturn(loadMap);

    target = new IndexedTableLoaderDruidModule();
    injector = makeInjector();
  }

  @Test
  public void testInjectIndexTableLoadMapIsInjectable()
  {
    Map<String, IndexedTableConfig> injectedLoadMap = injector.getInstance(Key.get(new TypeLiteral<Map<String, IndexedTableConfig>>() {}));
    Assert.assertEquals(loadMap, injectedLoadMap);
  }
  @Test
  public void testInjectIndexedTableLoaderConfigWithoutNumThreadsIsInjectable()
  {
    properties.remove("druid.indexedTable.loader.numThreads");
    IndexedTableLoaderConfig config = injector.getInstance(IndexedTableLoaderConfig.class);
    Assert.assertNotNull(config);
  }

  @Test
  public void testInjectMapOfIndexTableConfigsIsInjectableAndSingleton()
  {
    Map<String, IndexedTableConfig> tableLoaders =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, IndexedTableConfig>>() {}));
    Map<String, IndexedTableConfig> otherTableLoaders =
        injector.getInstance(Key.get(new TypeLiteral<Map<String, IndexedTableConfig>>() {}));
    Assert.assertSame(tableLoaders, otherTableLoaders);
  }
  @Test(expected = ProvisionException.class)
  public void testInjectIndexedTableLoaderConfigWithoutPathToConfigShouldThrowProvisionException()
  {
    properties.remove("druid.indexedTable.loader.configFilePath");
    injector.getInstance(IndexedTableLoaderConfig.class);
  }

  @Test
  public void testInjectIndexedTableLoaderConfigIsInjectable()
  {
    IndexedTableLoaderConfig config = injector.getInstance(IndexedTableLoaderConfig.class);
    Assert.assertNotNull(config);
  }

  @Test
  public void testInjectIndexedTableManagerIsInjectableAndSingleton()
  {
    IndexedTableManager first = injector.getInstance(IndexedTableManager.class);
    IndexedTableManager second = injector.getInstance(IndexedTableManager.class);
    Assert.assertSame(first, second);
  }

  private Injector makeInjector()
  {
    return Guice.createInjector(target, new LifecycleModule(), new AbstractModule()
    {
      @Override
      protected void configure()
      {
        bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
        bind(Properties.class).toInstance(properties);
        bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(mapper);
        bindScope(LazySingleton.class, Scopes.SINGLETON);
      }
    });
  }
}
