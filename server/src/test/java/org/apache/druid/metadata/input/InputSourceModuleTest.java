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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.jackson.JacksonModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class InputSourceModuleTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final String SQL_NAMED_TYPE = "sql";

  @Before
  public void setUp()
  {
    InputSourceModule inputSourceModule = new InputSourceModule();
    for (Module jacksonModule : inputSourceModule.getJacksonModules()) {
      mapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testSubTypeRegistration()
  {
    MapperConfig config = mapper.getDeserializationConfig();
    AnnotatedClass annotatedClass = AnnotatedClassResolver.resolveWithoutSuperTypes(config, SqlInputSource.class);
    List<String> subtypes = mapper.getSubtypeResolver()
                                  .collectAndResolveSubtypesByClass(config, annotatedClass)
                                  .stream()
                                  .map(NamedType::getName)
                                  .collect(Collectors.toList());
    Assert.assertNotNull(subtypes);
    Assert.assertEquals(SQL_NAMED_TYPE, Iterables.getOnlyElement(subtypes));
  }

  @Test
  public void testHttpInputSourceAllowConfig()
  {
    Properties props = new Properties();
    props.put("druid.ingestion.http.allowListDomains", "[\"allow.com\"]");
    Injector injector = makeInjectorWithProperties(props);
    HttpInputSourceConfig instance = injector.getInstance(HttpInputSourceConfig.class);
    Assert.assertEquals(new HttpInputSourceConfig(Collections.singletonList("allow.com"), null), instance);
  }

  @Test
  public void testHttpInputSourceDenyConfig()
  {
    Properties props = new Properties();
    props.put("druid.ingestion.http.denyListDomains", "[\"deny.com\"]");
    Injector injector = makeInjectorWithProperties(props);
    HttpInputSourceConfig instance = injector.getInstance(HttpInputSourceConfig.class);
    Assert.assertEquals(new HttpInputSourceConfig(null, Collections.singletonList("deny.com")), instance);
  }

  @Test(expected = ProvisionException.class)
  public void testHttpInputSourceBothAllowDenyConfig()
  {
    Properties props = new Properties();
    props.put("druid.ingestion.http.allowListDomains", "[\"allow.com\"]");
    props.put("druid.ingestion.http.denyListDomains", "[\"deny.com\"]");
    Injector injector = makeInjectorWithProperties(props);
    injector.getInstance(HttpInputSourceConfig.class);
  }

  @Test
  public void testHttpInputSourceDefaultConfig()
  {
    Properties props = new Properties();
    Injector injector = makeInjectorWithProperties(props);
    HttpInputSourceConfig instance = injector.getInstance(HttpInputSourceConfig.class);
    Assert.assertEquals(new HttpInputSourceConfig(null, null), instance);
    Assert.assertNull(instance.getAllowListDomains());
    Assert.assertNull(instance.getDenyListDomains());
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new LifecycleModule(),
            new ServerModule(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
            },
            new JacksonModule(),
            new InputSourceModule()
        ));
  }
}
