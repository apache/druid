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

package io.druid.common.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.LazySingleton;
import io.druid.metadata.DefaultPasswordProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;
import java.util.UUID;

public class AWSCredentialsConfigTest
{
  private static final String PROPERTY_PREFIX = UUID.randomUUID().toString();
  private static final String SOME_SECRET = "someSecret";
  private final Properties properties = new Properties();

  @Before
  public void setUp()
  {
    cleanProperties();
  }

  @After
  public void tearDown()
  {
    cleanProperties();
  }

  private void cleanProperties()
  {
    properties.clear();
  }

  @Test
  public void testStringProperty()
  {
    properties.put(PROPERTY_PREFIX + ".accessKey", SOME_SECRET);
    properties.put(PROPERTY_PREFIX + ".secretKey", SOME_SECRET);

    final Injector injector = Guice.createInjector(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          JsonConfigProvider.bind(binder, PROPERTY_PREFIX, AWSCredentialsConfig.class);
        }
    );
    final AWSCredentialsConfig credentialsConfig = injector.getInstance(AWSCredentialsConfig.class);
    Assert.assertEquals(SOME_SECRET, credentialsConfig.getAccessKey().getPassword());
    Assert.assertEquals(SOME_SECRET, credentialsConfig.getSecretKey().getPassword());
  }

  @Test
  public void testJsonProperty() throws Exception
  {
    final String someSecret = new ObjectMapper().writeValueAsString(new DefaultPasswordProvider(SOME_SECRET));
    properties.put(PROPERTY_PREFIX + ".accessKey", someSecret);
    properties.put(PROPERTY_PREFIX + ".secretKey", someSecret);

    final Injector injector = Guice.createInjector(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/redis");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          JsonConfigProvider.bind(binder, PROPERTY_PREFIX, AWSCredentialsConfig.class);
        }
    );
    final AWSCredentialsConfig credentialsConfig = injector.getInstance(AWSCredentialsConfig.class);
    Assert.assertEquals(SOME_SECRET, credentialsConfig.getAccessKey().getPassword());
    Assert.assertEquals(SOME_SECRET, credentialsConfig.getSecretKey().getPassword());
  }
}
