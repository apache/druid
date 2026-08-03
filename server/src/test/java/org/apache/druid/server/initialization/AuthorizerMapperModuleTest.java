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

package org.apache.druid.server.initialization;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class AuthorizerMapperModuleTest
{
  private Injector injector;

  @Before
  public void setUp()
  {
    injector = Guice.createInjector(
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(new Properties());
          binder.bind(AuthConfig.class).toInstance(new AuthConfig());
          binder.bind(ServiceEmitter.class).toInstance(new NoopServiceEmitter());
        },
        new AuthorizerMapperModule()
    );
  }

  @Test
  public void testAuthorizerNameValidatorIsInjectedAsSingleton()
  {
    AuthValidator authValidator = injector.getInstance(AuthValidator.class);
    AuthValidator other = injector.getInstance(AuthValidator.class);
    Assert.assertSame(authValidator, other);
  }

  @Test
  public void testEmitAuthMetrics_defaultsFalse_emitterNullInMapper()
  {
    AuthorizerMapper mapper = injector.getInstance(AuthorizerMapper.class);
    Assert.assertNull(mapper.getServiceEmitter());
  }

  @Test
  public void testEmitAuthMetrics_true_emitterBoundToMapper()
  {
    StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    Injector inj = Guice.createInjector(
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(new Properties());
          binder.bind(AuthConfig.class).toInstance(AuthConfig.newBuilder().setEmitAuthMetrics(true).build());
          binder.bind(ServiceEmitter.class).toInstance(emitter);
        },
        new AuthorizerMapperModule()
    );
    AuthorizerMapper mapper = inj.getInstance(AuthorizerMapper.class);
    Assert.assertSame(emitter, mapper.getServiceEmitter());
  }
}
