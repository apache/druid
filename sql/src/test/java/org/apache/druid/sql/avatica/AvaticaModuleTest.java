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

package org.apache.druid.sql.avatica;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.eclipse.jetty.server.Handler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class AvaticaModuleTest
{
  private static final String HOST_AND_PORT = "HOST_AND_PORT";

  @Mock
  private DruidNode druidNode;
  @Mock
  private DruidMeta druidMeta;

  private AvaticaModule target;
  private Injector injector;

  @Before
  public void setUp()
  {
    EasyMock.expect(druidNode.getHostAndPortToUse()).andStubReturn(HOST_AND_PORT);
    EasyMock.replay(druidNode);
    target = new AvaticaModule();
    injector = Guice.createInjector(
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(DruidNode.class).annotatedWith(Self.class).toInstance(druidNode);
          binder.bind(DruidMeta.class).toInstance(druidMeta);
        },
        target
    );
  }

  @Test
  public void testAvaticaMonitorIsInjectedAsSingleton()
  {
    AvaticaMonitor monitor = injector.getInstance(AvaticaMonitor.class);
    Assert.assertNotNull(monitor);
    AvaticaMonitor other = injector.getInstance(AvaticaMonitor.class);
    Assert.assertSame(monitor, other);
  }

  @Test
  public void testAvaticaMonitorIsRegisterdWithMetricsModule()
  {
    Set<Class<? extends Monitor>> monitors =
        injector.getInstance(Key.get(new TypeLiteral<Set<Class<? extends Monitor>>>(){}));
    Assert.assertTrue(monitors.contains(AvaticaMonitor.class));
  }

  @Test
  public void testAvaticaServerConfigIsInjectable()
  {
    AvaticaServerConfig config = injector.getInstance(AvaticaServerConfig.class);
    Assert.assertNotNull(config);
  }

  @Test
  public void testDruidAvaticaHandlerIsInjected()
  {
    DruidAvaticaHandler handler = injector.getInstance(DruidAvaticaHandler.class);
    Assert.assertNotNull(handler);
    DruidAvaticaHandler other = injector.getInstance(DruidAvaticaHandler.class);
    Assert.assertNotSame(handler, other);
  }

  @Test
  public void testDruidAvaticaHandlerIsRegisterdWithJerseyModule()
  {
    Set<Handler> handlers =
        injector.getInstance(Key.get(new TypeLiteral<Set<Handler>>(){}));
    Assert.assertTrue(handlers.stream().anyMatch(h -> DruidAvaticaHandler.class.equals(h.getClass())));
  }
}
