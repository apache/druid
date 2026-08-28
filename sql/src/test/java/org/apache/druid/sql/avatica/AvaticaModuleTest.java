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
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.eclipse.jetty.server.Handler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;
import java.util.Set;

@ExtendWith(EasyMockExtension.class)
public class AvaticaModuleTest
{
  private static final String HOST_AND_PORT = "HOST_AND_PORT";

  @Mock
  private DruidNode druidNode;
  @Mock
  private DruidMeta druidMeta;

  private AvaticaModule target;
  private Injector injector;

  @BeforeEach
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
    Assertions.assertNotNull(monitor);
    AvaticaMonitor other = injector.getInstance(AvaticaMonitor.class);
    Assertions.assertSame(monitor, other);
  }

  @Test
  public void testAvaticaMonitorIsRegisterdWithMetricsModule()
  {
    Set<Class<? extends Monitor>> monitors =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Assertions.assertTrue(monitors.contains(AvaticaMonitor.class));
  }

  @Test
  public void testAvaticaServerConfigIsInjectable()
  {
    AvaticaServerConfig config = injector.getInstance(AvaticaServerConfig.class);
    Assertions.assertNotNull(config);
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_MAX_CONNECTIONS, config.getMaxConnections());
    Assertions.assertEquals(
        AvaticaServerConfig.DEFAULT_MAX_STATEMENTS_PER_CONNECTION,
        config.getMaxStatementsPerConnection()
    );
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_CONNECTION_IDLE_TIMEOUT, config.getConnectionIdleTimeout());
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_MIN_ROWS_PER_FRAME, config.getMinRowsPerFrame());
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_MAX_ROWS_PER_FRAME, config.getMaxRowsPerFrame());
  }

  @Test
  public void testAvaticaServerConfigProperties()
  {
    Properties properties = new Properties();
    final JsonConfigProvider<AvaticaServerConfig> provider = JsonConfigProvider.of(
        "druid.sql.avatica",
        AvaticaServerConfig.class
    );
    properties.setProperty("druid.sql.avatica.maxRowsPerFrame", "50000");
    properties.setProperty("druid.sql.avatica.minRowsPerFrame", "10000");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final AvaticaServerConfig config = provider.get();
    Assertions.assertNotNull(config);
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_MAX_CONNECTIONS, config.getMaxConnections());
    Assertions.assertEquals(
        AvaticaServerConfig.DEFAULT_MAX_STATEMENTS_PER_CONNECTION,
        config.getMaxStatementsPerConnection()
    );
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_CONNECTION_IDLE_TIMEOUT, config.getConnectionIdleTimeout());
    Assertions.assertEquals(10_000, config.getMinRowsPerFrame());
    Assertions.assertEquals(50_000, config.getMaxRowsPerFrame());
  }

  @Test
  public void testAvaticaServerConfigPropertiesSmallerMaxIsAlsoMin()
  {
    Properties properties = new Properties();
    final JsonConfigProvider<AvaticaServerConfig> provider = JsonConfigProvider.of(
        "druid.sql.avatica",
        AvaticaServerConfig.class
    );
    properties.setProperty("druid.sql.avatica.maxRowsPerFrame", "50");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final AvaticaServerConfig config = provider.get();
    Assertions.assertNotNull(config);
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_MAX_CONNECTIONS, config.getMaxConnections());
    Assertions.assertEquals(
        AvaticaServerConfig.DEFAULT_MAX_STATEMENTS_PER_CONNECTION,
        config.getMaxStatementsPerConnection()
    );
    Assertions.assertEquals(AvaticaServerConfig.DEFAULT_CONNECTION_IDLE_TIMEOUT, config.getConnectionIdleTimeout());
    Assertions.assertEquals(50, config.getMinRowsPerFrame());
    Assertions.assertEquals(50, config.getMaxRowsPerFrame());
  }

  @Test
  public void testAvaticaServerConfigPropertiesBadMinRowsPerFrame()
  {
    Properties properties = new Properties();
    final JsonConfigProvider<AvaticaServerConfig> provider = JsonConfigProvider.of(
        "druid.sql.avatica",
        AvaticaServerConfig.class
    );
    properties.setProperty("druid.sql.avatica.minRowsPerFrame", "-1");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final AvaticaServerConfig config = provider.get();
    Assertions.assertNotNull(config);
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        config::getMinRowsPerFrame
    );
    Assertions.assertTrue(
        e.getMessage().contains("'druid.sql.avatica.minRowsPerFrame' must be set to a value greater than 0")
    );
  }

  @Test
  public void testDruidAvaticaJsonHandlerIsInjected()
  {
    DruidAvaticaJsonHandler handler = injector.getInstance(DruidAvaticaJsonHandler.class);
    Assertions.assertNotNull(handler);
    DruidAvaticaJsonHandler other = injector.getInstance(DruidAvaticaJsonHandler.class);
    Assertions.assertNotSame(handler, other);
  }

  @Test
  public void testDruidAvaticaProtobufHandlerIsInjected()
  {
    DruidAvaticaProtobufHandler handler = injector.getInstance(DruidAvaticaProtobufHandler.class);
    Assertions.assertNotNull(handler);
    DruidAvaticaProtobufHandler other = injector.getInstance(DruidAvaticaProtobufHandler.class);
    Assertions.assertNotSame(handler, other);
  }

  @Test
  public void testDruidAvaticaJsonHandlerIsRegisterdWithJerseyModule()
  {
    Set<Handler> handlers =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Assertions.assertTrue(handlers.stream().anyMatch(h -> DruidAvaticaJsonHandler.class.equals(h.getClass())));
  }

  @Test
  public void testDruidAvaticaProtobufHandlerIsRegisterdWithJerseyModule()
  {
    Set<Handler> handlers = injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Assertions.assertTrue(handlers.stream().anyMatch(h -> DruidAvaticaProtobufHandler.class.equals(h.getClass())));
  }
}
