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

package org.apache.druid.server.audit;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class AuditManagerConfigTest
{
  private static final String CONFIG_BASE = "druid.audit.manager";

  @Test
  public void testDefaultAuditConfig()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<AuditManagerConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        AuditManagerConfig.class
    );

    provider.inject(new Properties(), injector.getInstance(JsonConfigurator.class));
    final AuditManagerConfig config = provider.get();
    Assert.assertTrue(config instanceof SQLAuditManagerConfig);

    final SQLAuditManagerConfig sqlAuditConfig = (SQLAuditManagerConfig) config;
    Assert.assertTrue(sqlAuditConfig.isAuditSystemRequests());
    Assert.assertFalse(sqlAuditConfig.isSkipNullField());
    Assert.assertFalse(sqlAuditConfig.isIncludePayloadAsDimensionInMetric());
    Assert.assertEquals(-1, sqlAuditConfig.getMaxPayloadSizeBytes());
    Assert.assertEquals(7 * 86400 * 1000, sqlAuditConfig.getAuditHistoryMillis());
  }

  @Test
  public void testLogAuditConfigWithDefaults()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<AuditManagerConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        AuditManagerConfig.class
    );

    final Properties props = new Properties();
    props.setProperty("druid.audit.manager.type", "log");

    provider.inject(props, injector.getInstance(JsonConfigurator.class));
    final AuditManagerConfig config = provider.get();
    Assert.assertTrue(config instanceof LoggingAuditManagerConfig);

    final LoggingAuditManagerConfig logAuditConfig = (LoggingAuditManagerConfig) config;
    Assert.assertTrue(logAuditConfig.isAuditSystemRequests());
    Assert.assertFalse(logAuditConfig.isSkipNullField());
    Assert.assertEquals(-1, logAuditConfig.getMaxPayloadSizeBytes());
    Assert.assertEquals(AuditLogger.Level.INFO, logAuditConfig.getLogLevel());
  }

  @Test
  public void testLogAuditConfigWithOverrides()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<AuditManagerConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        AuditManagerConfig.class
    );

    final Properties props = new Properties();
    props.setProperty("druid.audit.manager.type", "log");
    props.setProperty("druid.audit.manager.logLevel", "WARN");
    props.setProperty("druid.audit.manager.auditSystemRequests", "true");

    provider.inject(props, injector.getInstance(JsonConfigurator.class));

    final AuditManagerConfig config = provider.get();
    Assert.assertTrue(config instanceof LoggingAuditManagerConfig);

    final LoggingAuditManagerConfig logAuditConfig = (LoggingAuditManagerConfig) config;
    Assert.assertTrue(logAuditConfig.isAuditSystemRequests());
    Assert.assertFalse(logAuditConfig.isSkipNullField());
    Assert.assertEquals(-1, logAuditConfig.getMaxPayloadSizeBytes());
    Assert.assertEquals(AuditLogger.Level.WARN, logAuditConfig.getLogLevel());
  }

  @Test
  public void testSqlAuditConfigWithOverrides()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<AuditManagerConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        AuditManagerConfig.class
    );

    final Properties props = new Properties();
    props.setProperty("druid.audit.manager.type", "sql");
    props.setProperty("druid.audit.manager.skipNullField", "true");
    props.setProperty("druid.audit.manager.maxPayloadSizeBytes", "100");
    props.setProperty("druid.audit.manager.auditHistoryMillis", "1000");
    props.setProperty("druid.audit.manager.includePayloadAsDimensionInMetric", "true");

    provider.inject(props, injector.getInstance(JsonConfigurator.class));

    final AuditManagerConfig config = provider.get();
    Assert.assertTrue(config instanceof SQLAuditManagerConfig);

    final SQLAuditManagerConfig sqlAuditConfig = (SQLAuditManagerConfig) config;
    Assert.assertTrue(sqlAuditConfig.isSkipNullField());
    Assert.assertTrue(sqlAuditConfig.isIncludePayloadAsDimensionInMetric());
    Assert.assertEquals(100, sqlAuditConfig.getMaxPayloadSizeBytes());
    Assert.assertEquals(1000L, sqlAuditConfig.getAuditHistoryMillis());
  }

  private Injector createInjector()
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> JsonConfigProvider.bind(binder, CONFIG_BASE, AuditManagerConfig.class)
        )
    );
  }
}
