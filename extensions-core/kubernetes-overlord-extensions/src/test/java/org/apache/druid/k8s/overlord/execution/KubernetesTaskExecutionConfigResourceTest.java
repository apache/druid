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

package org.apache.druid.k8s.overlord.execution;

import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerStaticConfig;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KubernetesTaskExecutionConfigResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private HttpServletRequest req;
  private KubernetesTaskRunnerDynamicConfig dynamicConfig;
  private KubernetesTaskRunnerStaticConfig staticConfig;

  private static final KubernetesTaskRunnerDynamicConfig DEFAULT_DYNAMIC_CONFIG =
      new DefaultKubernetesTaskRunnerDynamicConfig(
          KubernetesTaskRunnerDynamicConfig.DEFAULT_STRATEGY,
          Integer.MAX_VALUE
      );

  @Before
  public void setUp()
  {
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    auditManager = EasyMock.createMock(AuditManager.class);
    req = EasyMock.createMock(HttpServletRequest.class);
    dynamicConfig = EasyMock.createMock(KubernetesTaskRunnerDynamicConfig.class);
    staticConfig = new KubernetesTaskRunnerStaticConfig();
  }

  @Test
  public void setExecutionConfigSuccessfulUpdate()
  {
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(null));
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        dynamicConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.ok());
    EasyMock.replay(configManager, auditManager, dynamicConfig);

    Response result = testedResource.setExecutionConfig(dynamicConfig, req);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfigFailedUpdate()
  {
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(null));
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        dynamicConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.failure(new RuntimeException()));
    EasyMock.replay(configManager, auditManager, dynamicConfig);

    Response result = testedResource.setExecutionConfig(dynamicConfig, req);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentCapacityWhenRequestCapacityNull()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 5);
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(currentConfig));

    PodTemplateSelectStrategy requestStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, null);

    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, 5);

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        expectedMergedConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(requestConfig, req);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentStrategyWhenRequestStrategyNull()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 2);
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(currentConfig));

    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, 7);

    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 7);

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        expectedMergedConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(requestConfig, req);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentWhenBothRequestFieldsNull()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(currentConfig));

    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, null);

    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        expectedMergedConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(requestConfig, req);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void getExecutionConfig_ReturnsDefaultWhenNoConfigSet()
  {
    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class,
        DEFAULT_DYNAMIC_CONFIG
    )).andReturn(new AtomicReference<>(DEFAULT_DYNAMIC_CONFIG));
    EasyMock.replay(configManager, auditManager);

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        staticConfig
    );

    Response result = testedResource.getExecutionConfig();
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());

    KubernetesTaskRunnerDynamicConfig returnedConfig = (KubernetesTaskRunnerDynamicConfig) result.getEntity();
    assertNotNull(returnedConfig);
    assertEquals(DEFAULT_DYNAMIC_CONFIG, returnedConfig);
  }
}
