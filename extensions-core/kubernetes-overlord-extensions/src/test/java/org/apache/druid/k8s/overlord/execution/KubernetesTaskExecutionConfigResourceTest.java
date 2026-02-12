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

import com.google.common.base.Suppliers;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerEffectiveConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerStaticConfig;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KubernetesTaskExecutionConfigResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private HttpServletRequest req;
  private static final KubernetesTaskRunnerEffectiveConfig DEFAULT_CONFIG = new KubernetesTaskRunnerEffectiveConfig(
      new KubernetesTaskRunnerStaticConfig(),
      null
  );
  private static final KubernetesTaskRunnerDynamicConfig DEFAULT_DYNAMIC_CONFIG =
      new DefaultKubernetesTaskRunnerDynamicConfig(
          DEFAULT_CONFIG.getPodTemplateSelectStrategy(),
          DEFAULT_CONFIG.getCapacity()
      );

  @Before
  public void setUp()
  {
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    auditManager = EasyMock.createMock(AuditManager.class);
    req = EasyMock.createMock(HttpServletRequest.class);
  }

  @Test
  public void setExecutionConfigSuccessfulUpdate()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );

    KubernetesTaskRunnerDynamicConfig inputConfig = new DefaultKubernetesTaskRunnerDynamicConfig(
        new TaskTypePodTemplateSelectStrategy(), 10
    );

    KubernetesTaskRunnerDynamicConfig expectedMerged = new DefaultKubernetesTaskRunnerDynamicConfig(
        new TaskTypePodTemplateSelectStrategy(), 10
    );

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        expectedMerged,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.ok());
    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(inputConfig, req);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfigFailedUpdate()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );

    KubernetesTaskRunnerDynamicConfig inputConfig = new DefaultKubernetesTaskRunnerDynamicConfig(
        new TaskTypePodTemplateSelectStrategy(), 10
    );

    KubernetesTaskRunnerDynamicConfig expectedMerged = new DefaultKubernetesTaskRunnerDynamicConfig(
        new TaskTypePodTemplateSelectStrategy(), 10
    );

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        expectedMerged,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.fail(new RuntimeException(), false));
    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(inputConfig, req);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentCapacityWhenRequestCapacityNull()
  {
    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 5);
    KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
        new KubernetesTaskRunnerStaticConfig(),
        Suppliers.ofInstance(currentDynamic)
    );

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        effectiveConfig
    );

    PodTemplateSelectStrategy requestStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, null);

    // Effective current: (TaskType, 5). Request: (TaskType, null). Merged: (TaskType, 5).
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
    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 2);
    KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
        new KubernetesTaskRunnerStaticConfig(),
        Suppliers.ofInstance(currentDynamic)
    );

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        effectiveConfig
    );

    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, 7);

    // Effective current: (TaskType, 2). Request: (null, 7). Merged: (TaskType, 7).
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
    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);
    KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
        new KubernetesTaskRunnerStaticConfig(),
        Suppliers.ofInstance(currentDynamic)
    );

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        effectiveConfig
    );

    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, null);

    // Effective current: (TaskType, 9). Request: (null, null). Merged: (TaskType, 9).
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
    EasyMock.replay(configManager, auditManager);

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );

    Response result = testedResource.getExecutionConfig();
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());

    KubernetesTaskRunnerDynamicConfig returnedConfig = (KubernetesTaskRunnerDynamicConfig) result.getEntity();
    assertNotNull(returnedConfig);
    assertEquals(DEFAULT_DYNAMIC_CONFIG, returnedConfig);
  }
}
