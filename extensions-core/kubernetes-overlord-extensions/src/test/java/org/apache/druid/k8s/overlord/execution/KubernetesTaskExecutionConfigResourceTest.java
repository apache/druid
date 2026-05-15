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
import com.google.common.collect.ImmutableList;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerEffectiveConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerStaticConfig;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KubernetesTaskExecutionConfigResourceTest
{
  private static final KubernetesTaskRunnerEffectiveConfig DEFAULT_CONFIG = new KubernetesTaskRunnerEffectiveConfig(
      new KubernetesTaskRunnerStaticConfig(),
      null
  );
  private static final KubernetesTaskRunnerDynamicConfig DEFAULT_DYNAMIC_CONFIG =
      new DefaultKubernetesTaskRunnerDynamicConfig(
          DEFAULT_CONFIG.getPodTemplateSelectStrategy(),
          DEFAULT_CONFIG.getCapacity()
      );

  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private HttpServletRequest req;

  @BeforeEach
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

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(null));
    expectAuditInfoRequest();
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        inputConfig,
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

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(null));
    expectAuditInfoRequest();
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        inputConfig,
        AuthorizationUtils.buildAuditInfo(req)
    )).andReturn(ConfigManager.SetResult.failure(new RuntimeException()));
    EasyMock.replay(configManager, auditManager);

    Response result = testedResource.setExecutionConfig(inputConfig, req);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentCapacityWhenRequestCapacityNull()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 5);
    PodTemplateSelectStrategy requestStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, null);
    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, 5);

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(currentDynamic));
    expectAuditInfoRequest();
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
        DEFAULT_CONFIG
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 2);
    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, 7);
    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 7);

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(currentDynamic));
    expectAuditInfoRequest();
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
        DEFAULT_CONFIG
    );

    PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    KubernetesTaskRunnerDynamicConfig currentDynamic = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);
    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, null);
    KubernetesTaskRunnerDynamicConfig expectedMergedConfig = new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(currentDynamic));
    expectAuditInfoRequest();
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
  public void setExecutionConfig_DoesNotPersistStaticFallbackValues()
  {
    KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
        KubernetesTaskRunnerConfig.builder()
                                  .withCapacity(10)
                                  .build(),
        null
    );
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        effectiveConfig
    );

    KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(
        new TaskTypePodTemplateSelectStrategy(),
        null
    );

    EasyMock.expect(configManager.watch(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.class
    )).andReturn(new AtomicReference<>(null));
    expectAuditInfoRequest();
    EasyMock.expect(configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        requestConfig,
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

  @Test
  public void getExecutionConfig_ReturnsEffectiveConfig()
  {
    KubernetesTaskRunnerEffectiveConfig effectiveConfig = new KubernetesTaskRunnerEffectiveConfig(
        KubernetesTaskRunnerConfig.builder()
                                  .withCapacity(10)
                                  .build(),
        Suppliers.ofInstance(
            new DefaultKubernetesTaskRunnerDynamicConfig(
                new TaskTypePodTemplateSelectStrategy(),
                null
            )
        )
    );
    EasyMock.replay(configManager, auditManager);

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        effectiveConfig
    );

    Response result = testedResource.getExecutionConfig();
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());

    KubernetesTaskRunnerDynamicConfig returnedConfig = (KubernetesTaskRunnerDynamicConfig) result.getEntity();
    assertEquals(new DefaultKubernetesTaskRunnerDynamicConfig(new TaskTypePodTemplateSelectStrategy(), 10), returnedConfig);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getExecutionConfigHistory_SanityCheck()
  {
    AuditInfo admin = new AuditInfo("admin", "crewmate", "initial setup", "10.0.0.1");
    AuditInfo operator = new AuditInfo("operator", "imposter", "scaled up capacity", "10.0.0.2");
    AuditInfo paranoidUser = new AuditInfo("paranoid-user", "crewmate", "rollback to safe config", "10.0.0.3");

    String configKey = KubernetesTaskRunnerDynamicConfig.CONFIG_KEY;

    AuditEntry entry1 = AuditEntry.builder()
        .key(configKey)
        .type(configKey)
        .auditInfo(admin)
        .serializedPayload("{\"type\":\"default\",\"podTemplateSelectStrategy\":{\"type\":\"taskType\"},\"capacity\":5}")
        .auditTime(DateTimes.of("2024-06-01T10:00:00Z"))
        .build();
    AuditEntry entry2 = AuditEntry.builder()
        .key(configKey)
        .type(configKey)
        .auditInfo(operator)
        .serializedPayload("{\"type\":\"default\",\"podTemplateSelectStrategy\":{\"type\":\"taskType\"},\"capacity\":20}")
        .auditTime(DateTimes.of("2024-09-15T14:30:00Z"))
        .build();
    AuditEntry entry3 = AuditEntry.builder()
        .key(configKey)
        .type(configKey)
        .auditInfo(paranoidUser)
        .serializedPayload("{\"type\":\"default\",\"podTemplateSelectStrategy\":{\"type\":\"taskType\"},\"capacity\":10}")
        .auditTime(DateTimes.of("2024-11-20T08:00:00Z"))
        .build();

    List<AuditEntry> fullHistory = ImmutableList.of(entry3, entry2, entry1);
    List<AuditEntry> lastTwo = ImmutableList.of(entry3, entry2);
    String intervalStr = "2024-06-01/2024-10-01";
    Interval interval = Intervals.of(intervalStr);
    List<AuditEntry> intervalFiltered = ImmutableList.of(entry2, entry1);

    auditManager = EasyMock.createMock(AuditManager.class);
    EasyMock.expect(auditManager.fetchAuditHistory(configKey, configKey, 2)).andReturn(lastTwo);
    EasyMock.replay(configManager, auditManager);

    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );
    Response result = testedResource.getExecutionConfigHistory(null, 2);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    List<AuditEntry> resultEntries = (List<AuditEntry>) result.getEntity();
    assertEquals(2, resultEntries.size());
    assertEquals(lastTwo, resultEntries);
    EasyMock.verify(auditManager);

    EasyMock.reset(configManager, auditManager);
    EasyMock.expect(auditManager.fetchAuditHistory(configKey, configKey, interval)).andReturn(intervalFiltered);
    EasyMock.replay(configManager, auditManager);

    testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );
    result = testedResource.getExecutionConfigHistory(intervalStr, null);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    resultEntries = (List<AuditEntry>) result.getEntity();
    assertEquals(2, resultEntries.size());
    assertEquals(intervalFiltered, resultEntries);
    EasyMock.verify(auditManager);

    EasyMock.reset(configManager, auditManager);
    EasyMock.expect(auditManager.fetchAuditHistory(configKey, configKey, interval)).andReturn(intervalFiltered);
    EasyMock.replay(configManager, auditManager);

    testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );
    result = testedResource.getExecutionConfigHistory(intervalStr, 99);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    assertEquals(intervalFiltered, result.getEntity());
    EasyMock.verify(auditManager);

    EasyMock.reset(configManager, auditManager);
    EasyMock.expect(auditManager.fetchAuditHistory(configKey, configKey, null)).andReturn(fullHistory);
    EasyMock.replay(configManager, auditManager);

    testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );
    result = testedResource.getExecutionConfigHistory(null, null);
    assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    resultEntries = (List<AuditEntry>) result.getEntity();
    assertEquals(3, resultEntries.size());
    assertEquals(fullHistory, resultEntries);
    EasyMock.verify(auditManager);

    EasyMock.reset(configManager, auditManager);
    EasyMock.expect(auditManager.fetchAuditHistory(configKey, configKey, -1))
            .andThrow(new IllegalArgumentException("count must be positive"));
    EasyMock.replay(configManager, auditManager);

    testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager,
        DEFAULT_CONFIG
    );
    result = testedResource.getExecutionConfigHistory(null, -1);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
    Map<String, Object> errorEntity = (Map<String, Object>) result.getEntity();
    assertEquals("count must be positive", errorEntity.get("error"));
    EasyMock.verify(auditManager);
  }

  private void expectAuditInfoRequest()
  {
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
  }
}
