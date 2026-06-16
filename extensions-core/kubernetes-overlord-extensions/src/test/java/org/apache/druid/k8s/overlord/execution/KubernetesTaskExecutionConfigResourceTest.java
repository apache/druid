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

import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigEtag;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.server.security.AuthConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.function.UnaryOperator;

public class KubernetesTaskExecutionConfigResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private HttpServletRequest req;
  private KubernetesTaskRunnerDynamicConfig dynamicConfig;

  @BeforeEach
  public void setUp()
  {
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    auditManager = EasyMock.createMock(AuditManager.class);
    req = EasyMock.createMock(HttpServletRequest.class);
    dynamicConfig = EasyMock.createMock(KubernetesTaskRunnerDynamicConfig.class);
  }

  @Test
  public void getExecutionConfigDerivesBodyAndEtagFromSameBytes()
  {
    final byte[] currentBytes = "current-k8s-execution-config".getBytes(StandardCharsets.UTF_8);
    final KubernetesTaskRunnerDynamicConfig expectedConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(new TaskTypePodTemplateSelectStrategy(), 10);
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );

    EasyMock.expect(configManager.getCurrentBytes(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY))
            .andReturn(currentBytes)
            .once();
    EasyMock.expect(
        configManager.convertByteToConfig(
            EasyMock.aryEq(currentBytes),
            EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
            (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull()
        )
    ).andReturn(expectedConfig).once();
    EasyMock.replay(configManager, auditManager);

    final Response result = testedResource.getExecutionConfig();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assertions.assertEquals(expectedConfig, result.getEntity());
    Assertions.assertEquals(ConfigEtag.compute(currentBytes), result.getMetadata().getFirst(HttpHeaders.ETAG));
  }

  @Test
  public void setExecutionConfigSuccessfulUpdate()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader("If-Match")).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.setIfMatch(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        (String) EasyMock.isNull(),
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
        (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull(),
        EasyMock.anyObject(UnaryOperator.class),
        EasyMock.anyObject(AuditInfo.class)
    )).andReturn(ConfigManager.SetResult.ok());
    EasyMock.replay(configManager, auditManager, dynamicConfig);

    final Response result = testedResource.setExecutionConfig(dynamicConfig, req);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfigFailedUpdate()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader("If-Match")).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);
    EasyMock.expect(configManager.setIfMatch(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        (String) EasyMock.isNull(),
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
        (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull(),
        EasyMock.anyObject(UnaryOperator.class),
        EasyMock.anyObject(AuditInfo.class)
    )).andReturn(ConfigManager.SetResult.failure(new RuntimeException()));
    EasyMock.replay(configManager, auditManager, dynamicConfig);

    final Response result = testedResource.setExecutionConfig(dynamicConfig, req);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
  }

  @Test
  public void setExecutionConfigWithBlankIfMatchReturnsBadRequest()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );
    EasyMock.expect(req.getHeader(HttpHeaders.IF_MATCH)).andReturn("  ").once();
    EasyMock.replay(req, configManager, auditManager, dynamicConfig);

    final Response result = testedResource.setExecutionConfig(dynamicConfig, req);

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), result.getStatus());
    Assertions.assertTrue(result.getEntity() instanceof ErrorResponse);
    Assertions.assertEquals(
        "If-Match header must not be blank",
        ((ErrorResponse) result.getEntity()).getUnderlyingException().getMessage()
    );
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentCapacityWhenRequestCapacityNull()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );

    final PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    final KubernetesTaskRunnerDynamicConfig currentConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 5);

    final PodTemplateSelectStrategy requestStrategy = new TaskTypePodTemplateSelectStrategy();
    final KubernetesTaskRunnerDynamicConfig requestConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, null);

    final KubernetesTaskRunnerDynamicConfig expectedMergedConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(requestStrategy, 5);
    final Capture<UnaryOperator<KubernetesTaskRunnerDynamicConfig>> updateCapture = EasyMock.newCapture();

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader("If-Match")).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.setIfMatch(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        (String) EasyMock.isNull(),
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
        (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull(),
        EasyMock.capture(updateCapture),
        EasyMock.anyObject(AuditInfo.class)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    final Response result = testedResource.setExecutionConfig(requestConfig, req);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assertions.assertEquals(expectedMergedConfig, updateCapture.getValue().apply(currentConfig));
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentStrategyWhenRequestStrategyNull()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );

    final PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    final KubernetesTaskRunnerDynamicConfig currentConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 2);

    final KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, 7);

    final KubernetesTaskRunnerDynamicConfig expectedMergedConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 7);
    final Capture<UnaryOperator<KubernetesTaskRunnerDynamicConfig>> updateCapture = EasyMock.newCapture();

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader("If-Match")).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.setIfMatch(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        (String) EasyMock.isNull(),
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
        (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull(),
        EasyMock.capture(updateCapture),
        EasyMock.anyObject(AuditInfo.class)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    final Response result = testedResource.setExecutionConfig(requestConfig, req);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assertions.assertEquals(expectedMergedConfig, updateCapture.getValue().apply(currentConfig));
  }

  @Test
  public void setExecutionConfig_MergeUsesCurrentWhenBothRequestFieldsNull()
  {
    final KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
    );

    final PodTemplateSelectStrategy currentStrategy = new TaskTypePodTemplateSelectStrategy();
    final KubernetesTaskRunnerDynamicConfig currentConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);

    final KubernetesTaskRunnerDynamicConfig requestConfig = new DefaultKubernetesTaskRunnerDynamicConfig(null, null);

    final KubernetesTaskRunnerDynamicConfig expectedMergedConfig =
        new DefaultKubernetesTaskRunnerDynamicConfig(currentStrategy, 9);
    final Capture<UnaryOperator<KubernetesTaskRunnerDynamicConfig>> updateCapture = EasyMock.newCapture();

    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getHeader("If-Match")).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    EasyMock.expect(configManager.setIfMatch(
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY),
        (String) EasyMock.isNull(),
        EasyMock.eq(KubernetesTaskRunnerDynamicConfig.class),
        (KubernetesTaskRunnerDynamicConfig) EasyMock.isNull(),
        EasyMock.capture(updateCapture),
        EasyMock.anyObject(AuditInfo.class)
    )).andReturn(ConfigManager.SetResult.ok());

    EasyMock.replay(configManager, auditManager);

    final Response result = testedResource.setExecutionConfig(requestConfig, req);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
    Assertions.assertEquals(expectedMergedConfig, updateCapture.getValue().apply(currentConfig));
  }
}
