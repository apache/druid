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
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KubernetesTaskExecutionConfigResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private HttpServletRequest req;
  private KubernetesTaskRunnerDynamicConfig dynamicConfig;

  @Before
  public void setUp()
  {
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    auditManager = EasyMock.createMock(AuditManager.class);
    req = EasyMock.createMock(HttpServletRequest.class);
    dynamicConfig = EasyMock.createMock(KubernetesTaskRunnerDynamicConfig.class);
  }

  @Test
  public void setExecutionConfigSuccessfulUpdate()
  {
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
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
    KubernetesTaskExecutionConfigResource testedResource = new KubernetesTaskExecutionConfigResource(
        configManager,
        auditManager
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
}
