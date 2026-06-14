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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigEtag;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.ServerCloneStatus;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.UnaryOperator;

public class CoordinatorDynamicConfigsResourceTest
{
  private CoordinatorConfigManager manager;
  private AuditManager auditManager;
  private CoordinatorDynamicConfigSyncer coordinatorDynamicConfigSyncer;
  private CloneStatusManager cloneStatusManager;

  @Before
  public void setUp() throws Exception
  {
    manager = EasyMock.createStrictMock(CoordinatorConfigManager.class);
    auditManager = EasyMock.createStrictMock(AuditManager.class);
    coordinatorDynamicConfigSyncer = EasyMock.createStrictMock(CoordinatorDynamicConfigSyncer.class);
    cloneStatusManager = EasyMock.createStrictMock(CloneStatusManager.class);
  }

  @Test
  public void testGetDynamicConfigsDerivesBodyAndEtagFromSameBytes()
  {
    final byte[] currentBytes = "current-dynamic-config".getBytes(StandardCharsets.UTF_8);
    final CoordinatorDynamicConfig expectedConfig = CoordinatorDynamicConfig.builder()
                                                                            .withPauseCoordination(true)
                                                                            .build();

    EasyMock.expect(manager.getCurrentDynamicConfigBytes()).andReturn(currentBytes).once();
    EasyMock.expect(manager.convertBytesToDynamicConfig(EasyMock.aryEq(currentBytes)))
            .andReturn(expectedConfig)
            .once();
    EasyMock.replay(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager);

    final Response response = new CoordinatorDynamicConfigsResource(
        manager,
        auditManager,
        coordinatorDynamicConfigSyncer,
        cloneStatusManager
    ).getDynamicConfigs();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedConfig, response.getEntity());
    Assert.assertEquals(ConfigEtag.compute(currentBytes), response.getMetadata().getFirst(HttpHeaders.ETAG));

    EasyMock.verify(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager);
  }

  @Test
  public void testGetBrokerStatus()
  {
    EasyMock.expect(coordinatorDynamicConfigSyncer.getInSyncBrokers())
            .andReturn(
                  ImmutableSet.of(
                      new BrokerSyncStatus("host1", 8080, 1000
                      )
                  )
            )
            .once();
    EasyMock.replay(coordinatorDynamicConfigSyncer);
    EasyMock.replay(cloneStatusManager);

    final Response response = new CoordinatorDynamicConfigsResource(
        manager,
        auditManager,
        coordinatorDynamicConfigSyncer,
        cloneStatusManager
    ).getBrokerStatus();

    Assert.assertEquals(200, response.getStatus());

    ConfigSyncStatus expected = new ConfigSyncStatus(
        ImmutableSet.of(
            new BrokerSyncStatus("host1", 8080, 1000)
        )
    );
    Assert.assertEquals(expected, response.getEntity());
  }

  @Test
  public void testGetCloneStatus()
  {
    List<ServerCloneStatus> statusMetrics = ImmutableList.of(
        new ServerCloneStatus("hist3", "hist1", ServerCloneStatus.State.IN_PROGRESS, 2, 0, 1000),
        ServerCloneStatus.unknown("hist4", "hist3")
    );

    EasyMock.expect(cloneStatusManager.getStatusForAllServers()).andReturn(statusMetrics).once();
    EasyMock.expect(cloneStatusManager.getStatusForServer("hist2")).andReturn(ServerCloneStatus.unknown("hist4", "hist3")).once();
    EasyMock.replay(coordinatorDynamicConfigSyncer);
    EasyMock.replay(cloneStatusManager);

    CoordinatorDynamicConfigsResource resource = new CoordinatorDynamicConfigsResource(
        manager,
        auditManager,
        coordinatorDynamicConfigSyncer,
        cloneStatusManager
    );
    Response response = resource.getCloneStatus(null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CloneStatus(statusMetrics), response.getEntity());

    response = resource.getCloneStatus("hist2");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ServerCloneStatus.unknown("hist4", "hist3"), response.getEntity());
  }

  @Test
  public void testSetDynamicConfigsUsesTransformUpdate()
  {
    final String etag = "\"etag\"";
    final HttpServletRequest request = EasyMock.createNiceMock(HttpServletRequest.class);
    final CoordinatorDynamicConfig.Builder updateBuilder = CoordinatorDynamicConfig.builder()
                                                                                  .withPauseCoordination(true);
    final CoordinatorDynamicConfig currentConfig = CoordinatorDynamicConfig.builder()
                                                                           .withMaxSegmentsToMove(5)
                                                                           .build();
    final Capture<UnaryOperator<CoordinatorDynamicConfig>> updateCapture = EasyMock.newCapture();

    EasyMock.expect(request.getHeader(HttpHeaders.IF_MATCH)).andReturn(etag).once();
    EasyMock.expect(
        manager.updateDynamicConfig(
            EasyMock.capture(updateCapture),
            EasyMock.eq(etag),
            EasyMock.anyObject(AuditInfo.class)
        )
    ).andReturn(SetResult.ok()).once();
    coordinatorDynamicConfigSyncer.queueBroadcastConfigToBrokers();
    EasyMock.expectLastCall().once();
    EasyMock.replay(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager, request);

    final Response response = new CoordinatorDynamicConfigsResource(
        manager,
        auditManager,
        coordinatorDynamicConfigSyncer,
        cloneStatusManager
    ).setDynamicConfigs(updateBuilder, request);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(updateBuilder.build(currentConfig), updateCapture.getValue().apply(currentConfig));

    EasyMock.verify(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager, request);
  }

  @Test
  public void testSetDynamicConfigsWithBlankIfMatchReturnsBadRequest()
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);

    EasyMock.expect(request.getHeader(HttpHeaders.IF_MATCH)).andReturn("  ").once();
    EasyMock.replay(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager, request);

    final CoordinatorDynamicConfigsResource resource = new CoordinatorDynamicConfigsResource(
        manager,
        auditManager,
        coordinatorDynamicConfigSyncer,
        cloneStatusManager
    );

    final Response response = resource.setDynamicConfigs(CoordinatorDynamicConfig.builder(), request);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ErrorResponse);
    Assert.assertEquals(
        "If-Match header must not be blank",
        ((ErrorResponse) response.getEntity()).getUnderlyingException().getMessage()
    );

    EasyMock.verify(manager, auditManager, coordinatorDynamicConfigSyncer, cloneStatusManager, request);
  }
}
