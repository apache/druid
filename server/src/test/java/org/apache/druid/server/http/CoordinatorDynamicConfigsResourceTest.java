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
import org.apache.druid.audit.AuditManager;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.ServerCloneStatus;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

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
  public void testGetBrokerStatus()
  {
    EasyMock.expect(coordinatorDynamicConfigSyncer.getInSyncBrokers())
            .andReturn(
                new ConfigSyncStatus(
                    ImmutableSet.of(
                        new BrokerSyncStatus("host1", 8080, 1000
                        )
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
        new ServerCloneStatus("hist3", ServerCloneStatus.State.LOADING, 2, 0, 1000),
        ServerCloneStatus.unknown("hist4")
    );

    EasyMock.expect(cloneStatusManager.getStatusForAllServers()).andReturn(statusMetrics).once();
    EasyMock.expect(cloneStatusManager.getStatusForServer("hist2")).andReturn(ServerCloneStatus.unknown("hist4")).once();
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
    Assert.assertEquals(statusMetrics, response.getEntity());

    response = resource.getCloneStatus("hist2");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(Map.of("hist2", ServerCloneStatus.unknown("hist4")), response.getEntity());
  }
}
