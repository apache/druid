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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.SegmentReplicationStatusManager;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

public class CoordinatorResourceTest
{
  private SegmentReplicationStatusManager segmentReplicationStatusManager;
  private DruidCoordinator coordinator;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createStrictMock(DruidCoordinator.class);
    segmentReplicationStatusManager = EasyMock.createStrictMock(SegmentReplicationStatusManager.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(segmentReplicationStatusManager);
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(coordinator.getCurrentLeader()).andReturn("boz").once();
    EasyMock.replay(coordinator, segmentReplicationStatusManager);

    final Response response = new CoordinatorResource(coordinator, segmentReplicationStatusManager).getLeader();
    Assert.assertEquals("boz", response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(coordinator.isLeader()).andReturn(true).once();
    EasyMock.expect(coordinator.isLeader()).andReturn(false).once();
    EasyMock.replay(coordinator, segmentReplicationStatusManager);

    // true
    final Response response1 = new CoordinatorResource(coordinator, segmentReplicationStatusManager).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", true), response1.getEntity());
    Assert.assertEquals(200, response1.getStatus());

    // false
    final Response response2 = new CoordinatorResource(coordinator, segmentReplicationStatusManager).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", false), response2.getEntity());
    Assert.assertEquals(404, response2.getStatus());
  }
}
