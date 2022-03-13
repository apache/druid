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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2LongMaps;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.LoadQueuePeonTester;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;

public class CoordinatorResourceTest
{
  private DruidCoordinator mock;

  @Before
  public void setUp()
  {
    mock = EasyMock.createStrictMock(DruidCoordinator.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mock);
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(mock.getCurrentLeader()).andReturn("boz").once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorResource(mock).getLeader();
    Assert.assertEquals("boz", response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(mock.isLeader()).andReturn(true).once();
    EasyMock.expect(mock.isLeader()).andReturn(false).once();
    EasyMock.replay(mock);

    // true
    final Response response1 = new CoordinatorResource(mock).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", true), response1.getEntity());
    Assert.assertEquals(200, response1.getStatus());

    // false
    final Response response2 = new CoordinatorResource(mock).isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", false), response2.getEntity());
    Assert.assertEquals(404, response2.getStatus());
  }

  @Test
  public void testGetLoadStatusSimple()
  {
    EasyMock.expect(mock.computeNumsUnavailableUsedSegmentsPerDataSource()).andReturn(Object2IntMaps.singleton("k", 1));
    EasyMock.replay(mock);

    final Response response1 = new CoordinatorResource(mock).getLoadStatus("simple", null, null);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());
    Assert.assertEquals(Object2IntMaps.singleton("k", 1), response1.getEntity());
  }

  @Test
  public void testGetLoadStatusFull()
  {
    EasyMock.expect(mock.computeUnderReplicationCountsPerDataSourcePerTier())
            .andReturn(ImmutableMap.of("datasource", Object2LongMaps.singleton("k", 1L)));
    EasyMock.replay(mock);

    final Response response1 = new CoordinatorResource(mock).getLoadStatus(null, "full", null);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());
    Assert.assertEquals(ImmutableMap.of("datasource", Object2LongMaps.singleton("k", 1L)), response1.getEntity());
  }

  @Test
  public void testGetLoadStatus()
  {
    EasyMock.expect(mock.getLoadStatus()).andReturn(ImmutableMap.of("a", 1.0));
    EasyMock.replay(mock);

    final Response response1 = new CoordinatorResource(mock).getLoadStatus(null, null, null);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());
    Assert.assertEquals(ImmutableMap.of("a", 1.0), response1.getEntity());
  }

  @Test
  public void testGetLoadQueueFull() throws Exception
  {
    EasyMock.expect(mock.getLoadManagementPeons()).andReturn(ImmutableMap.of("a", new LoadQueuePeonTester()));
    EasyMock.replay(mock);

    final Response response1 = new CoordinatorResource(mock).getLoadQueue(null, "full");

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of("a", new LoadQueuePeonTester())),
        mapper.writeValueAsString(response1.getEntity())
    );
  }

  @Test
  public void testGetLoadQueue() throws Exception
  {
    EasyMock.expect(mock.getLoadManagementPeons()).andReturn(ImmutableMap.of("a", new LoadQueuePeonTester()));
    EasyMock.replay(mock);

    final Response response1 = new CoordinatorResource(mock).getLoadQueue(null, null);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of("a", ImmutableMap.of("segmentsToLoad", new ArrayList(),
                                                                       "segmentsToDrop", new ArrayList()
        ))),
        mapper.writeValueAsString(response1.getEntity())
    );
  }
}
