/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import com.google.common.collect.ImmutableList;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.java.util.common.Intervals;
import io.druid.server.coordination.ServerType;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import io.druid.server.security.AuthenticationResult;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IntervalsResourceTest
{
  private InventoryView inventoryView;
  private DruidServer server;
  private List<DataSegment> dataSegmentList;
  private HttpServletRequest request;

  @Before
  public void setUp()
  {
    inventoryView = EasyMock.createStrictMock(InventoryView.class);
    server = EasyMock.createStrictMock(DruidServer.class);
    request = EasyMock.createStrictMock(HttpServletRequest.class);

    dataSegmentList = new ArrayList<>();
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            Intervals.of("2010-01-01T00:00:00.000Z/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            20
        )
    );
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            Intervals.of("2010-01-22T00:00:00.000Z/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            10
        )
    );
    dataSegmentList.add(
        new DataSegment(
            "datasource2",
            Intervals.of("2010-01-01T00:00:00.000Z/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            5
        )
    );
    server = new DruidServer("who", "host", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
  }

  @Test
  public void testGetIntervals()
  {
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(inventoryView, request);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(Intervals.of("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    expectedIntervals.add(Intervals.of("2010-01-22T00:00:00.000Z/2010-01-23T00:00:00.000Z"));
    IntervalsResource intervalsResource = new IntervalsResource(
        inventoryView,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );

    Response response = intervalsResource.getIntervals(request);
    TreeMap<Interval, Map<String, Map<String, Object>>> actualIntervals = (TreeMap) response.getEntity();
    Assert.assertEquals(2, actualIntervals.size());
    Assert.assertEquals(expectedIntervals.get(1), actualIntervals.firstKey());
    Assert.assertEquals(10L, actualIntervals.get(expectedIntervals.get(1)).get("datasource1").get("size"));
    Assert.assertEquals(1, actualIntervals.get(expectedIntervals.get(1)).get("datasource1").get("count"));
    Assert.assertEquals(expectedIntervals.get(0), actualIntervals.lastKey());
    Assert.assertEquals(20L, actualIntervals.get(expectedIntervals.get(0)).get("datasource1").get("size"));
    Assert.assertEquals(1, actualIntervals.get(expectedIntervals.get(0)).get("datasource1").get("count"));
    Assert.assertEquals(5L, actualIntervals.get(expectedIntervals.get(0)).get("datasource2").get("size"));
    Assert.assertEquals(1, actualIntervals.get(expectedIntervals.get(0)).get("datasource2").get("count"));

  }

  @Test
  public void testSimpleGetSpecificIntervals()
  {
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(inventoryView, request);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(Intervals.of("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    IntervalsResource intervalsResource = new IntervalsResource(
        inventoryView,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );

    Response response = intervalsResource.getSpecificIntervals("2010-01-01T00:00:00.000Z/P1D", "simple", null, request);
    Map<Interval, Map<String, Object>> actualIntervals = (Map) response.getEntity();
    Assert.assertEquals(1, actualIntervals.size());
    Assert.assertTrue(actualIntervals.containsKey(expectedIntervals.get(0)));
    Assert.assertEquals(25L, actualIntervals.get(expectedIntervals.get(0)).get("size"));
    Assert.assertEquals(2, actualIntervals.get(expectedIntervals.get(0)).get("count"));

  }

  @Test
  public void testFullGetSpecificIntervals()
  {
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(inventoryView, request);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(Intervals.of("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    IntervalsResource intervalsResource = new IntervalsResource(
        inventoryView,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );

    Response response = intervalsResource.getSpecificIntervals("2010-01-01T00:00:00.000Z/P1D", null, "full", request);
    TreeMap<Interval, Map<String, Map<String, Object>>> actualIntervals = (TreeMap) response.getEntity();
    Assert.assertEquals(1, actualIntervals.size());
    Assert.assertEquals(expectedIntervals.get(0), actualIntervals.firstKey());
    Assert.assertEquals(20L, actualIntervals.get(expectedIntervals.get(0)).get("datasource1").get("size"));
    Assert.assertEquals(1, actualIntervals.get(expectedIntervals.get(0)).get("datasource1").get("count"));
    Assert.assertEquals(5L, actualIntervals.get(expectedIntervals.get(0)).get("datasource2").get("size"));
    Assert.assertEquals(1, actualIntervals.get(expectedIntervals.get(0)).get("datasource2").get("count"));

  }

  @Test
  public void testGetSpecificIntervals()
  {
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(inventoryView, request);

    IntervalsResource intervalsResource = new IntervalsResource(
        inventoryView,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );

    Response response = intervalsResource.getSpecificIntervals("2010-01-01T00:00:00.000Z/P1D", null, null, request);
    Map<String, Object> actualIntervals = (Map) response.getEntity();
    Assert.assertEquals(2, actualIntervals.size());
    Assert.assertEquals(25L, actualIntervals.get("size"));
    Assert.assertEquals(2, actualIntervals.get("count"));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(inventoryView);
  }

}
