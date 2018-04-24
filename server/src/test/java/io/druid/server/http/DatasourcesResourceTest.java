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
import com.google.common.collect.ImmutableSet;
import io.druid.client.CoordinatorServerView;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.java.util.common.Intervals;
import io.druid.server.coordination.ServerType;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Resource;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DatasourcesResourceTest
{
  private CoordinatorServerView inventoryView;
  private DruidServer server;
  private List<DruidDataSource> listDataSources;
  private List<DataSegment> dataSegmentList;
  private HttpServletRequest request;

  @Before
  public void setUp()
  {
    request = EasyMock.createStrictMock(HttpServletRequest.class);
    inventoryView = EasyMock.createStrictMock(CoordinatorServerView.class);
    server = EasyMock.niceMock(DruidServer.class);
    dataSegmentList = new ArrayList<>();
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            Intervals.of("2010-01-01/P1D"),
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
            "datasource1",
            Intervals.of("2010-01-22/P1D"),
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
            "datasource2",
            Intervals.of("2010-01-01/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            30
        )
    );
    listDataSources = new ArrayList<>();
    listDataSources.add(
        new DruidDataSource("datasource1", new HashMap<>()).addSegment(dataSegmentList.get(0))
    );
    listDataSources.add(
        new DruidDataSource("datasource2", new HashMap<>()).addSegment(dataSegmentList.get(1))
    );
  }

  @Test
  public void testGetFullQueryableDataSources()
  {
    // first request
    EasyMock.expect(server.getDataSources()).andReturn(
        ImmutableList.of(listDataSources.get(0), listDataSources.get(1))
    ).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
      new AuthenticationResult("druid", "druid", null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    // second request
    EasyMock.expect(server.getDataSources()).andReturn(
        ImmutableList.of(listDataSources.get(0), listDataSources.get(1))
    ).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(inventoryView, server, request);
    DatasourcesResource datasourcesResource = new DatasourcesResource(
        inventoryView,
        null,
        null,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
    Response response = datasourcesResource.getQueryableDataSources("full", null, request);
    Set<ImmutableDruidDataSource> result = (Set<ImmutableDruidDataSource>) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(
        listDataSources.stream().map(DruidDataSource::toImmutableDruidDataSource).collect(Collectors.toSet()),
        new HashSet<>(result)
    );

    response = datasourcesResource.getQueryableDataSources(null, null, request);
    List<String> result1 = (List<String>) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, result1.size());
    Assert.assertTrue(result1.contains("datasource1"));
    Assert.assertTrue(result1.contains("datasource2"));
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSecuredGetFullQueryableDataSources()
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null);
    // first request
    EasyMock.expect(server.getDataSources()).andReturn(
      ImmutableList.of(listDataSources.get(0), listDataSources.get(1))
  ).once();

    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        authenticationResult
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).once();

    // second request
    EasyMock.expect(server.getDataSources()).andReturn(
        ImmutableList.of(listDataSources.get(0), listDataSources.get(1))
    ).once();

    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        authenticationResult
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).once();
    EasyMock.replay(inventoryView, server, request);

    AuthorizerMapper authMapper = new AuthorizerMapper(null) {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult1, Resource resource, Action action)
          {
            if (resource.getName().equals("datasource1")) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }

        };
      }
    };

    DatasourcesResource datasourcesResource = new DatasourcesResource(
        inventoryView,
        null,
        null,
        new AuthConfig(),
        authMapper
    );
    Response response = datasourcesResource.getQueryableDataSources("full", null, request);
    Set<ImmutableDruidDataSource> result = (Set<ImmutableDruidDataSource>) response.getEntity();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(
        listDataSources.subList(0, 1).stream()
                       .map(DruidDataSource::toImmutableDruidDataSource)
                       .collect(Collectors.toSet()),
        new HashSet<>(result)
    );

    response = datasourcesResource.getQueryableDataSources(null, null, request);
    List<String> result1 = (List<String>) response.getEntity();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, result1.size());
    Assert.assertTrue(result1.contains("datasource1"));

    EasyMock.verify(inventoryView, server, request);
  }

  @Test
  public void testGetSimpleQueryableDataSources()
  {
    EasyMock.expect(server.getDataSources()).andReturn(
        listDataSources
    ).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        listDataSources.get(0)
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource2")).andReturn(
        listDataSources.get(1)
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(inventoryView, server, request);
    DatasourcesResource datasourcesResource = new DatasourcesResource(
        inventoryView,
        null,
        null,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
    Response response = datasourcesResource.getQueryableDataSources(null, "simple", request);
    Assert.assertEquals(200, response.getStatus());
    List<Map<String, Object>> results = (List<Map<String, Object>>) response.getEntity();
    int index = 0;
    for (Map<String, Object> entry : results) {
      Assert.assertEquals(listDataSources.get(index).getName(), entry.get("name").toString());
      Assert.assertTrue(((Map) ((Map) entry.get("properties")).get("tiers")).containsKey(null));
      Assert.assertNotNull((((Map) entry.get("properties")).get("segments")));
      Assert.assertEquals(1, ((Map) ((Map) entry.get("properties")).get("segments")).get("count"));
      index++;
    }
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testFullGetTheDataSource()
  {
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);
    Response response = datasourcesResource.getTheDataSource("datasource1", "full");
    ImmutableDruidDataSource result = (ImmutableDruidDataSource) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(dataSource1.toImmutableDruidDataSource(), result);
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testNullGetTheDataSource()
  {
    EasyMock.expect(server.getDataSource("none")).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);
    Assert.assertEquals(204, datasourcesResource.getTheDataSource("none", null).getStatus());
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSource()
  {
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    dataSource1.addSegment(
        new DataSegment("datasegment1", Intervals.of("2010-01-01/P1D"), null, null, null, null, null, 0x9, 10)
    );
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);
    Response response = datasourcesResource.getTheDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(1, ((Map) (result.get("tiers").get(null))).get("segmentCount"));
    Assert.assertEquals(10L, ((Map) (result.get("tiers").get(null))).get("size"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-02T00:00:00.000Z", result.get("segments").get("maxTime").toString());
    Assert.assertEquals(1, result.get("segments").get("count"));
    Assert.assertEquals(10L, result.get("segments").get("size"));
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSourceManyTiers()
  {
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        listDataSources.get(0)
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn("cold").atLeastOnce();

    DruidServer server2 = EasyMock.createStrictMock(DruidServer.class);
    EasyMock.expect(server2.getDataSource("datasource1")).andReturn(
        listDataSources.get(1)
    ).atLeastOnce();
    EasyMock.expect(server2.getTier()).andReturn("hot").atLeastOnce();

    DruidServer server3 = EasyMock.createStrictMock(DruidServer.class);
    EasyMock.expect(server3.getDataSource("datasource1")).andReturn(
        listDataSources.get(1)
    ).atLeastOnce();
    EasyMock.expect(server3.getTier()).andReturn("cold").atLeastOnce();

    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server, server2, server3)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server, server2, server3);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);
    Response response = datasourcesResource.getTheDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(2, ((Map) (result.get("tiers").get("cold"))).get("segmentCount"));
    Assert.assertEquals(30L, ((Map) (result.get("tiers").get("cold"))).get("size"));
    Assert.assertEquals(1, ((Map) (result.get("tiers").get("hot"))).get("segmentCount"));
    Assert.assertEquals(20L, ((Map) (result.get("tiers").get("hot"))).get("size"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-23T00:00:00.000Z", result.get("segments").get("maxTime").toString());
    Assert.assertEquals(2, result.get("segments").get("count"));
    Assert.assertEquals(30L, result.get("segments").get("size"));
    EasyMock.verify(inventoryView, server, server2, server3);
  }

  @Test
  public void testGetSegmentDataSourceIntervals()
  {
    server = new DruidServer("who", "host", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(Intervals.of("2010-01-22T00:00:00.000Z/2010-01-23T00:00:00.000Z"));
    expectedIntervals.add(Intervals.of("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);

    Response response = datasourcesResource.getSegmentDataSourceIntervals("invalidDataSource", null, null);
    Assert.assertEquals(response.getEntity(), null);

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", null, null);
    TreeSet<Interval> actualIntervals = (TreeSet) response.getEntity();
    Assert.assertEquals(2, actualIntervals.size());
    Assert.assertEquals(expectedIntervals.get(0), actualIntervals.first());
    Assert.assertEquals(expectedIntervals.get(1), actualIntervals.last());

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", "simple", null);
    TreeMap<Interval, Map<String, Object>> results = (TreeMap) response.getEntity();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedIntervals.get(0), results.firstKey());
    Assert.assertEquals(expectedIntervals.get(1), results.lastKey());
    Assert.assertEquals(1, results.firstEntry().getValue().get("count"));
    Assert.assertEquals(1, results.lastEntry().getValue().get("count"));

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", null, "full");
    results = ((TreeMap<Interval, Map<String, Object>>) response.getEntity());
    int i = 1;
    for (Map.Entry<Interval, Map<String, Object>> entry : results.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getIdentifier())).get(
              "metadata"
          )
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testGetSegmentDataSourceSpecificInterval()
  {
    server = new DruidServer("who", "host", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView);

    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null, new AuthConfig(), null);
    Response response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "invalidDataSource",
        "2010-01-01/P1D",
        null,
        null
    );
    Assert.assertEquals(null, response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "datasource1",
        "2010-03-01/P1D",
        null,
        null
    ); // interval not present in the datasource
    Assert.assertEquals(ImmutableSet.of(), response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1D", null, null);
    Assert.assertEquals(ImmutableSet.of(dataSegmentList.get(0).getIdentifier()), response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1M", null, null);
    Assert.assertEquals(
        ImmutableSet.of(dataSegmentList.get(1).getIdentifier(), dataSegmentList.get(0).getIdentifier()),
        response.getEntity()
    );

    response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "datasource1",
        "2010-01-01/P1M",
        "simple",
        null
    );
    HashMap<Interval, Map<String, Object>> results = ((HashMap<Interval, Map<String, Object>>) response.getEntity());
    Assert.assertEquals(2, results.size());
    int i;
    for (i = 0; i < 2; i++) {
      Assert.assertTrue(results.containsKey(dataSegmentList.get(i).getInterval()));
      Assert.assertEquals(1, (results.get(dataSegmentList.get(i).getInterval())).get("count"));
    }

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1M", null, "full");
    TreeMap<Interval, Map<String, Object>> results1 = ((TreeMap<Interval, Map<String, Object>>) response.getEntity());
    i = 1;
    for (Map.Entry<Interval, Map<String, Object>> entry : results1.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getIdentifier())).get(
              "metadata"
          )
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testDeleteDataSourceSpecificInterval()
  {
    String interval = "2010-01-01_P1D";
    Interval theInterval = Intervals.of(interval.replace("_", "/"));

    IndexingServiceClient indexingServiceClient = EasyMock.createStrictMock(IndexingServiceClient.class);
    indexingServiceClient.killSegments("datasource1", theInterval);
    EasyMock.expectLastCall().once();
    EasyMock.replay(indexingServiceClient, server);

    DatasourcesResource datasourcesResource = new DatasourcesResource(
        inventoryView,
        null,
        indexingServiceClient,
        new AuthConfig(),
        null
    );
    Response response = datasourcesResource.deleteDataSourceSpecificInterval("datasource1", interval);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(null, response.getEntity());
    EasyMock.verify(indexingServiceClient, server);
  }

  @Test
  public void testDeleteDataSource()
  {
    IndexingServiceClient indexingServiceClient = EasyMock.createStrictMock(IndexingServiceClient.class);
    EasyMock.replay(indexingServiceClient, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(
        inventoryView,
        null,
        indexingServiceClient,
        new AuthConfig(),
        null
    );
    Response response = datasourcesResource.deleteDataSource("datasource", "true", "???");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().toString().contains("java.lang.IllegalArgumentException"));

    EasyMock.verify(indexingServiceClient, server);
  }

}
