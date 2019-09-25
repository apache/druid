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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.SegmentLoadInfo;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataSegmentManager;
import org.apache.druid.metadata.UnknownSegmentIdException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.test.utils.ImmutableDruidDataSourceTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DataSourcesResourceTest
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
            "",
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
            "",
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
            "",
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
      new AuthenticationResult("druid", "druid", null, null)
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
        new AuthenticationResult("druid", "druid", null, null)
    ).once();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(inventoryView, server, request);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    Response response = dataSourcesResource.getQueryableDataSources("full", null, request);
    Set<ImmutableDruidDataSource> result = (Set<ImmutableDruidDataSource>) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, result.size());
    ImmutableDruidDataSourceTestUtils.assertEquals(
        listDataSources.stream().map(DruidDataSource::toImmutableDruidDataSource).collect(Collectors.toList()),
        new ArrayList<>(result)
    );

    response = dataSourcesResource.getQueryableDataSources(null, null, request);
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
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);
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

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();

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

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
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

    DataSourcesResource dataSourcesResource = new DataSourcesResource(inventoryView, null, null, null, authMapper);
    Response response = dataSourcesResource.getQueryableDataSources("full", null, request);
    Set<ImmutableDruidDataSource> result = (Set<ImmutableDruidDataSource>) response.getEntity();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, result.size());
    ImmutableDruidDataSourceTestUtils.assertEquals(
        listDataSources.get(0).toImmutableDruidDataSource(),
        Iterables.getOnlyElement(result)
    );

    response = dataSourcesResource.getQueryableDataSources(null, null, request);
    List<String> result1 = (List<String>) response.getEntity();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, result1.size());
    Assert.assertTrue(result1.contains("datasource1"));

    EasyMock.verify(inventoryView, server, request);
  }

  @Test
  public void testGetSimpleQueryableDataSources()
  {
    EasyMock.expect(server.getDataSources()).andReturn(listDataSources).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(listDataSources.get(0)).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource2")).andReturn(listDataSources.get(1)).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(inventoryView, server, request);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    Response response = dataSourcesResource.getQueryableDataSources(null, "simple", request);
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
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, null);
    Response response = dataSourcesResource.getDataSource("datasource1", "full");
    ImmutableDruidDataSource result = (ImmutableDruidDataSource) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    ImmutableDruidDataSourceTestUtils.assertEquals(dataSource1.toImmutableDruidDataSource(), result);
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testNullGetTheDataSource()
  {
    EasyMock.expect(server.getDataSource("none")).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, null);
    Assert.assertEquals(204, dataSourcesResource.getDataSource("none", null).getStatus());
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSource()
  {
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    dataSource1.addSegment(
        new DataSegment("datasegment1", Intervals.of("2010-01-01/P1D"), "", null, null, null, null, 0x9, 10)
    );
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, null);
    Response response = dataSourcesResource.getDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(1, ((Map) (result.get("tiers").get(null))).get("segmentCount"));
    Assert.assertEquals(10L, ((Map) (result.get("tiers").get(null))).get("size"));
    Assert.assertEquals(10L, ((Map) (result.get("tiers").get(null))).get("replicatedSize"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-02T00:00:00.000Z", result.get("segments").get("maxTime").toString());
    Assert.assertEquals(1, result.get("segments").get("count"));
    Assert.assertEquals(10L, result.get("segments").get("size"));
    Assert.assertEquals(10L, result.get("segments").get("replicatedSize"));
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSourceManyTiers()
  {
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(listDataSources.get(0)).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn("cold").atLeastOnce();

    DruidServer server2 = EasyMock.createStrictMock(DruidServer.class);
    EasyMock.expect(server2.getDataSource("datasource1")).andReturn(listDataSources.get(1)).atLeastOnce();
    EasyMock.expect(server2.getTier()).andReturn("hot").atLeastOnce();

    DruidServer server3 = EasyMock.createStrictMock(DruidServer.class);
    EasyMock.expect(server3.getDataSource("datasource1")).andReturn(listDataSources.get(1)).atLeastOnce();
    EasyMock.expect(server3.getTier()).andReturn("cold").atLeastOnce();

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server, server2, server3)).atLeastOnce();

    EasyMock.replay(inventoryView, server, server2, server3);
    DataSourcesResource dataSourcesResource = new DataSourcesResource(inventoryView, null, null, null, null);
    Response response = dataSourcesResource.getDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(2, ((Map) (result.get("tiers").get("cold"))).get("segmentCount"));
    Assert.assertEquals(30L, ((Map) (result.get("tiers").get("cold"))).get("size"));
    Assert.assertEquals(30L, ((Map) (result.get("tiers").get("cold"))).get("replicatedSize"));
    Assert.assertEquals(1, ((Map) (result.get("tiers").get("hot"))).get("segmentCount"));
    Assert.assertEquals(20L, ((Map) (result.get("tiers").get("hot"))).get("size"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-23T00:00:00.000Z", result.get("segments").get("maxTime").toString());
    Assert.assertEquals(2, result.get("segments").get("count"));
    Assert.assertEquals(30L, result.get("segments").get("size"));
    Assert.assertEquals(50L, result.get("segments").get("replicatedSize"));
    EasyMock.verify(inventoryView, server, server2, server3);
  }

  @Test
  public void testSimpleGetTheDataSourceWithReplicatedSegments()
  {
    server = new DruidServer("server1", "host1", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    DruidServer server2 = new DruidServer("server2", "host2", null, 1234, ServerType.HISTORICAL, "tier2", 0);
    DruidServer server3 = new DruidServer("server3", "host3", null, 1234, ServerType.HISTORICAL, "tier1", 0);

    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
    server2.addDataSegment(dataSegmentList.get(0));
    server2.addDataSegment(dataSegmentList.get(1));
    server3.addDataSegment(dataSegmentList.get(2));

    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server, server2, server3)
    ).atLeastOnce();

    EasyMock.replay(inventoryView);

    DataSourcesResource dataSourcesResource = new DataSourcesResource(inventoryView, null, null, null, null);
    Response response = dataSourcesResource.getDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result1 = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(2, ((Map) (result1.get("tiers").get("tier1"))).get("segmentCount"));
    Assert.assertEquals(30L, ((Map) (result1.get("tiers").get("tier1"))).get("size"));
    Assert.assertEquals(30L, ((Map) (result1.get("tiers").get("tier1"))).get("replicatedSize"));
    Assert.assertEquals(2, ((Map) (result1.get("tiers").get("tier2"))).get("segmentCount"));
    Assert.assertEquals(30L, ((Map) (result1.get("tiers").get("tier2"))).get("size"));
    Assert.assertNotNull(result1.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result1.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-23T00:00:00.000Z", result1.get("segments").get("maxTime").toString());
    Assert.assertEquals(2, result1.get("segments").get("count"));
    Assert.assertEquals(30L, result1.get("segments").get("size"));
    Assert.assertEquals(60L, result1.get("segments").get("replicatedSize"));

    response = dataSourcesResource.getDataSource("datasource2", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result2 = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(1, ((Map) (result2.get("tiers").get("tier1"))).get("segmentCount"));
    Assert.assertEquals(30L, ((Map) (result2.get("tiers").get("tier1"))).get("size"));
    Assert.assertEquals(60L, ((Map) (result2.get("tiers").get("tier1"))).get("replicatedSize"));
    Assert.assertNotNull(result2.get("segments"));
    Assert.assertEquals("2010-01-01T00:00:00.000Z", result2.get("segments").get("minTime").toString());
    Assert.assertEquals("2010-01-02T00:00:00.000Z", result2.get("segments").get("maxTime").toString());
    Assert.assertEquals(1, result2.get("segments").get("count"));
    Assert.assertEquals(30L, result2.get("segments").get("size"));
    Assert.assertEquals(60L, result2.get("segments").get("replicatedSize"));
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testGetSegmentDataSourceIntervals()
  {
    server = new DruidServer("who", "host", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();
    EasyMock.replay(inventoryView);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(Intervals.of("2010-01-22T00:00:00.000Z/2010-01-23T00:00:00.000Z"));
    expectedIntervals.add(Intervals.of("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, null);

    Response response = dataSourcesResource.getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
        "invalidDataSource",
        null,
        null
    );
    Assert.assertEquals(response.getEntity(), null);

    response = dataSourcesResource.getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
        "datasource1",
        null,
        null
    );
    TreeSet<Interval> actualIntervals = (TreeSet) response.getEntity();
    Assert.assertEquals(2, actualIntervals.size());
    Assert.assertEquals(expectedIntervals.get(0), actualIntervals.first());
    Assert.assertEquals(expectedIntervals.get(1), actualIntervals.last());

    response = dataSourcesResource.getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
        "datasource1",
        "simple",
        null
    );
    TreeMap<Interval, Map<DataSourcesResource.SimpleProperties, Object>> results = (TreeMap) response.getEntity();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedIntervals.get(0), results.firstKey());
    Assert.assertEquals(expectedIntervals.get(1), results.lastKey());
    Assert.assertEquals(1, results.firstEntry().getValue().get(DataSourcesResource.SimpleProperties.count));
    Assert.assertEquals(1, results.lastEntry().getValue().get(DataSourcesResource.SimpleProperties.count));

    response = dataSourcesResource.getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
        "datasource1",
        null,
        "full"
    );
    Map<Interval, Map<SegmentId, Object>> results2 = ((Map<Interval, Map<SegmentId, Object>>) response.getEntity());
    int i = 1;
    for (Map.Entry<Interval, Map<SegmentId, Object>> entry : results2.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getId())).get("metadata")
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testGetServedSegmentsInIntervalInDataSource()
  {
    server = new DruidServer("who", "host", null, 1234, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).atLeastOnce();
    EasyMock.replay(inventoryView);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, null);
    Response response = dataSourcesResource.getServedSegmentsInInterval(
        "invalidDataSource",
        "2010-01-01/P1D",
        null,
        null
    );
    Assert.assertEquals(null, response.getEntity());

    response = dataSourcesResource.getServedSegmentsInInterval(
        "datasource1",
        "2010-03-01/P1D",
        null,
        null
    ); // interval not present in the datasource
    Assert.assertEquals(ImmutableSet.of(), response.getEntity());

    response = dataSourcesResource.getServedSegmentsInInterval("datasource1", "2010-01-01/P1D", null, null);
    Assert.assertEquals(ImmutableSet.of(dataSegmentList.get(0).getId()), response.getEntity());

    response = dataSourcesResource.getServedSegmentsInInterval("datasource1", "2010-01-01/P1M", null, null);
    Assert.assertEquals(
        ImmutableSet.of(dataSegmentList.get(1).getId(), dataSegmentList.get(0).getId()),
        response.getEntity()
    );

    response = dataSourcesResource.getServedSegmentsInInterval(
        "datasource1",
        "2010-01-01/P1M",
        "simple",
        null
    );
    Map<Interval, Map<DataSourcesResource.SimpleProperties, Object>> results =
        ((Map<Interval, Map<DataSourcesResource.SimpleProperties, Object>>) response.getEntity());
    Assert.assertEquals(2, results.size());
    int i;
    for (i = 0; i < 2; i++) {
      Assert.assertTrue(results.containsKey(dataSegmentList.get(i).getInterval()));
      Assert.assertEquals(
          1,
          (results.get(dataSegmentList.get(i).getInterval())).get(DataSourcesResource.SimpleProperties.count)
      );
    }

    response = dataSourcesResource.getServedSegmentsInInterval("datasource1", "2010-01-01/P1M", null, "full");
    Map<Interval, Map<SegmentId, Object>> results1 = ((Map<Interval, Map<SegmentId, Object>>) response.getEntity());
    i = 1;
    for (Map.Entry<Interval, Map<SegmentId, Object>> entry : results1.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getId())).get("metadata")
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testKillSegmentsInIntervalInDataSource()
  {
    String interval = "2010-01-01_P1D";
    Interval theInterval = Intervals.of(interval.replace('_', '/'));

    IndexingServiceClient indexingServiceClient = EasyMock.createStrictMock(IndexingServiceClient.class);
    indexingServiceClient.killSegments("datasource1", theInterval);
    EasyMock.expectLastCall().once();
    EasyMock.replay(indexingServiceClient, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, indexingServiceClient, null);
    Response response = dataSourcesResource.killSegmentsInInterval("datasource1", interval);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(null, response.getEntity());
    EasyMock.verify(indexingServiceClient, server);
  }

  @Test
  public void testMarkAsUnusedAllSegmentsInDataSource()
  {
    IndexingServiceClient indexingServiceClient = EasyMock.createStrictMock(IndexingServiceClient.class);
    EasyMock.replay(indexingServiceClient, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, indexingServiceClient, null);
    try {
      Response response =
          dataSourcesResource.markAsUnusedAllSegmentsOrKillSegmentsInInterval("datasource", "true", "???");
      // 400 (Bad Request) or an IllegalArgumentException is expected.
      Assert.assertEquals(400, response.getStatus());
      Assert.assertNotNull(response.getEntity());
      Assert.assertTrue(response.getEntity().toString().contains("java.lang.IllegalArgumentException"));
    }
    catch (IllegalArgumentException ignore) {
      // expected
    }

    EasyMock.verify(indexingServiceClient, server);
  }

  @Test
  public void testIsHandOffComplete()
  {
    MetadataRuleManager databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    Rule loadRule = new IntervalLoadRule(Intervals.of("2013-01-02T00:00:00Z/2013-01-03T00:00:00Z"), null);
    Rule dropRule = new IntervalDropRule(Intervals.of("2013-01-01T00:00:00Z/2013-01-02T00:00:00Z"));
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, databaseRuleManager, null, null);

    // test dropped
    EasyMock.expect(databaseRuleManager.getRulesWithDefault("dataSource1"))
            .andReturn(ImmutableList.of(loadRule, dropRule))
            .once();
    EasyMock.replay(databaseRuleManager);

    String interval1 = "2013-01-01T01:00:00Z/2013-01-01T02:00:00Z";
    Response response1 = dataSourcesResource.isHandOffComplete("dataSource1", interval1, 1, "v1");
    Assert.assertTrue((boolean) response1.getEntity());

    EasyMock.verify(databaseRuleManager);

    // test isn't dropped and no timeline found
    EasyMock.reset(databaseRuleManager);
    EasyMock.expect(databaseRuleManager.getRulesWithDefault("dataSource1"))
            .andReturn(ImmutableList.of(loadRule, dropRule))
            .once();
    EasyMock.expect(inventoryView.getTimeline(new TableDataSource("dataSource1")))
            .andReturn(null)
            .once();
    EasyMock.replay(inventoryView, databaseRuleManager);

    String interval2 = "2013-01-02T01:00:00Z/2013-01-02T02:00:00Z";
    Response response2 = dataSourcesResource.isHandOffComplete("dataSource1", interval2, 1, "v1");
    Assert.assertFalse((boolean) response2.getEntity());

    EasyMock.verify(inventoryView, databaseRuleManager);

    // test isn't dropped and timeline exist
    String interval3 = "2013-01-02T02:00:00Z/2013-01-02T03:00:00Z";
    SegmentLoadInfo segmentLoadInfo = new SegmentLoadInfo(createSegment(Intervals.of(interval3), "v1", 1));
    segmentLoadInfo.addServer(createHistoricalServerMetadata("test"));
    VersionedIntervalTimeline<String, SegmentLoadInfo> timeline =
        new VersionedIntervalTimeline<String, SegmentLoadInfo>(null)
    {
      @Override
      public List<TimelineObjectHolder<String, SegmentLoadInfo>> lookupWithIncompletePartitions(Interval interval)
      {
        PartitionHolder<SegmentLoadInfo> partitionHolder =
            new PartitionHolder<>(new NumberedPartitionChunk<>(1, 1, segmentLoadInfo));
        List<TimelineObjectHolder<String, SegmentLoadInfo>> ret = new ArrayList<>();
        ret.add(new TimelineObjectHolder<>(Intervals.of(interval3), "v1", partitionHolder));
        return ret;
      }
    };
    EasyMock.reset(inventoryView, databaseRuleManager);
    EasyMock.expect(databaseRuleManager.getRulesWithDefault("dataSource1"))
            .andReturn(ImmutableList.of(loadRule, dropRule))
            .once();
    EasyMock.expect(inventoryView.getTimeline(new TableDataSource("dataSource1")))
            .andReturn(timeline)
            .once();
    EasyMock.replay(inventoryView, databaseRuleManager);

    Response response3 = dataSourcesResource.isHandOffComplete("dataSource1", interval3, 1, "v1");
    Assert.assertTrue((boolean) response3.getEntity());

    EasyMock.verify(inventoryView, databaseRuleManager);
  }

  @Test
  public void testMarkSegmentAsUsed()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DataSegment segment = dataSegmentList.get(0);
    EasyMock.expect(segmentsMetadata.markSegmentAsUsed(segment.getId().toString())).andReturn(true).once();
    EasyMock.replay(segmentsMetadata);

    DataSourcesResource dataSourcesResource = new DataSourcesResource(null, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markSegmentAsUsed(segment.getDataSource(), segment.getId().toString());
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadata);
  }

  @Test
  public void testMarkSegmentAsUsedNoChange()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DataSegment segment = dataSegmentList.get(0);
    EasyMock.expect(segmentsMetadata.markSegmentAsUsed(segment.getId().toString())).andReturn(false).once();
    EasyMock.replay(segmentsMetadata);

    DataSourcesResource dataSourcesResource = new DataSourcesResource(null, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markSegmentAsUsed(segment.getDataSource(), segment.getId().toString());
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("segmentStateChanged", false), response.getEntity());
    EasyMock.verify(segmentsMetadata);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInterval()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andReturn(3).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsIntervalNoneUpdated()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andReturn(0).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsSet() throws UnknownSegmentIdException
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Set<String> segmentIds = ImmutableSet.of(dataSegmentList.get(1).getId().toString());
    int numUpdatedSegments =
        segmentsMetadata.markAsUsedNonOvershadowedSegments(EasyMock.eq("datasource1"), EasyMock.eq(segmentIds));
    EasyMock.expect(numUpdatedSegments).andReturn(3).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, segmentIds)
    );
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsIntervalException()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadata.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andThrow(new RuntimeException("Error!")).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(500, response.getStatus());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsNoDataSource()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(null).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-22/P1D"), null)
    );
    Assert.assertEquals(204, response.getStatus());
    EasyMock.verify(segmentsMetadata);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadNoArguments()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, null)
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadBothArguments()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-22/P1D"), ImmutableSet.of())
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadEmptyArray()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, ImmutableSet.of())
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsNoPayload()
  {
    MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments("datasource1", null);
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testSegmentLoadChecksForVersion()
  {
    Interval interval = Intervals.of("2011-04-01/2011-04-02");
    Assert.assertFalse(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v2", 2)
        )
    );

    Assert.assertTrue(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v2", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

    Assert.assertTrue(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

  }

  @Test
  public void testSegmentLoadChecksForAssignableServer()
  {
    Interval interval = Intervals.of("2011-04-01/2011-04-02");
    Assert.assertTrue(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

    Assert.assertFalse(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createRealtimeServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );
  }

  @Test
  public void testSegmentLoadChecksForPartitionNumber()
  {
    Interval interval = Intervals.of("2011-04-01/2011-04-02");
    Assert.assertTrue(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 1)
        )
    );

    Assert.assertFalse(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

  }

  @Test
  public void testSegmentLoadChecksForInterval()
  {

    Assert.assertFalse(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(Intervals.of("2011-04-01/2011-04-02"), "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(Intervals.of("2011-04-01/2011-04-03"), "v1", 1)
        )
    );

    Assert.assertTrue(
        DataSourcesResource.isSegmentLoaded(
            Collections.singletonList(
                new ImmutableSegmentLoadInfo(
                    createSegment(Intervals.of("2011-04-01/2011-04-04"), "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(Intervals.of("2011-04-02/2011-04-03"), "v1", 1)
        )
    );
  }

  @Test
  public void testMarkSegmentsAsUnused()
  {
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final Set<String> segmentIds =
        dataSegmentList.stream().map(ds -> ds.getId().toString()).collect(Collectors.toSet());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markSegmentsAsUnused("datasource1", segmentIds)).andReturn(1).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, segmentIds);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 1), response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsAsUnusedNoChanges()
  {
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final Set<String> segmentIds =
        dataSegmentList.stream().map(ds -> ds.getId().toString()).collect(Collectors.toSet());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markSegmentsAsUnused("datasource1", segmentIds)).andReturn(0).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, segmentIds);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsAsUnusedException()
  {
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final Set<String> segmentIds =
        dataSegmentList.stream().map(ds -> ds.getId().toString()).collect(Collectors.toSet());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markSegmentsAsUnused("datasource1", segmentIds))
            .andThrow(new RuntimeException("Exception occurred"))
            .once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, segmentIds);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInInterval()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markAsUnusedSegmentsInInterval("datasource1", theInterval)).andReturn(1).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 1), response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalNoChanges()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markAsUnusedSegmentsInInterval("datasource1", theInterval)).andReturn(0).once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalException()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadata.markAsUnusedSegmentsInInterval("datasource1", theInterval))
            .andThrow(new RuntimeException("Exception occurred"))
            .once();
    EasyMock.replay(segmentsMetadata, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    EasyMock.verify(segmentsMetadata, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsUnusedNullPayload()
  {
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(
        "Invalid request payload, either interval or segmentIds array must be specified",
        response.getEntity()
    );
  }

  @Test
  public void testMarkSegmentsUnusedInvalidPayload()
  {
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, null);

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testMarkSegmentsUnusedInvalidPayloadBothArguments()
  {
    final MetadataSegmentManager segmentsMetadata = EasyMock.createMock(MetadataSegmentManager.class);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadata, null, null, null);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-01/P1D"), ImmutableSet.of());

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
  }

  private DruidServerMetadata createRealtimeServerMetadata(String name)
  {
    return createServerMetadata(name, ServerType.REALTIME);
  }

  private DruidServerMetadata createHistoricalServerMetadata(String name)
  {
    return createServerMetadata(name, ServerType.HISTORICAL);
  }

  private DruidServerMetadata createServerMetadata(String name, ServerType type)
  {
    return new DruidServerMetadata(name, name, null, 10000, type, "tier", 1);
  }

  private DataSegment createSegment(Interval interval, String version, int partitionNumber)
  {
    return new DataSegment(
        "test_ds",
        interval,
        version,
        null,
        null,
        null,
        new NumberedShardSpec(partitionNumber, 100),
        0, 0
    );
  }
}
