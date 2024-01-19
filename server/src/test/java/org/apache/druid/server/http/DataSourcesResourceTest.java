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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.SegmentLoadInfo;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.UnknownSegmentIdsException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCoordinator;
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
  private SegmentsMetadataManager segmentsMetadataManager;
  private AuditManager auditManager;

  @Before
  public void setUp()
  {
    request = EasyMock.createStrictMock(HttpServletRequest.class);
    inventoryView = EasyMock.createStrictMock(CoordinatorServerView.class);
    server = EasyMock.niceMock(DruidServer.class);
    auditManager = EasyMock.niceMock(AuditManager.class);
    dataSegmentList = new ArrayList<>();
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            Intervals.of("2010-01-01/P1D"),
            "v0",
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
            "v0",
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
            "v0",
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
    segmentsMetadataManager = EasyMock.createMock(SegmentsMetadataManager.class);
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
    DataSourcesResource dataSourcesResource = createResource();
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

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, null, authMapper, null, auditManager);
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
    DataSourcesResource dataSourcesResource = createResource();
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
    DataSourcesResource dataSourcesResource = createResource();
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
    DataSourcesResource dataSourcesResource = createResource();
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
    DataSourcesResource dataSourcesResource = createResource();
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
    DataSourcesResource dataSourcesResource = createResource();
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

    DataSourcesResource dataSourcesResource = createResource();
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
    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.getIntervalsWithServedSegmentsOrAllServedSegmentsPerIntervals(
        "invalidDataSource",
        null,
        null
    );
    Assert.assertNull(response.getEntity());

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

    DataSourcesResource dataSourcesResource = createResource();
    Response response = dataSourcesResource.getServedSegmentsInInterval(
        "invalidDataSource",
        "2010-01-01/P1D",
        null,
        null
    );
    Assert.assertNull(response.getEntity());

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
    String interval = "2010-01-01/P1D";
    Interval theInterval = Intervals.of(interval.replace('_', '/'));

    OverlordClient overlordClient = EasyMock.createStrictMock(OverlordClient.class);
    EasyMock.expect(overlordClient.runKillTask("api-issued", "datasource1", theInterval, null, null))
            .andReturn(Futures.immediateFuture("kill_task_1"));
    EasyMock.replay(overlordClient, server);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, overlordClient, null, null, auditManager);
    prepareRequestForAudit();
    Response response = dataSourcesResource.killUnusedSegmentsInInterval("datasource1", interval, request);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertNull(response.getEntity());
    EasyMock.verify(overlordClient, server);
  }

  @Test
  public void testMarkAsUnusedAllSegmentsInDataSourceBadRequest()
  {
    OverlordClient overlordClient = EasyMock.createStrictMock(OverlordClient.class);
    EasyMock.replay(overlordClient, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, null, overlordClient, null, null, auditManager);
    DruidExceptionMatcher.invalidInput().assertThrowsAndMatches(
        () -> dataSourcesResource.markAsUnusedAllSegmentsOrKillUnusedSegmentsInInterval("datasource", "true", "???", request)
    );
  }

  @Test
  public void testMarkAsUnusedAllSegmentsInDataSource()
  {
    prepareRequestForAudit();

    OverlordClient overlordClient = EasyMock.createStrictMock(OverlordClient.class);
    EasyMock.replay(overlordClient, server);
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, segmentsMetadataManager, null, overlordClient, null, null, auditManager);
    Response response = dataSourcesResource
        .markAsUnusedAllSegmentsOrKillUnusedSegmentsInInterval("datasource", null, null, request);
    Assert.assertEquals(200, response.getStatus());

    EasyMock.verify(request);
  }

  @Test
  public void testIsHandOffComplete()
  {
    MetadataRuleManager databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    Rule loadRule = new IntervalLoadRule(Intervals.of("2013-01-02T00:00:00Z/2013-01-03T00:00:00Z"), null, null);
    Rule dropRule = new IntervalDropRule(Intervals.of("2013-01-01T00:00:00Z/2013-01-02T00:00:00Z"));
    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(inventoryView, null, databaseRuleManager, null, null, null, auditManager);

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
    DataSegment segment = dataSegmentList.get(0);
    EasyMock.expect(segmentsMetadataManager.markSegmentAsUsed(segment.getId().toString())).andReturn(true).once();
    EasyMock.replay(segmentsMetadataManager);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markSegmentAsUsed(segment.getDataSource(), segment.getId().toString());
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadataManager);
  }

  @Test
  public void testMarkSegmentAsUsedNoChange()
  {
    DataSegment segment = dataSegmentList.get(0);
    EasyMock.expect(segmentsMetadataManager.markSegmentAsUsed(segment.getId().toString())).andReturn(false).once();
    EasyMock.replay(segmentsMetadataManager);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markSegmentAsUsed(segment.getDataSource(), segment.getId().toString());
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("segmentStateChanged", false), response.getEntity());
    EasyMock.verify(segmentsMetadataManager);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInterval()
  {
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andReturn(3).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    DataSourcesResource dataSourcesResource = createResource();
    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsIntervalNoneUpdated()
  {
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andReturn(0).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsSet() throws UnknownSegmentIdsException
  {
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Set<String> segmentIds = ImmutableSet.of(dataSegmentList.get(1).getId().toString());
    int numUpdatedSegments =
        segmentsMetadataManager.markAsUsedNonOvershadowedSegments(EasyMock.eq("datasource1"), EasyMock.eq(segmentIds));
    EasyMock.expect(numUpdatedSegments).andReturn(3).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, segmentIds)
    );
    Assert.assertEquals(200, response.getStatus());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsIntervalException()
  {
    DruidDataSource dataSource = new DruidDataSource("datasource1", new HashMap<>());
    Interval interval = Intervals.of("2010-01-22/P1D");
    int numUpdatedSegments =
        segmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(EasyMock.eq("datasource1"), EasyMock.eq(interval));
    EasyMock.expect(numUpdatedSegments).andThrow(new RuntimeException("Error!")).once();
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(interval, null)
    );
    Assert.assertEquals(500, response.getStatus());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsNoDataSource()
  {
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(null).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-22/P1D"), null)
    );
    Assert.assertEquals(204, response.getStatus());
    EasyMock.verify(segmentsMetadataManager);
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadNoArguments()
  {
    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, null)
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadBothArguments()
  {
    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-22/P1D"), ImmutableSet.of())
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsInvalidPayloadEmptyArray()
  {
    DataSourcesResource dataSourcesResource = createResource();

    Response response = dataSourcesResource.markAsUsedNonOvershadowedSegments(
        "datasource1",
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, ImmutableSet.of())
    );
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testMarkAsUsedNonOvershadowedSegmentsNoPayload()
  {
    DataSourcesResource dataSourcesResource = createResource();

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
    final Set<SegmentId> segmentIds =
        dataSegmentList.stream()
                       .filter(segment -> segment.getDataSource().equals(dataSource1.getName()))
                       .map(DataSegment::getId)
                       .collect(Collectors.toSet());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markSegmentsAsUnused(segmentIds)).andReturn(1).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(
            null,
            segmentIds.stream()
                      .map(SegmentId::toString)
                      .collect(Collectors.toSet())
        );

    DataSourcesResource dataSourcesResource = createResource();
    prepareRequestForAudit();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 1), response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsAsUnusedNoChanges()
  {
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final Set<SegmentId> segmentIds =
        dataSegmentList.stream()
                       .filter(segment -> segment.getDataSource().equals(dataSource1.getName()))
                       .map(DataSegment::getId)
                       .collect(Collectors.toSet());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markSegmentsAsUnused(segmentIds)).andReturn(0).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(
            null,
            segmentIds.stream()
                      .map(SegmentId::toString)
                      .collect(Collectors.toSet())
        );

    DataSourcesResource dataSourcesResource = createResource();
    prepareRequestForAudit();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsAsUnusedException()
  {
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());
    final Set<SegmentId> segmentIds =
        dataSegmentList.stream()
                       .filter(segment -> segment.getDataSource().equals(dataSource1.getName()))
                       .map(DataSegment::getId)
                       .collect(Collectors.toSet());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markSegmentsAsUnused(segmentIds))
            .andThrow(new RuntimeException("Exception occurred"))
            .once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(
            null,
            segmentIds.stream()
                      .map(SegmentId::toString)
                      .collect(Collectors.toSet())
        );

    DataSourcesResource dataSourcesResource =
        createResource();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInInterval()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markAsUnusedSegmentsInInterval("datasource1", theInterval)).andReturn(1).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource = createResource();
    prepareRequestForAudit();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 1), response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalNoChanges()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markAsUnusedSegmentsInInterval("datasource1", theInterval)).andReturn(0).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource = createResource();
    prepareRequestForAudit();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("numChangedSegments", 0), response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkAsUnusedSegmentsInIntervalException()
  {
    final Interval theInterval = Intervals.of("2010-01-01/P1D");
    final DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap<>());

    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(server)).once();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(dataSource1).once();
    EasyMock.expect(segmentsMetadataManager.markAsUnusedSegmentsInInterval("datasource1", theInterval))
            .andThrow(new RuntimeException("Exception occurred"))
            .once();
    EasyMock.replay(segmentsMetadataManager, inventoryView, server);

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(theInterval, null);

    DataSourcesResource dataSourcesResource =
        createResource();
    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(500, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    EasyMock.verify(segmentsMetadataManager, inventoryView, server);
  }

  @Test
  public void testMarkSegmentsAsUnusedNullPayload()
  {
    DataSourcesResource dataSourcesResource =
        createResource();

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", null, request);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(
        "Invalid request payload, either interval or segmentIds array must be specified",
        response.getEntity()
    );
  }

  @Test
  public void testMarkSegmentsAsUnusedInvalidPayload()
  {
    DataSourcesResource dataSourcesResource =
        createResource();

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(null, null);

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testMarkSegmentsAsUnusedInvalidPayloadBothArguments()
  {
    DataSourcesResource dataSourcesResource =
        createResource();

    final DataSourcesResource.MarkDataSourceSegmentsPayload payload =
        new DataSourcesResource.MarkDataSourceSegmentsPayload(Intervals.of("2010-01-01/P1D"), ImmutableSet.of());

    Response response = dataSourcesResource.markSegmentsAsUnused("datasource1", payload, request);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertNotNull(response.getEntity());
  }

  @Test
  public void testGetDatasourceLoadstatusForceMetadataRefreshNull()
  {
    DataSourcesResource dataSourcesResource = createResource();
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", null, null, null, null, null);
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testGetDatasourceLoadstatusNoSegmentForInterval()
  {
    List<DataSegment> segments = ImmutableList.of();
    // Test when datasource fully loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq(
        "datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    EasyMock.replay(segmentsMetadataManager);

    DataSourcesResource dataSourcesResource = new DataSourcesResource(
        inventoryView,
        segmentsMetadataManager,
        null,
        null,
        null,
        null,
        null
    );
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, null, null, null);
    Assert.assertEquals(204, response.getStatus());
  }

  @Test
  public void testGetDatasourceLoadstatusDefault()
  {
    DataSegment datasource1Segment1 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        10
    );

    DataSegment datasource1Segment2 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-22/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        20
    );
    DataSegment datasource2Segment1 = new DataSegment(
        "datasource2",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        30
    );
    List<DataSegment> segments = ImmutableList.of(datasource1Segment1, datasource1Segment2);
    Map<SegmentId, SegmentLoadInfo> completedLoadInfoMap = ImmutableMap.of(
        datasource1Segment1.getId(), new SegmentLoadInfo(datasource1Segment1),
        datasource1Segment2.getId(), new SegmentLoadInfo(datasource1Segment2),
        datasource2Segment1.getId(), new SegmentLoadInfo(datasource2Segment1)
    );
    Map<SegmentId, SegmentLoadInfo> halfLoadedInfoMap = ImmutableMap.of(
        datasource1Segment1.getId(), new SegmentLoadInfo(datasource1Segment1)
    );

    // Test when datasource fully loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    EasyMock.expect(inventoryView.getLoadInfoForAllSegments()).andReturn(completedLoadInfoMap).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView);

    DataSourcesResource dataSourcesResource = createResource();
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, null, null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(1, ((Map) response.getEntity()).size());
    Assert.assertTrue(((Map) response.getEntity()).containsKey("datasource1"));
    Assert.assertEquals(100.0, ((Map) response.getEntity()).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager, inventoryView);
    EasyMock.reset(segmentsMetadataManager, inventoryView);

    // Test when datasource half loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    EasyMock.expect(inventoryView.getLoadInfoForAllSegments()).andReturn(halfLoadedInfoMap).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView);

    dataSourcesResource = createResource();
    response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, null, null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(1, ((Map) response.getEntity()).size());
    Assert.assertTrue(((Map) response.getEntity()).containsKey("datasource1"));
    Assert.assertEquals(50.0, ((Map) response.getEntity()).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager, inventoryView);
  }

  @Test
  public void testGetDatasourceLoadstatusSimple()
  {
    DataSegment datasource1Segment1 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        10
    );

    DataSegment datasource1Segment2 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-22/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        20
    );
    DataSegment datasource2Segment1 = new DataSegment(
        "datasource2",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        30
    );
    List<DataSegment> segments = ImmutableList.of(datasource1Segment1, datasource1Segment2);
    Map<SegmentId, SegmentLoadInfo> completedLoadInfoMap = ImmutableMap.of(
        datasource1Segment1.getId(), new SegmentLoadInfo(datasource1Segment1),
        datasource1Segment2.getId(), new SegmentLoadInfo(datasource1Segment2),
        datasource2Segment1.getId(), new SegmentLoadInfo(datasource2Segment1)
    );
    Map<SegmentId, SegmentLoadInfo> halfLoadedInfoMap = ImmutableMap.of(
        datasource1Segment1.getId(), new SegmentLoadInfo(datasource1Segment1)
    );

    // Test when datasource fully loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    EasyMock.expect(inventoryView.getLoadInfoForAllSegments()).andReturn(completedLoadInfoMap).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView);

    DataSourcesResource dataSourcesResource = createResource();
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, "simple", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(1, ((Map) response.getEntity()).size());
    Assert.assertTrue(((Map) response.getEntity()).containsKey("datasource1"));
    Assert.assertEquals(0, ((Map) response.getEntity()).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager, inventoryView);
    EasyMock.reset(segmentsMetadataManager, inventoryView);

    // Test when datasource half loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    EasyMock.expect(inventoryView.getLoadInfoForAllSegments()).andReturn(halfLoadedInfoMap).once();
    EasyMock.replay(segmentsMetadataManager, inventoryView);

    dataSourcesResource = createResource();
    response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, "simple", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(1, ((Map) response.getEntity()).size());
    Assert.assertTrue(((Map) response.getEntity()).containsKey("datasource1"));
    Assert.assertEquals(1, ((Map) response.getEntity()).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager, inventoryView);
  }

  @Test
  public void testGetDatasourceLoadstatusFull()
  {
    DataSegment datasource1Segment1 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        10
    );

    DataSegment datasource1Segment2 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-22/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        20
    );
    List<DataSegment> segments = ImmutableList.of(datasource1Segment1, datasource1Segment2);

    final Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier = new HashMap<>();
    Object2LongMap<String> tier1 = new Object2LongOpenHashMap<>();
    tier1.put("datasource1", 0L);
    Object2LongMap<String> tier2 = new Object2LongOpenHashMap<>();
    tier2.put("datasource1", 3L);
    underReplicationCountsPerDataSourcePerTier.put("tier1", tier1);
    underReplicationCountsPerDataSourcePerTier.put("tier2", tier2);

    // Test when datasource fully loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    DruidCoordinator druidCoordinator = EasyMock.createMock(DruidCoordinator.class);
    EasyMock.expect(druidCoordinator.getTierToDatasourceToUnderReplicatedCount(segments, false))
            .andReturn(underReplicationCountsPerDataSourcePerTier).once();

    EasyMock.replay(segmentsMetadataManager, druidCoordinator);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(null, segmentsMetadataManager, null, null, null, druidCoordinator, auditManager);
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, null, "full", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(2, ((Map) response.getEntity()).size());
    Assert.assertEquals(1, ((Map) ((Map) response.getEntity()).get("tier1")).size());
    Assert.assertEquals(1, ((Map) ((Map) response.getEntity()).get("tier2")).size());
    Assert.assertEquals(0L, ((Map) ((Map) response.getEntity()).get("tier1")).get("datasource1"));
    Assert.assertEquals(3L, ((Map) ((Map) response.getEntity()).get("tier2")).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager);
  }

  @Test
  public void testGetDatasourceLoadstatusFullAndComputeUsingClusterView()
  {
    DataSegment datasource1Segment1 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-01/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        10
    );

    DataSegment datasource1Segment2 = new DataSegment(
        "datasource1",
        Intervals.of("2010-01-22/P1D"),
        "",
        null,
        null,
        null,
        null,
        0x9,
        20
    );
    List<DataSegment> segments = ImmutableList.of(datasource1Segment1, datasource1Segment2);

    final Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier = new HashMap<>();
    Object2LongMap<String> tier1 = new Object2LongOpenHashMap<>();
    tier1.put("datasource1", 0L);
    Object2LongMap<String> tier2 = new Object2LongOpenHashMap<>();
    tier2.put("datasource1", 3L);
    underReplicationCountsPerDataSourcePerTier.put("tier1", tier1);
    underReplicationCountsPerDataSourcePerTier.put("tier2", tier2);

    // Test when datasource fully loaded
    EasyMock.expect(segmentsMetadataManager.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(EasyMock.eq("datasource1"), EasyMock.anyObject(Interval.class), EasyMock.anyBoolean()))
            .andReturn(Optional.of(segments)).once();
    DruidCoordinator druidCoordinator = EasyMock.createMock(DruidCoordinator.class);
    EasyMock.expect(druidCoordinator.getTierToDatasourceToUnderReplicatedCount(segments, true))
            .andReturn(underReplicationCountsPerDataSourcePerTier).once();

    EasyMock.replay(segmentsMetadataManager, druidCoordinator);

    DataSourcesResource dataSourcesResource =
        new DataSourcesResource(null, segmentsMetadataManager, null, null, null, druidCoordinator, auditManager);
    Response response = dataSourcesResource.getDatasourceLoadstatus("datasource1", true, null, null, "full", "computeUsingClusterView");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertNotNull(response.getEntity());
    Assert.assertEquals(2, ((Map) response.getEntity()).size());
    Assert.assertEquals(1, ((Map) ((Map) response.getEntity()).get("tier1")).size());
    Assert.assertEquals(1, ((Map) ((Map) response.getEntity()).get("tier2")).size());
    Assert.assertEquals(0L, ((Map) ((Map) response.getEntity()).get("tier1")).get("datasource1"));
    Assert.assertEquals(3L, ((Map) ((Map) response.getEntity()).get("tier2")).get("datasource1"));
    EasyMock.verify(segmentsMetadataManager);
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

  private void prepareRequestForAudit()
  {
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").anyTimes();
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").anyTimes();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(null).anyTimes();
    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();

    EasyMock.expect(request.getMethod()).andReturn("POST").anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn("/request/uri").anyTimes();
    EasyMock.expect(request.getQueryString()).andReturn("query=string").anyTimes();

    EasyMock.replay(request);
  }

  private DataSourcesResource createResource()
  {
    return new DataSourcesResource(
        inventoryView,
        segmentsMetadataManager,
        null,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        auditManager
    );
  }
}
