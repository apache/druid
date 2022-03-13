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

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;

public class ServersResourceTest
{
  private DruidServer server;
  private DataSegment segment;
  private ServersResource serversResource;
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(new InjectableValues()
    {
      @Override
      public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance)
      {
        if ("org.apache.druid.timeline.DataSegment$PruneSpecsHolder".equals(valueId)) {
          return DataSegment.PruneSpecsHolder.DEFAULT;
        } else {
          return null;
        }
      }
    });

    DruidServer dummyServer = new DruidServer("dummy", "host", null, 1234L, ServerType.HISTORICAL, "tier", 0);
    segment = DataSegment.builder()
                         .dataSource("dataSource")
                         .interval(Intervals.of("2016-03-22T14Z/2016-03-22T15Z"))
                         .version("v0")
                         .size(1L)
                         .build();
    dummyServer.addDataSegment(segment);

    CoordinatorServerView inventoryView = EasyMock.createMock(CoordinatorServerView.class);
    EasyMock.expect(inventoryView.getInventory()).andReturn(ImmutableList.of(dummyServer)).anyTimes();
    EasyMock.expect(inventoryView.getInventoryValue(dummyServer.getName())).andReturn(dummyServer).anyTimes();
    EasyMock.replay(inventoryView);
    server = dummyServer;
    serversResource = new ServersResource(inventoryView);
  }

  @Test
  public void testGetClusterServersFull() throws Exception
  {
    Response res = serversResource.getClusterServers("full", null);
    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = "[{\"host\":\"host\","
                      + "\"maxSize\":1234,"
                      + "\"type\":\"historical\","
                      + "\"tier\":\"tier\","
                      + "\"priority\":0,"
                      + "\"segments\":{\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\":"
                      + "{\"dataSource\":\"dataSource\",\"interval\":\"2016-03-22T14:00:00.000Z/2016-03-22T15:00:00.000Z\",\"version\":\"v0\",\"loadSpec\":{},\"dimensions\":\"\",\"metrics\":\"\","
                      + "\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":0,\"partitions\":1},\"binaryVersion\":null,\"size\":1,\"identifier\":\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\"}},"
                      + "\"currSize\":1}]";
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetClusterServersSimple() throws Exception
  {
    Response res = serversResource.getClusterServers(null, "simple");
    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = "[{\"host\":\"host\",\"tier\":\"tier\",\"type\":\"historical\",\"priority\":0,\"currSize\":1,\"maxSize\":1234}]";
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetServerFull() throws Exception
  {
    Response res = serversResource.getServer(server.getName(), null);
    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = "{\"host\":\"host\","
                      + "\"maxSize\":1234,"
                      + "\"type\":\"historical\","
                      + "\"tier\":\"tier\","
                      + "\"priority\":0,"
                      + "\"segments\":{\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\":"
                      + "{\"dataSource\":\"dataSource\",\"interval\":\"2016-03-22T14:00:00.000Z/2016-03-22T15:00:00.000Z\",\"version\":\"v0\",\"loadSpec\":{},\"dimensions\":\"\",\"metrics\":\"\","
                      + "\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":0,\"partitions\":1},\"binaryVersion\":null,\"size\":1,\"identifier\":\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\"}},"
                      + "\"currSize\":1}";
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetServerSimple() throws Exception
  {
    Response res = serversResource.getServer(server.getName(), "simple");
    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = "{\"host\":\"host\",\"tier\":\"tier\",\"type\":\"historical\",\"priority\":0,\"currSize\":1,\"maxSize\":1234}";
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testDruidServerSerde() throws Exception
  {
    DruidServer server = new DruidServer("dummy", "dummyHost", null, 1234, ServerType.HISTORICAL, "dummyTier", 1);
    String serverJson = objectMapper.writeValueAsString(server);
    String expected = "{\"name\":\"dummy\",\"host\":\"dummyHost\",\"hostAndTlsPort\":null,\"maxSize\":1234,\"type\":\"historical\",\"tier\":\"dummyTier\",\"priority\":1}";
    Assert.assertEquals(expected, serverJson);
    DruidServer deserializedServer = objectMapper.readValue(serverJson, DruidServer.class);
    Assert.assertEquals(server, deserializedServer);
  }

  @Test
  public void testDruidServerMetadataSerde() throws Exception
  {
    DruidServerMetadata metadata = new DruidServerMetadata(
        "dummy",
        "host",
        null,
        1234,
        ServerType.HISTORICAL,
        "tier",
        1
    );
    String metadataJson = objectMapper.writeValueAsString(metadata);
    String expected = "{\"name\":\"dummy\",\"host\":\"host\",\"hostAndTlsPort\":null,\"maxSize\":1234,\"type\":\"historical\",\"tier\":\"tier\",\"priority\":1}";
    Assert.assertEquals(expected, metadataJson);
    DruidServerMetadata deserializedMetadata = objectMapper.readValue(metadataJson, DruidServerMetadata.class);
    Assert.assertEquals(metadata, deserializedMetadata);

    metadata = new DruidServerMetadata(
        "host:123",
        "host:123",
        null,
        0,
        ServerType.HISTORICAL,
        "t1",
        0
    );

    Assert.assertEquals(metadata, objectMapper.readValue(
        "{\"name\":\"host:123\",\"maxSize\":0,\"type\":\"HISTORICAL\",\"tier\":\"t1\",\"priority\":0,\"host\":\"host:123\"}",
        DruidServerMetadata.class
    ));

    metadata = new DruidServerMetadata(
        "host:123",
        "host:123",
        "host:214",
        0,
        ServerType.HISTORICAL,
        "t1",
        0
    );
    Assert.assertEquals(metadata, objectMapper.readValue(
        "{\"name\":\"host:123\",\"maxSize\":0,\"type\":\"HISTORICAL\",\"tier\":\"t1\",\"priority\":0,\"host\":\"host:123\",\"hostAndTlsPort\":\"host:214\"}",
        DruidServerMetadata.class
    ));
    Assert.assertEquals(metadata, objectMapper.readValue(
        objectMapper.writeValueAsString(metadata),
        DruidServerMetadata.class
    ));
  }

  @Test
  public void testGetServerNotFound()
  {
    CoordinatorServerView inventoryView = EasyMock.createMock(CoordinatorServerView.class);
    EasyMock.expect(inventoryView.getInventoryValue("not_exist_server")).andReturn(null);
    EasyMock.replay(inventoryView);
    ServersResource serversResource = new ServersResource(inventoryView);

    Response res = serversResource.getServer("not_exist_server", "simple");

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), res.getStatus());
    Assert.assertEquals("server [not_exist_server] does not exist.", ((Map) res.getEntity()).get("error"));
  }

  @Test
  public void testGetServerSegmentsNotFound()
  {
    CoordinatorServerView inventoryView = EasyMock.createMock(CoordinatorServerView.class);
    EasyMock.expect(inventoryView.getInventoryValue("not_exist_server")).andReturn(null);
    EasyMock.replay(inventoryView);
    ServersResource serversResource = new ServersResource(inventoryView);

    Response res = serversResource.getServerSegments("not_exist_server", "full");

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), res.getStatus());
    Assert.assertEquals("server [not_exist_server] does not exist.", ((Map) res.getEntity()).get("error"));
  }

  @Test
  public void testGetFullServerSegments() throws Exception
  {
    Response res = serversResource.getServerSegments(server.getName(), "full");

    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    String result = objectMapper.writeValueAsString(res.getEntity());
    DataSegment[] actual = objectMapper.readValue(result, DataSegment[].class);
    Assert.assertEquals(server.getTotalSegments(), actual.length);
    Assert.assertEquals(server.iterateAllSegments().iterator().next(), actual[0]);
  }

  @Test
  public void testGetSimpleServerSegments() throws Exception
  {
    Response res = serversResource.getServerSegments(server.getName(), null);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = objectMapper.writeValueAsString(Collections.singletonList(segment.getId()));
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetServerSegment_ServerNotFound()
  {
    CoordinatorServerView inventoryView = EasyMock.createMock(CoordinatorServerView.class);
    EasyMock.expect(inventoryView.getInventoryValue("not_exist_server")).andReturn(null);
    EasyMock.replay(inventoryView);
    ServersResource serversResource = new ServersResource(inventoryView);

    Response res = serversResource.getServerSegment("not_exist_server", "segmentId");

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), res.getStatus());
    Assert.assertEquals("server [not_exist_server] does not exist.", ((Map) res.getEntity()).get("error"));
  }

  @Test
  public void testGetServerSegment() throws Exception
  {
    Response res = serversResource.getServerSegment(server.getName(), segment.getId().toString());

    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());


    String result = objectMapper.writeValueAsString(res.getEntity());
    String expected = objectMapper.writeValueAsString(segment);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetServerSegment_SegmentNotFound()
  {
    Response res = serversResource.getServerSegment(server.getName(), "not-exist-datasource_1000_3000");

    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), res.getStatus());
    Assert.assertEquals("segment [not-exist-datasource_1000_3000] not found in server [dummy].", ((Map) res.getEntity()).get("error"));
  }
}
