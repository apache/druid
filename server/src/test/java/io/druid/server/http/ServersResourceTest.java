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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.client.CoordinatorServerView;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

public class ServersResourceTest
{
  private DruidServer server;
  private ServersResource serversResource;
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    DruidServer dummyServer = new DruidServer("dummy", "host", null, 1234L, ServerType.HISTORICAL, "tier", 0);
    DataSegment segment = DataSegment.builder()
                                     .dataSource("dataSource")
                                     .interval(new Interval("2016-03-22T14Z/2016-03-22T15Z"))
                                     .version("v0")
                                     .size(1L)
                                     .build();
    dummyServer.addDataSegment(segment.getIdentifier(), segment);

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
                      + "\"shardSpec\":{\"type\":\"none\"},\"binaryVersion\":null,\"size\":1,\"identifier\":\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\"}},"
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
                      + "\"shardSpec\":{\"type\":\"none\"},\"binaryVersion\":null,\"size\":1,\"identifier\":\"dataSource_2016-03-22T14:00:00.000Z_2016-03-22T15:00:00.000Z_v0\"}},"
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
}
