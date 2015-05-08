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
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatasourcesResourceTest
{

  @Test
  public void testGetFullQueryableDataSources() throws Exception
  {
    InventoryView inventoryView = EasyMock.createStrictMock(InventoryView.class);
    DruidServer server = EasyMock.createStrictMock(DruidServer.class);
    DruidDataSource[] druidDataSources = {
        new DruidDataSource("datasource1", new HashMap()),
        new DruidDataSource("datasource2", new HashMap())
    };
    EasyMock.expect(server.getDataSources()).andReturn(
        ImmutableList.of(druidDataSources[0], druidDataSources[1])
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getQueryableDataSources("full", null);
    Set<DruidDataSource> result = (Set<DruidDataSource>)response.getEntity();
    DruidDataSource[] resultantDruidDataSources = new DruidDataSource[result.size()];
    result.toArray(resultantDruidDataSources);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, resultantDruidDataSources.length);
    Assert.assertArrayEquals(druidDataSources, resultantDruidDataSources);

    response = datasourcesResource.getQueryableDataSources(null, null);
    List<String> result1 = (List<String>)response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, result1.size());
    Assert.assertTrue(result1.contains("datasource1"));
    Assert.assertTrue(result1.contains("datasource2"));
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testGetSimpleQueryableDataSources() throws Exception
  {
    InventoryView inventoryView = EasyMock.createStrictMock(InventoryView.class);
    DruidServer server = EasyMock.createStrictMock(DruidServer.class);
    List<Map<String, Object>> input = new ArrayList(2);
    HashMap<String, Object> dataSourceProp1 = new HashMap<>();
    dataSourceProp1.put("name", "datasource1");
    dataSourceProp1.put("partitionName", "partition");
    dataSourceProp1.put("datasegment",
                 new DataSegment("datasource1", new Interval("2010-01-01/P1D"), null, null, null, null, null, 0x9, 0));

    HashMap<String, Object> dataSourceProp2 = new HashMap<>();
    dataSourceProp2.put("name", "datasource2");
    dataSourceProp2.put("partitionName", "partition");
    dataSourceProp2.put("datasegment",
                 new DataSegment("datasource2", new Interval("2010-01-01/P1D"), null, null, null, null, null, 0x9, 0));
    input.add(dataSourceProp1);
    input.add(dataSourceProp2);
    List<DruidDataSource> listDataSources = new ArrayList<>();
    for(Map<String, Object> entry : input){
      listDataSources.add(new DruidDataSource(entry.get("name").toString(), new HashMap())
                              .addSegment(entry.get("partitionName").toString(), (DataSegment)entry.get("datasegment")));
    }
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
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getQueryableDataSources(null, "simple");
    Assert.assertEquals(200, response.getStatus());
    List<Map<String, Object>> results = (List<Map<String, Object>>)response.getEntity();
    int index = 0;
    for(Map<String, Object> entry : results){
      Assert.assertEquals(input.get(index).get("name"), entry.get("name").toString());
      Assert.assertTrue(((Map) ((Map) entry.get("properties")).get("tiers")).containsKey(null));
      Assert.assertNotNull((((Map) entry.get("properties")).get("segments")));
      Assert.assertEquals(1, ((Map) ((Map) entry.get("properties")).get("segments")).get("count"));
      index++;
    }
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testFullGetTheDataSource() throws Exception
  {
    InventoryView inventoryView = EasyMock.createStrictMock(InventoryView.class);
    DruidServer server = EasyMock.createStrictMock(DruidServer.class);
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap());
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getTheDataSource("datasource1", "full");
    DruidDataSource result = (DruidDataSource)response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(dataSource1, result);
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testNullGetTheDataSource() throws Exception
  {
    InventoryView inventoryView = EasyMock.createStrictMock(InventoryView.class);
    DruidServer server = EasyMock.createStrictMock(DruidServer.class);
    EasyMock.expect(server.getDataSource("none")).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Assert.assertEquals(204, datasourcesResource.getTheDataSource("none", null).getStatus());
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSource() throws Exception
  {
    InventoryView inventoryView = EasyMock.createStrictMock(InventoryView.class);
    DruidServer server = EasyMock.createStrictMock(DruidServer.class);
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap());
    dataSource1.addSegment("partition",
                           new DataSegment("datasegment1", new Interval("2010-01-01/P1D"), null, null, null, null, null, 0x9, 0));
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getTheDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>)response.getEntity();
    Assert.assertEquals(1, ((Map)(result.get("tiers").get(null))).get("segmentCount"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertNotNull(result.get("segments").get("minTime").toString(), "2010-01-01T00:00:00.000Z");
    Assert.assertNotNull(result.get("segments").get("maxTime").toString(), "2010-01-02T00:00:00.000Z");
    EasyMock.verify(inventoryView, server);
  }
}
