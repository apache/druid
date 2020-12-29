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

package org.apache.druid.client.selector.filter.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class LookupCoordinatorClientTest
{
  @Test(expected = RuntimeException.class)
  public void testFetchNodeStatusError() throws IOException, InterruptedException
  {
    LookupsCoordinatorClient testClient = createMockLookupsCoordinatorClient(HttpResponseStatus.FORBIDDEN);
    testClient.fetchLookupNodeStatus();
  }

  @Test
  public void testFetchNodeStatus() throws IOException, InterruptedException
  {
    LookupsCoordinatorClient testClient = createMockLookupsCoordinatorClient(HttpResponseStatus.OK);
    Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> lookupStatus = testClient
        .fetchLookupNodeStatus();

    Assert.assertEquals(lookupStatus.size(), 1);
    Assert.assertTrue(lookupStatus.containsKey("__default"));
    Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> lookupTier = lookupStatus.get("__default");
    HostAndPort hp = HostAndPort.fromString("localhost:8080");
    Assert.assertTrue(lookupTier.containsKey(hp));
    LookupsState<LookupExtractorFactoryMapContainer> lookups = lookupTier.get(hp);
    Assert.assertTrue(lookups.getCurrent().containsKey("countries"));
    Assert.assertTrue(lookups.getCurrent().containsKey("test"));
  }

  public LookupsCoordinatorClient createMockLookupsCoordinatorClient(HttpResponseStatus returnCode)
      throws IOException, InterruptedException
  {
    StringFullResponseHolder successfulReq = EasyMock.createMock(StringFullResponseHolder.class);
    EasyMock.expect(successfulReq.getStatus()).andReturn(returnCode).anyTimes();
    String nodeStatus = "{\"__default\":{\"localhost:8080\":{\"current\":{\"test\":{\"version\":\"2020-07-14T19:18:51.303Z\",\"lookupExtractorFactory\":{\"type\":\"map\",\"map\":{\"a\":\"alpha\",\"b\":\"beta\",\"c\":\"charlie\"},\"isOneToOne\":false}},\"countries\":{\"version\":\"2020-08-10T17:56:39.745Z\",\"lookupExtractorFactory\":{\"type\":\"cachedNamespace\",\"extractionNamespace\":{\"type\":\"jdbc\",\"connectorConfig\":{\"createTables\":false,\"host\":\"localhost\",\"port\":1527,\"connectURI\":\"jdbc:mysql://localhost:3306/data\",\"user\":\"lookup\",\"password\":{\"type\":\"default\",\"password\":\"druid\"},\"dbcp\":null},\"table\":\"countries\",\"keyColumn\":\"code\",\"valueColumn\":\"name\",\"tsColumn\":\"created_at\",\"filter\":null,\"pollPeriod\":\"PT0S\"},\"firstCacheTimeout\":5000,\"injective\":false}}},\"toLoad\":{},\"toDrop\":[]}}}";
    EasyMock.expect(successfulReq.getContent()).andReturn(nodeStatus).anyTimes();
    DruidLeaderClient mockLeader = EasyMock.createMock(DruidLeaderClient.class);
    Request mockRequest = EasyMock.mock(Request.class);
    EasyMock.expect(mockLeader.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/lookups/nodeStatus"))
        .andReturn(mockRequest).anyTimes();
    EasyMock.expect(mockLeader.go(mockRequest)).andReturn(successfulReq).anyTimes();
    EasyMock.replay(successfulReq, mockLeader, mockRequest);
    return new LookupsCoordinatorClient(new ObjectMapper(), mockLeader);
  }
}
