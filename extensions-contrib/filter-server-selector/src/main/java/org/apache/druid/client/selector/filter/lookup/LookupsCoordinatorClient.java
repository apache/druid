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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

public class LookupsCoordinatorClient
{
  
  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public LookupsCoordinatorClient(
      ObjectMapper jsonMapper,
      @Coordinator DruidLeaderClient druidLeaderClient)
  {
    this.jsonMapper = jsonMapper;
    this.druidLeaderClient = druidLeaderClient;
  }
  
  public Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> fetchLookupNodeStatus()
  {
    try {
      StringFullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.GET,
              "/druid/coordinator/v1/lookups/nodeStatus"));

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching status of lookups on nodes with status[%s] content[%s]",
            response.getStatus(),
            response.getContent());
      }

      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>>>()
          {
          });
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
