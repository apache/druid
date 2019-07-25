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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CoordinatorResourceTestClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String coordinator;
  private final StatusResponseHandler responseHandler;

  @Inject
  CoordinatorResourceTestClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.coordinator = config.getCoordinatorUrl();
    this.responseHandler = StatusResponseHandler.getInstance();
  }

  private String getCoordinatorURL()
  {
    return StringUtils.format(
        "%s/druid/coordinator/v1/",
        coordinator
    );
  }

  private String getMetadataSegmentsURL(String dataSource)
  {
    return StringUtils.format("%smetadata/datasources/%s/segments", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
  }

  private String getIntervalsURL(String dataSource)
  {
    return StringUtils.format("%sdatasources/%s/intervals", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
  }

  private String getFullSegmentsURL(String dataSource)
  {
    return StringUtils.format("%sdatasources/%s/segments?full", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
  }

  private String getLoadStatusURL()
  {
    return StringUtils.format("%s%s", getCoordinatorURL(), "loadstatus");
  }

  // return a list of the segment dates for the specified datasource
  public List<String> getMetadataSegments(final String dataSource)
  {
    ArrayList<String> segments;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getMetadataSegmentsURL(dataSource));

      segments = jsonMapper.readValue(
          response.getContent(), new TypeReference<List<String>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return segments;
  }

  // return a list of the segment dates for the specified datasource
  public List<String> getSegmentIntervals(final String dataSource)
  {
    ArrayList<String> segments;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getIntervalsURL(dataSource));

      segments = jsonMapper.readValue(
          response.getContent(), new TypeReference<List<String>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return segments;
  }

  // return a set of the segment versions for the specified datasource
  public List<DataSegment> getAvailableSegments(final String dataSource)
  {
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getFullSegmentsURL(dataSource));

      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<DataSegment>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Integer> getLoadStatus()
  {
    Map<String, Integer> status;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getLoadStatusURL());

      status = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, Integer>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return status;
  }

  public boolean areSegmentsLoaded(String dataSource)
  {
    final Map<String, Integer> status = getLoadStatus();
    return (status.containsKey(dataSource) && status.get(dataSource) == 100.0);
  }

  public void unloadSegmentsForDataSource(String dataSource)
  {
    try {
      makeRequest(HttpMethod.DELETE, StringUtils.format("%sdatasources/%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource)));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteSegmentsDataSource(String dataSource, Interval interval)
  {
    try {
      makeRequest(
          HttpMethod.DELETE,
          StringUtils.format(
              "%sdatasources/%s/intervals/%s",
              getCoordinatorURL(),
              StringUtils.urlEncode(dataSource),
              interval.toString().replace('/', '_')
          )
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public HttpResponseStatus getProxiedOverlordScalingResponseStatus()
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format(
              "%s/druid/indexer/v1/scaling",
              coordinator
          )
      );
      return response.getStatus();
    }
    catch (Exception e) {
      throw new RE(e, "Unable to get scaling status from [%s]", coordinator);
    }
  }

  public Map<String, Object> initializeLookups(String filePath) throws Exception
  {
    String url = StringUtils.format("%slookups/config", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(ImmutableMap.of())
        ), responseHandler
    ).get();

    if (!response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE(
          "Error while querying[%s] status[%s] content[%s]",
          url,
          response.getStatus(),
          response.getContent()
      );
    }

    StatusResponseHolder response2 = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(jsonMapper.readValue(CoordinatorResourceTestClient.class.getResourceAsStream(filePath), new TypeReference<Map<Object, Object>>(){}))
        ), responseHandler
    ).get();

    if (!response2.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
      throw new ISE(
          "Error while querying[%s] status[%s] content[%s]",
          url,
          response2.getStatus(),
          response2.getContent()
      );
    }

    Map<String, Object> results2 = jsonMapper.readValue(
        response.getContent(),
        new TypeReference<Map<String, Object>>()
        {
        }
    );

    return results2;
  }

  private Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> getLookupLoadStatus()
  {
    String url = StringUtils.format("%slookups/nodeStatus", getCoordinatorURL());

    Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> status;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, url);

      status = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return status;
  }

  public boolean areLookupsLoaded(String lookup)
  {
    final Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> status = getLookupLoadStatus();

    final Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> defaultTier = status.get("__default");

    boolean isLoaded = true;
    for (Map.Entry<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> host : defaultTier.entrySet()) {
      isLoaded &= host.getValue().getCurrent().containsKey(lookup);
    }

    return isLoaded;
  }

  private StatusResponseHolder makeRequest(HttpMethod method, String url)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(method, new URL(url)),
          responseHandler
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while making request to url[%s] status[%s] content[%s]",
            url,
            response.getStatus(),
            response.getContent()
        );
      }
      return response;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
