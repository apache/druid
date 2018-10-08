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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import java.net.URL;
import java.nio.charset.StandardCharsets;
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
    this.responseHandler = new StatusResponseHandler(StandardCharsets.UTF_8);
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
    return StringUtils.format("%smetadata/datasources/%s/segments", getCoordinatorURL(), dataSource);
  }

  private String getIntervalsURL(String dataSource)
  {
    return StringUtils.format("%sdatasources/%s/intervals", getCoordinatorURL(), dataSource);
  }

  private String getLoadStatusURL()
  {
    return StringUtils.format("%s%s", getCoordinatorURL(), "loadstatus");
  }

  // return a list of the segment dates for the specified datasource
  public List<String> getMetadataSegments(final String dataSource)
  {
    ArrayList<String> segments = null;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getMetadataSegmentsURL(dataSource));

      segments = jsonMapper.readValue(
          response.getContent(), new TypeReference<ArrayList<String>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return segments;
  }

  // return a list of the segment dates for the specified datasource
  public List<String> getSegmentIntervals(final String dataSource)
  {
    ArrayList<String> segments = null;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getIntervalsURL(dataSource));

      segments = jsonMapper.readValue(
          response.getContent(), new TypeReference<ArrayList<String>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return segments;
  }

  private Map<String, Integer> getLoadStatus()
  {
    Map<String, Integer> status = null;
    try {
      StatusResponseHolder response = makeRequest(HttpMethod.GET, getLoadStatusURL());

      status = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, Integer>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
      makeRequest(HttpMethod.DELETE, StringUtils.format("%sdatasources/%s", getCoordinatorURL(), dataSource));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
              dataSource, interval.toString().replace("/", "_")
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
    }
  }
}
