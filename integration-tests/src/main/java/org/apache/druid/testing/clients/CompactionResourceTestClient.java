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
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.util.Map;

public class CompactionResourceTestClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String coordinator;
  private final StatusResponseHandler responseHandler;

  @Inject
  CompactionResourceTestClient(
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

  public void submitCompactionConfig(final DataSourceCompactionConfig dataSourceCompactionConfig) throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.POST, new URL(url)).setContent(
            "application/json",
            jsonMapper.writeValueAsBytes(dataSourceCompactionConfig)
        ), responseHandler
    ).get();

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while submiting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public void deleteCompactionConfig(final String dataSource) throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction/%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.DELETE, new URL(url)), responseHandler).get();

    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while deleting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public CoordinatorCompactionConfig getCoordinatorCompactionConfigs() throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<CoordinatorCompactionConfig>() {});
  }

  public DataSourceCompactionConfig getDataSourceCompactionConfig(String dataSource) throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction/%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction config status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<DataSourceCompactionConfig>() {});
  }

  public void forceTriggerAutoCompaction() throws Exception
  {
    String url = StringUtils.format("%scompaction/compact", getCoordinatorURL());
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while force trigger auto compaction status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public void updateCompactionTaskSlot(Double compactionTaskSlotRatio, Integer maxCompactionTaskSlots) throws Exception
  {
    String url = StringUtils.format("%sconfig/compaction/taskslots?ratio=%s&max=%s",
                                    getCoordinatorURL(),
                                    StringUtils.urlEncode(compactionTaskSlotRatio.toString()),
                                    StringUtils.urlEncode(maxCompactionTaskSlots.toString()));
    StatusResponseHolder response = httpClient.go(new Request(HttpMethod.POST, new URL(url)), responseHandler).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while updating compaction task slot status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
  }

  public Map<String, String> getCompactionProgress(String dataSource) throws Exception
  {
    String url = StringUtils.format("%scompaction/progress?dataSource=%s", getCoordinatorURL(), StringUtils.urlEncode(dataSource));
    StatusResponseHolder response = httpClient.go(
        new Request(HttpMethod.GET, new URL(url)), responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while getting compaction progress status[%s] content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(response.getContent(), new TypeReference<Map<String, String>>() {});
  }
}
