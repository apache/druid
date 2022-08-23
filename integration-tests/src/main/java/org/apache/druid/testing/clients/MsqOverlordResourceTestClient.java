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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.guice.models.MSQTaskReportDeserializable;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

public class MsqOverlordResourceTestClient extends OverlordResourceTestClient
{
  ObjectMapper jsonMapper;

  @Inject
  MsqOverlordResourceTestClient(
      @Json ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, httpClient, config);
    this.jsonMapper = jsonMapper;
  }

  public Map<String, MSQTaskReportDeserializable> getTaskReportForMsqTask(String taskId)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format(
              "%s%s",
              getIndexerURL(),
              StringUtils.format("task/%s/reports", StringUtils.urlEncode(taskId))
          )
      );
      jsonMapper.registerModules(new MSQIndexingModule().getJacksonModules());
      jsonMapper.registerSubtypes(ImmutableList.of(MSQTaskReportDeserializable.class));
      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, MSQTaskReportDeserializable>>()
          {
          }
      );
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class MsqTaskReportType extends HashMap<String, MSQTaskReport>
  {

  }
}
