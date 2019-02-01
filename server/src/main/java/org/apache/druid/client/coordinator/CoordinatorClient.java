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

package org.apache.druid.client.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.query.SegmentDescriptor;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class CoordinatorClient
{
  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public CoordinatorClient(
      ObjectMapper jsonMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.druidLeaderClient = druidLeaderClient;
  }

  /**
   * Checks the given segment is handed off or not.
   * It can return null if the HTTP call returns 404 which can happen during rolling update.
   */
  @Nullable
  public Boolean isHandOffComplete(String dataSource, SegmentDescriptor descriptor)
  {
    try {
      FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.GET,
              StringUtils.format(
                  "/druid/coordinator/v1/datasources/%s/handoffComplete?interval=%s&partitionNumber=%d&version=%s",
                  StringUtils.urlEncode(dataSource),
                  descriptor.getInterval(),
                  descriptor.getPartitionNumber(),
                  descriptor.getVersion()
              )
          )
      );

      if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
        return null;
      }

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching serverView status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }
      return jsonMapper.readValue(response.getContent(), new TypeReference<Boolean>()
      {
      });
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<ImmutableSegmentLoadInfo> fetchServerView(String dataSource, Interval interval, boolean incompleteOk)
  {
    try {
      FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET,
                                        StringUtils.format(
                                            "/druid/coordinator/v1/datasources/%s/intervals/%s/serverview?partial=%s",
                                            dataSource,
                                            interval.toString().replace('/', '_'),
                                            incompleteOk
                                        ))
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching serverView status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<ImmutableSegmentLoadInfo>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
