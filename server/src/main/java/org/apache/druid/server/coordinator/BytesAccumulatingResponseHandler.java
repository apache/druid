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

package org.apache.druid.server.coordinator;

import org.apache.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * An async BytesAccumulatingResponseHandler which returns unfinished response
 */
public class BytesAccumulatingResponseHandler extends InputStreamResponseHandler
{
  private int status;
  private String description;

  @Override
  public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    status = response.getStatus().getCode();
    description = response.getStatus().getReasonPhrase();
    return ClientResponse.unfinished(super.handleResponse(response, trafficCop).getObj());
  }

  public int getStatus()
  {
    return status;
  }

  public String getDescription()
  {
    return description;
  }

}
