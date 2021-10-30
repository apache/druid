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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.Query;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

public class QueryResourceTestClient extends AbstractQueryResourceTestClient<Query>
{

  @Inject
  QueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this(jsonMapper, smileMapper, httpClient, config.getRouterUrl(), MediaType.APPLICATION_JSON, null);
  }

  private QueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @TestClient HttpClient httpClient,
      String routerUrl,
      String contentType,
      String accept
  )
  {
    super(jsonMapper, smileMapper, httpClient, routerUrl, contentType, accept);
  }

  /**
   * clone a new instance of current object with given encoding.
   * Note: For {@link AbstractQueryResourceTestClient#queryAsync(String, Object)} operation, contentType could only be application/json
   *
   * @param contentType Content-Type header of request. Cannot be NULL. Both application/json and application/x-jackson-smile are allowed
   * @param accept      Accept header of request. Both application/json and application/x-jackson-smile are allowed
   */
  public QueryResourceTestClient withEncoding(String contentType, @Nullable String accept)
  {
    return new QueryResourceTestClient(
        this.jsonMapper,
        this.smileMapper,
        this.httpClient,
        this.routerUrl,
        contentType,
        accept
    );
  }
}
