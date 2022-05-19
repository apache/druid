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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.QueryResourceTestClient;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class TestQueryHelper extends AbstractTestQueryHelper<QueryWithResults>
{

  @Inject
  TestQueryHelper(
      ObjectMapper jsonMapper,
      QueryResourceTestClient queryClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, queryClient, config);
  }

  private TestQueryHelper(
      ObjectMapper jsonMapper,
      QueryResourceTestClient queryResourceTestClient,
      String broker,
      String brokerTLS,
      String router,
      String routerTLS
  )
  {
    super(
        jsonMapper,
        queryResourceTestClient,
        broker,
        brokerTLS,
        router,
        routerTLS
    );
  }

  @Override
  public String getQueryURL(String schemeAndHost)
  {
    return StringUtils.format("%s/druid/v2?pretty", schemeAndHost);
  }

  /**
   * clone a new instance of current object with given encoding
   *
   * @param contentType Content-Type header of request. Cannot be NULL. Both application/json and application/x-jackson-smile are allowed
   * @param accept      Accept header of request. Both application/json and application/x-jackson-smile are allowed
   */
  public TestQueryHelper withEncoding(@NotNull String contentType, @Nullable String accept)
  {
    return new TestQueryHelper(
        this.jsonMapper,
        ((QueryResourceTestClient) this.queryClient).withEncoding(contentType, accept),
        this.broker,
        this.brokerTLS,
        this.router,
        this.routerTLS
    );
  }
}
