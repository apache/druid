/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.indexing.common.TaskInfoProvider;

public class KafkaIndexTaskClientFactory
{
  private HttpClient httpClient;
  private ObjectMapper mapper;

  @Inject
  public KafkaIndexTaskClientFactory(@Global HttpClient httpClient, @Json ObjectMapper mapper)
  {
    this.httpClient = httpClient;
    this.mapper = mapper;
  }

  public KafkaIndexTaskClient build(TaskInfoProvider taskInfoProvider)
  {
    return new KafkaIndexTaskClient(httpClient, mapper, taskInfoProvider);
  }
}
