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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public abstract class SeekableStreamIndexTaskClientFactory<T extends SeekableStreamIndexTaskClient>
    implements IndexTaskClientFactory<T>
{
  private HttpClient httpClient;
  private ObjectMapper mapper;

  @Inject
  public SeekableStreamIndexTaskClientFactory(
      @EscalatedGlobal HttpClient httpClient,
      @Json ObjectMapper mapper
  )
  {
    this.httpClient = httpClient;
    this.mapper = mapper;
  }

  @Override
  public abstract T build(
      TaskInfoProvider taskInfoProvider,
      String dataSource,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  );

  protected HttpClient getHttpClient()
  {
    return httpClient;
  }

  protected ObjectMapper getMapper()
  {
    return mapper;
  }
}
