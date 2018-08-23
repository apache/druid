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

package io.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.task.IndexTaskClientFactory;
import io.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public class ParallelIndexTaskClientFactory implements IndexTaskClientFactory<ParallelIndexTaskClient>
{
  private final HttpClient httpClient;
  private final ObjectMapper mapper;

  @Inject
  public ParallelIndexTaskClientFactory(
      @EscalatedGlobal HttpClient httpClient,
      @Smile ObjectMapper mapper
  )
  {
    this.httpClient = httpClient;
    this.mapper = mapper;
  }

  @Override
  public ParallelIndexTaskClient build(
      TaskInfoProvider taskInfoProvider,
      String callerId,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  )
  {
    Preconditions.checkState(numThreads == 1, "expect numThreads to be 1");
    return new ParallelIndexTaskClient(
        httpClient,
        mapper,
        taskInfoProvider,
        httpTimeout,
        callerId,
        numRetries
    );
  }
}
