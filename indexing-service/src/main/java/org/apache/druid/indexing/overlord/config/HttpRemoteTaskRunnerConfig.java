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

package org.apache.druid.indexing.overlord.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class HttpRemoteTaskRunnerConfig extends RemoteTaskRunnerConfig
{
  @JsonProperty
  private int workerSyncNumThreads = 5;

  @JsonProperty
  private int shutdownRequestMaxRetries = 3;

  @JsonProperty
  private Period shutdownRequestHttpTimeout = new Period("PT1M");

  @JsonProperty
  private int assignRequestMaxRetries = 3;

  @JsonProperty
  private Period assignRequestHttpTimeout = new Period("PT1M");

  @JsonProperty
  private Period syncRequestTimeout = new Period("PT3M");

  @JsonProperty
  private Period serverUnstabilityTimeout = new Period("PT1M");

  public int getWorkerSyncNumThreads()
  {
    return workerSyncNumThreads;
  }

  public int getShutdownRequestMaxRetries()
  {
    return shutdownRequestMaxRetries;
  }

  public Period getShutdownRequestHttpTimeout()
  {
    return shutdownRequestHttpTimeout;
  }

  public int getAssignRequestMaxRetries()
  {
    return assignRequestMaxRetries;
  }

  public Period getAssignRequestHttpTimeout()
  {
    return assignRequestHttpTimeout;
  }

  public Period getSyncRequestTimeout()
  {
    return syncRequestTimeout;
  }

  public Period getServerUnstabilityTimeout()
  {
    return serverUnstabilityTimeout;
  }
}
