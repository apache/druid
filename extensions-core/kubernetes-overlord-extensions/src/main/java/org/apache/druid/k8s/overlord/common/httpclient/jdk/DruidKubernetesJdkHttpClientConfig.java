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

package org.apache.druid.k8s.overlord.common.httpclient.jdk;


import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class DruidKubernetesJdkHttpClientConfig
{

  @JsonProperty
  @Nullable
  private Integer maxWorkerThreads = null;

  @JsonProperty
  private int coreWorkerThreads = 50;

  @JsonProperty
  private long workerThreadKeepAliveTime = 60L;

  public int getMaxWorkerThreads()
  {
    if (maxWorkerThreads == null || maxWorkerThreads < coreWorkerThreads) {
      return coreWorkerThreads;
    } else {
      return maxWorkerThreads;
    }
  }

  public int getCoreWorkerThreads()
  {
    return coreWorkerThreads;
  }

  public long getWorkerThreadKeepAliveTime()
  {
    return workerThreadKeepAliveTime;
  }

}
