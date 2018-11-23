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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.Executor;

public class CaffeineCacheConfig
{
  @JsonProperty
  private long expireAfter = -1;

  @JsonProperty
  private long sizeInBytes = -1;

  @JsonProperty
  // Do not use COMMON_FJP unless you're running 8u60 or higher
  // see https://github.com/ben-manes/caffeine/issues/77
  private CacheExecutorFactory cacheExecutorFactory = CacheExecutorFactory.COMMON_FJP;

  @JsonProperty
  private boolean evictOnClose = false;

  public long getExpireAfter()
  {
    return expireAfter;
  }

  public long getSizeInBytes()
  {
    return sizeInBytes;
  }

  public Executor createExecutor()
  {
    return cacheExecutorFactory.createExecutor();
  }

  public boolean isEvictOnClose()
  {
    return evictOnClose;
  }
}
