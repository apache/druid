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

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Period;

/**
 */
public class HttpServerInventoryViewConfig
{
  @JsonProperty
  private final long serverTimeout;

  @JsonProperty
  private final int numThreads;

  @JsonCreator
  public HttpServerInventoryViewConfig(
      @JsonProperty("serverTimeout") Period serverTimeout,
      @JsonProperty("numThreads") Integer numThreads
  )
  {
    this.serverTimeout = serverTimeout != null
                         ? serverTimeout.toStandardDuration().getMillis()
                         : 4*60*1000; //4 mins

    this.numThreads = numThreads != null ? numThreads.intValue() : 5;

    Preconditions.checkArgument(this.serverTimeout > 0, "server timeout must be > 0 ms");
    Preconditions.checkArgument(this.numThreads > 1, "numThreads must be > 1");
  }

  public long getServerTimeout()
  {
    return serverTimeout;
  }

  public int getNumThreads()
  {
    return numThreads;
  }
}
