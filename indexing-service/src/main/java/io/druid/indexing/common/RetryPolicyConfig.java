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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class RetryPolicyConfig
{
  @JsonProperty
  private Period minWait = new Period("PT1M");

  @JsonProperty
  private Period maxWait = new Period("PT10M");

  @JsonProperty
  private long maxRetryCount = 10;

  public Period getMinWait()
  {
    return minWait;
  }

  RetryPolicyConfig setMinWait(Period minWait)
  {
    this.minWait = minWait;
    return this;
  }

  public Period getMaxWait()
  {
    return maxWait;
  }

  RetryPolicyConfig setMaxWait(Period maxWait)
  {
    this.maxWait = maxWait;
    return this;
  }

  public long getMaxRetryCount()
  {
    return maxRetryCount;
  }

  RetryPolicyConfig setMaxRetryCount(long maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
    return this;
  }
}
