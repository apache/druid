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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class RetryPolicyConfig
{
  @JsonProperty
  private Period minWait = new Period("PT5S");

  @JsonProperty
  private Period maxWait = new Period("PT1M");

  @JsonProperty
  private long maxRetryCount = 60;

  public Period getMinWait()
  {
    return minWait;
  }

  public RetryPolicyConfig setMinWait(Period minWait)
  {
    this.minWait = minWait;
    return this;
  }

  public Period getMaxWait()
  {
    return maxWait;
  }

  public RetryPolicyConfig setMaxWait(Period maxWait)
  {
    this.maxWait = maxWait;
    return this;
  }

  public long getMaxRetryCount()
  {
    return maxRetryCount;
  }

  public RetryPolicyConfig setMaxRetryCount(long maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
    return this;
  }
}
