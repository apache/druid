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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class RemoteTaskActionClientFactoryTest
{
  @Test
  public void test_buildRetryPolicy_withDefaultConfig()
  {
    final RetryPolicyConfig config = new RetryPolicyConfig();
    final StandardRetryPolicy retryPolicy = RemoteTaskActionClientFactory.buildRetryPolicy(config);

    // Default maxRetryCount is 13, so maxAttempts should be 14 (13 retries + 1 initial attempt)
    Assert.assertEquals(14, retryPolicy.maxAttempts());

    // Default minWait is PT5S (5 seconds)
    Assert.assertEquals(5000, retryPolicy.minWaitMillis());

    // Default maxWait is PT1M (1 minute)
    Assert.assertEquals(60000, retryPolicy.maxWaitMillis());
  }

  @Test
  public void test_buildRetryPolicy_withCustomConfig()
  {
    final RetryPolicyConfig config = new RetryPolicyConfig()
        .setMaxRetryCount(5)
        .setMinWait(new Period("PT10S"))
        .setMaxWait(new Period("PT2M"));

    final StandardRetryPolicy retryPolicy = RemoteTaskActionClientFactory.buildRetryPolicy(config);

    // maxRetryCount is 5, so maxAttempts should be 6 (5 retries + 1 initial attempt)
    Assert.assertEquals(6, retryPolicy.maxAttempts());

    // minWait is PT10S (10 seconds)
    Assert.assertEquals(10000, retryPolicy.minWaitMillis());

    // maxWait is PT2M (2 minutes)
    Assert.assertEquals(120000, retryPolicy.maxWaitMillis());
  }

  @Test
  public void test_buildRetryPolicy_withZeroRetries()
  {
    final RetryPolicyConfig config = new RetryPolicyConfig()
        .setMaxRetryCount(0)
        .setMinWait(new Period("PT1S"))
        .setMaxWait(new Period("PT30S"));

    final StandardRetryPolicy retryPolicy = RemoteTaskActionClientFactory.buildRetryPolicy(config);

    // maxRetryCount is 0, so maxAttempts should be 1 (0 retries + 1 initial attempt)
    Assert.assertEquals(1, retryPolicy.maxAttempts());

    // minWait is PT1S (1 second)
    Assert.assertEquals(1000, retryPolicy.minWaitMillis());

    // maxWait is PT30S (30 seconds)
    Assert.assertEquals(30000, retryPolicy.maxWaitMillis());
  }
}
