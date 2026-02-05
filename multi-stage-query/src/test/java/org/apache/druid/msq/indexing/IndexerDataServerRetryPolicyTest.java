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

package org.apache.druid.msq.indexing;

import org.apache.druid.java.util.common.RetryUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class IndexerDataServerRetryPolicyTest
{
  @Test
  public void testStandard()
  {
    final IndexerDataServerRetryPolicy policy = IndexerDataServerRetryPolicy.standard();

    Assert.assertEquals(5, policy.maxAttempts());
    Assert.assertEquals(RetryUtils.BASE_SLEEP_MILLIS, policy.minWaitMillis());
    Assert.assertEquals(RetryUtils.MAX_SLEEP_MILLIS, policy.maxWaitMillis());
  }

  @Test
  public void testNoRetries()
  {
    final IndexerDataServerRetryPolicy policy = IndexerDataServerRetryPolicy.noRetries();

    Assert.assertEquals(1, policy.maxAttempts());
    Assert.assertEquals(0, policy.minWaitMillis());
    Assert.assertEquals(0, policy.maxWaitMillis());
  }

  @Test
  public void testRetryThrowableWithGenericException()
  {
    final IndexerDataServerRetryPolicy policy = IndexerDataServerRetryPolicy.standard();

    Assert.assertTrue(policy.retryThrowable(new IOException("test")));
    Assert.assertTrue(policy.retryThrowable(new RuntimeException("test")));
  }

  @Test
  public void testRetryThrowableWithInterruptedException()
  {
    final IndexerDataServerRetryPolicy policy = IndexerDataServerRetryPolicy.standard();

    // Chains including InterruptedException should not be retried
    Assert.assertFalse(policy.retryThrowable(new InterruptedException()));
    Assert.assertFalse(policy.retryThrowable(new RuntimeException(new InterruptedException())));
  }
}
