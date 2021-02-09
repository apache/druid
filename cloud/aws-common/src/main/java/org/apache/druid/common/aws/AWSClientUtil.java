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

package org.apache.druid.common.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.RetryUtils;

import java.io.IOException;

public class AWSClientUtil
{
  /**
   * Checks whether an exception can be retried or not. Implementation is copied
   * from {@link com.amazonaws.retry.PredefinedRetryPolicies.SDKDefaultRetryCondition} except deprecated methods
   * have been replaced with their recent versions.
   */
  public static boolean isClientExceptionRecoverable(AmazonClientException exception)
  {
    // Always retry on client exceptions caused by IOException
    if (exception.getCause() instanceof IOException) {
      return true;
    }

    // A special check carried forwarded from previous implementation.
    if (exception instanceof AmazonServiceException
        && "RequestTimeout".equals(((AmazonServiceException) exception).getErrorCode())) {
      return true;
    }

    // This will retry for 5xx errors.
    if (RetryUtils.isRetryableServiceException(exception)) {
      return true;
    }

    if (RetryUtils.isThrottlingException(exception)) {
      return true;
    }

    return RetryUtils.isClockSkewError(exception);
  }
}
