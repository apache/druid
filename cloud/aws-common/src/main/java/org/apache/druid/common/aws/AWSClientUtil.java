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

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;

import java.io.IOException;
import java.util.Set;

public class AWSClientUtil
{
  /**
   * This list of error codes come from AWS SDK retry utils and
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">...</a>.
   */
  public static final Set<String> RECOVERABLE_ERROR_CODES = ImmutableSet.of(
      "503 SlowDown",
      "AuthFailure",
      "BandwidthLimitExceeded",
      "EC2ThrottledException",
      "IDPCommunicationError",
      "InternalError",
      "InvalidSignatureException",
      "PriorRequestNotComplete",
      "ProvisionedThroughputExceededException",
      "RequestExpired",
      "RequestInTheFuture",
      "RequestLimitExceeded",
      "RequestThrottled",
      "RequestThrottledException",
      "RequestTimeTooSkewed",
      "RequestTimeout",
      "RequestTimeoutException",
      "ServiceUnavailable",
      "SignatureDoesNotMatch",
      "SlowDown",
      "ThrottledException",
      "ThrottlingException",
      "TooManyRequestsException",
      "TransactionInProgressException",
      "Throttling"
  );

  /**
   * Checks whether an exception can be retried or not for AWS SDK v2.
   */
  public static boolean isClientExceptionRecoverable(SdkException exception)
  {
    // Always retry on client exceptions caused by IOException
    if (exception.getCause() instanceof IOException) {
      return true;
    }

    // Check if the SDK marks it as retryable
    if (exception.retryable()) {
      return true;
    }

    // Check for service exceptions with specific error codes
    if (exception instanceof AwsServiceException) {
      AwsServiceException serviceException = (AwsServiceException) exception;

      // Retry on 5xx errors
      if (serviceException.statusCode() >= 500) {
        return true;
      }

      // Retry on 429 (Too Many Requests)
      if (serviceException.statusCode() == 429) {
        return true;
      }

      // Check for specific error codes
      if (serviceException.awsErrorDetails() != null) {
        String errorCode = serviceException.awsErrorDetails().errorCode();
        if (errorCode != null && RECOVERABLE_ERROR_CODES.contains(errorCode)) {
          return true;
        }
      }
    }

    // Check for SdkClientException specific messages
    if (exception instanceof SdkClientException) {
      String message = exception.getMessage();
      if (message != null) {
        if (message.contains("Unable to execute HTTP request") ||
            message.contains("Data read has a different length than the expected") ||
            message.contains("Unable to find a region")) {
          return true;
        }
      }
    }

    return false;
  }
}
