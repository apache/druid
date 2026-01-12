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

package org.apache.druid.common.aws.v2;

import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.io.IOException;
import java.util.Set;

public class AWSClientUtil
{
  /**
   * This list of error codes comes from {@link RetryUtils}, and
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error Responses</a>.
   * These codes are used to determine if an error from batch operations like
   * {@link software.amazon.awssdk.services.s3.S3Client#deleteObjects} is recoverable.
   * In AWS SDK v2, batch delete errors are returned in the response as {@link S3Error} objects
   * rather than thrown as exceptions.
   */
  private static final Set<String> RECOVERABLE_ERROR_CODES = ImmutableSet.of(
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
   * Checks whether an exception can be retried or not.
   */
  public static boolean isClientExceptionRecoverable(SdkException exception)
  {
    // Always retry on client exceptions caused by IOException
    if (exception.getCause() instanceof IOException) {
      return true;
    }

    // A special check carried forwarded from the previous implementation.
    if (exception instanceof AwsServiceException
        && "RequestTimeout".equals(((AwsServiceException) exception).awsErrorDetails().errorCode())) {
      return true;
    }

    // This will retry for 5xx errors.
    return isExceptionRecoverable(exception)
           || RetryUtils.isRetryableException(exception)
           || RetryUtils.isThrottlingException(exception)
           || RetryUtils.isClockSkewException(exception);
  }

  public static boolean isExceptionRecoverable(SdkException exception)
  {
    if (exception instanceof AwsServiceException) {
      int statusCode = ((AwsServiceException) exception).statusCode();
      return statusCode >= 500 && statusCode < 600;
    }
    return false;
  }

  /**
   * Checks whether an S3 error code from batch operations (like deleteObjects) is recoverable.
   * In AWS SDK v2, batch operation errors are returned in the response rather than thrown as exceptions.
   *
   * @param errorCode the error code from {@link S3Error#code()}
   * @return true if the error is recoverable and the operation should be retried
   */
  public static boolean isS3ErrorCodeRecoverable(String errorCode)
  {
    return RECOVERABLE_ERROR_CODES.contains(errorCode);
  }
}
