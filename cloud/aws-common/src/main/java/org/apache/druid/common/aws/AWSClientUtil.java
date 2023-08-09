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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Set;

public class AWSClientUtil
{
  /**
   * This list of error code come from {@link RetryUtils}, and
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">...</a>. At the moment, aws sdk
   * does not expose a good way of retrying
   * {@link com.amazonaws.services.s3.AmazonS3#deleteObjects(DeleteObjectsRequest)} requests. This request is used in
   * org.apache.druid.storage.s3.S3DataSegmentKiller to delete a batch of segments from deep storage.
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

    if (RetryUtils.isClockSkewError(exception)) {
      return true;
    }

    if (exception instanceof MultiObjectDeleteException) {
      MultiObjectDeleteException multiObjectDeleteException = (MultiObjectDeleteException) exception;
      for (MultiObjectDeleteException.DeleteError error : multiObjectDeleteException.getErrors()) {
        if (RECOVERABLE_ERROR_CODES.contains(error.getCode())) {
          return true;
        }
      }
    }

    return false;
  }
}
