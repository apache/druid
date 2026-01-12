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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;

import java.io.IOException;

public class AWSClientUtilTest
{
  @Test
  public void testRecoverableException_IOException()
  {
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(SdkException.create("", new IOException())));
  }

  @Test
  public void testRecoverableException_StatusCode_ServerError()
  {
    for (Integer statusCode : ImmutableList.of(500, 502, 503)) {
      AwsServiceException ex = AwsServiceException
          .builder()
          .awsErrorDetails(AwsErrorDetails.builder().build())
          .statusCode(statusCode)
          .build();
      Assert.assertTrue(StringUtils.format("Status code: %s", statusCode), AWSClientUtil.isClientExceptionRecoverable(ex));
    }
  }

  @Test
  public void testRecoverableException_RequestTimeout()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("RequestTimeout").build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ProvisionedThroughputExceededException()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("ProvisionedThroughputExceededException").build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ClockSkewedError()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("RequestExpired").build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testS3ErrorCodeRecoverable_RetryableCode()
  {
    Assert.assertTrue(AWSClientUtil.isS3ErrorCodeRecoverable("RequestLimitExceeded"));
    Assert.assertTrue(AWSClientUtil.isS3ErrorCodeRecoverable("SlowDown"));
    Assert.assertTrue(AWSClientUtil.isS3ErrorCodeRecoverable("InternalError"));
    Assert.assertTrue(AWSClientUtil.isS3ErrorCodeRecoverable("ServiceUnavailable"));
    Assert.assertTrue(AWSClientUtil.isS3ErrorCodeRecoverable("ThrottlingException"));
  }

  @Test
  public void testS3ErrorCodeRecoverable_NonRetryableCode()
  {
    Assert.assertFalse(AWSClientUtil.isS3ErrorCodeRecoverable("AccessDenied"));
    Assert.assertFalse(AWSClientUtil.isS3ErrorCodeRecoverable("NoSuchKey"));
    Assert.assertFalse(AWSClientUtil.isS3ErrorCodeRecoverable("InvalidBucketName"));
  }

  @Test
  public void testNonRecoverableException_RuntimeException()
  {
    SdkException ex = SdkException.create("test", new RuntimeException());
    Assert.assertFalse(AWSClientUtil.isClientExceptionRecoverable(ex));
  }
}
