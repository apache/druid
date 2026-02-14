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

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.IOException;

public class AWSClientUtilTest
{
  @Test
  public void testRecoverableException_IOException()
  {
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(SdkClientException.builder().cause(new IOException()).build()));
  }

  @Test
  public void testRecoverableException_RequestTimeout()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("RequestTimeout")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("RequestTimeout")
            .build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_500()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("Internal Server Error")
        .statusCode(500)
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_502()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("Bad Gateway")
        .statusCode(502)
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_503()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("Service Unavailable")
        .statusCode(503)
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ProvisionedThroughputExceededException()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("ProvisionedThroughputExceededException")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("ProvisionedThroughputExceededException")
            .build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ClockSkewedError()
  {
    AwsServiceException ex = AwsServiceException.builder()
        .message("RequestExpired")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("RequestExpired")
            .build())
        .build();
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testNonRecoverableException_RuntimeException()
  {
    SdkClientException ex = SdkClientException.builder().cause(new RuntimeException()).build();
    Assert.assertFalse(AWSClientUtil.isClientExceptionRecoverable(ex));
  }
}
