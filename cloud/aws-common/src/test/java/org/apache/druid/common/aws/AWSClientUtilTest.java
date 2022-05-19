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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AWSClientUtilTest
{
  @Test
  public void testRecoverableException_IOException()
  {
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(new AmazonClientException(new IOException())));
  }

  @Test
  public void testRecoverableException_RequestTimeout()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("RequestTimeout");
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_500()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(500);
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_502()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(502);
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_503()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(503);
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ProvisionedThroughputExceededException()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("ProvisionedThroughputExceededException");
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ClockSkewedError()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("RequestExpired");
    Assert.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testNonRecoverableException_RuntimeException()
  {
    AmazonClientException ex = new AmazonClientException(new RuntimeException());
    Assert.assertFalse(AWSClientUtil.isClientExceptionRecoverable(ex));
  }
}
