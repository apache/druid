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

package org.apache.druid.storage.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Multi-Region Access Point requests must be signed with SigV4A, which the SDK only enables when
 * software.amazon.awssdk:http-auth-aws-crt is on the classpath. The request is aborted before transmission.
 */
public class S3MultiRegionAccessPointSigningTest
{
  private static final String MRAP_ARN = "arn:aws:s3::123456789012:accesspoint:abcdef0123456.mrap";

  @Test
  public void testMrapRequestIsSignedWithSigV4a()
  {
    final AtomicReference<String> authorization = new AtomicReference<>();
    final ExecutionInterceptor captureAndAbort = new ExecutionInterceptor()
    {
      @Override
      public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes)
      {
        authorization.set(context.httpRequest().firstMatchingHeader("Authorization").orElse(null));
        throw new AbortBeforeTransmission();
      }
    };

    try (S3Client client = S3Client.builder()
                                   .region(Region.US_EAST_1)
                                   .credentialsProvider(
                                       StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "secret"))
                                   )
                                   .overrideConfiguration(c -> c.addExecutionInterceptor(captureAndAbort))
                                   .build()) {
      // the SDK may wrap the interceptor's exception, so only the captured header is asserted
      Assertions.assertThrows(
          RuntimeException.class,
          () -> client.headObject(b -> b.bucket(MRAP_ARN).key("key"))
      );
    }

    Assertions.assertNotNull(authorization.get());
    Assertions.assertTrue(
        authorization.get().startsWith("AWS4-ECDSA-P256-SHA256"),
        "expected SigV4A signature, got: " + authorization.get()
    );
  }

  private static class AbortBeforeTransmission extends RuntimeException
  {
  }
}
