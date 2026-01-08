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

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class S3UtilsTest
{
  @Test
  public void testRetryWithIOExceptions()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              throw new IOException("hmm");
            },
            maxRetries
        ));
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith4XXErrors()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              if (count.incrementAndGet() >= 2) {
                return "hey";
              } else {
                S3Exception s3Exception = (S3Exception) S3Exception.builder()
                    .message("a 403 s3 exception")
                    .statusCode(403)
                    .build();
                throw new IOException(s3Exception);
              }
            },
            3
        ));
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsNotExceedingMaxRetries() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            S3Exception s3Exception = (S3Exception) S3Exception.builder()
                .message("a 5xx s3 exception")
                .statusCode(500)
                .build();
            throw new IOException(s3Exception);
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsExceedingMaxRetries()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              if (count.incrementAndGet() > maxRetries) {
                return "hey";
              } else {
                S3Exception s3Exception = (S3Exception) S3Exception.builder()
                    .message("a 5xx s3 exception")
                    .statusCode(500)
                    .build();
                throw new IOException(s3Exception);
              }
            },
            maxRetries
        )
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithSdkClientException() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            throw SdkClientException.builder()
                .message(
                    "Unable to find a region via the region provider chain. "
                    + "Must provide an explicit region in the builder or setup environment to supply a region."
                )
                .build();
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithS3InternalError() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "donezo";
          } else {
            S3Exception s3Exception = (S3Exception) S3Exception.builder()
                .message("We encountered an internal error. Please try again. (Service: Amazon S3; Status Code: 200; Error Code: InternalError; Request ID: some-id)")
                .statusCode(200)
                .build();
            throw s3Exception;
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithS3SlowDown() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "success";
          } else {
            S3Exception s3Exception = (S3Exception) S3Exception.builder()
                .message("Please reduce your request rate. SlowDown")
                .statusCode(200)
                .build();
            throw s3Exception;
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testNoRetryWithS3InternalErrorNon200Status()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        Exception.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              S3Exception s3Exception = (S3Exception) S3Exception.builder()
                  .message("InternalError occurred")
                  .statusCode(403)
                  .build();
              throw s3Exception;
            },
            3
        )
    );
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testNoRetryWithS3SlowDownNon200Status()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        Exception.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              S3Exception s3Exception = (S3Exception) S3Exception.builder()
                  .message("SlowDown message")
                  .statusCode(404)
                  .build();
              throw s3Exception;
            },
            3
        )
    );
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testRetryWithS3Status200ButDifferentError()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        Exception.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              S3Exception s3Exception = (S3Exception) S3Exception.builder()
                  .message("Some other error message")
                  .statusCode(200)
                  .build();
              throw s3Exception;
            },
            3
        )
    );
    Assert.assertEquals(1, count.get());
  }
}
