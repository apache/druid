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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Md5Utils;

import javax.crypto.AEADBadTagException;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class S3UtilsTest
{
  @Test
  public void testConfigureLegacyMd5Disabled()
  {
    final S3ClientBuilder s3ClientBuilder = S3Client.builder();

    S3Utils.configureLegacyMd5(new AWSClientConfig(), s3ClientBuilder);

    Assertions.assertFalse(
        s3ClientBuilder.plugins().stream().anyMatch(LegacyMd5Plugin.class::isInstance)
    );
  }

  @Test
  public void testConfigureLegacyMd5EnabledForSyncAndAsyncClients()
  {
    final AWSClientConfig clientConfig = EasyMock.createMock(AWSClientConfig.class);
    EasyMock.expect(clientConfig.isEnableLegacyMd5()).andReturn(true).once();
    EasyMock.replay(clientConfig);
    final S3ClientBuilder s3ClientBuilder = S3Client.builder();
    final S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder();

    S3Utils.configureLegacyMd5(clientConfig, s3ClientBuilder, s3AsyncClientBuilder);

    Assertions.assertEquals(1, s3ClientBuilder.plugins().size());
    Assertions.assertEquals(1, s3AsyncClientBuilder.plugins().size());
    Assertions.assertInstanceOf(LegacyMd5Plugin.class, s3ClientBuilder.plugins().get(0));
    Assertions.assertInstanceOf(LegacyMd5Plugin.class, s3AsyncClientBuilder.plugins().get(0));
    try (
        final S3Client s3Client = s3ClientBuilder
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .build();
        final S3AsyncClient s3AsyncClient = s3AsyncClientBuilder
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .build()
    ) {
      Assertions.assertSame(
          RequestChecksumCalculation.WHEN_REQUIRED,
          s3Client.serviceClientConfiguration().requestChecksumCalculation()
      );
      Assertions.assertSame(
          RequestChecksumCalculation.WHEN_REQUIRED,
          s3AsyncClient.serviceClientConfiguration().requestChecksumCalculation()
      );
    }
    EasyMock.verify(clientConfig);
  }

  @Test
  public void testConfigureLegacyMd5UsesMd5ForRequiredChecksumsOnly() throws IOException
  {
    final AWSClientConfig clientConfig = EasyMock.createMock(AWSClientConfig.class);
    EasyMock.expect(clientConfig.isEnableLegacyMd5()).andReturn(true).once();
    EasyMock.replay(clientConfig);
    final List<HttpExecuteRequest> requests = new ArrayList<>();
    final SdkHttpClient httpClient = new SdkHttpClient()
    {
      @Override
      public ExecutableHttpRequest prepareRequest(final HttpExecuteRequest request)
      {
        requests.add(request);
        return new ExecutableHttpRequest()
        {
          @Override
          public HttpExecuteResponse call()
          {
            return HttpExecuteResponse.builder()
                                      .response(SdkHttpResponse.builder().statusCode(200).build())
                                      .build();
          }

          @Override
          public void abort()
          {
            // Nothing to abort in this test client.
          }
        };
      }

      @Override
      public void close()
      {
        // No resources to close in this test client.
      }
    };
    final S3ClientBuilder s3ClientBuilder = S3Client.builder()
                                                    .credentialsProvider(AnonymousCredentialsProvider.create())
                                                    .region(Region.US_EAST_1)
                                                    .endpointOverride(URI.create("http://localhost"))
                                                    .forcePathStyle(true)
                                                    .httpClient(httpClient);
    S3Utils.configureLegacyMd5(clientConfig, s3ClientBuilder);

    try (final S3Client s3Client = s3ClientBuilder.build()) {
      s3Client.putObject(
          PutObjectRequest.builder().bucket("bucket").key("key").build(),
          RequestBody.fromString("payload")
      );
      s3Client.deleteObjects(
          DeleteObjectsRequest.builder()
                              .bucket("bucket")
                              .delete(Delete.builder().objects(ObjectIdentifier.builder().key("key").build()).build())
                              .build()
      );
    }

    Assertions.assertEquals(2, requests.size());
    Assertions.assertTrue(requests.get(0).httpRequest().firstMatchingHeader("Content-MD5").isEmpty());
    Assertions.assertTrue(requests.get(0).httpRequest().firstMatchingHeader("x-amz-checksum-crc32").isEmpty());
    final HttpExecuteRequest deleteObjectsRequest = requests.get(1);
    Assertions.assertEquals(
        Md5Utils.md5AsBase64(deleteObjectsRequest.contentStreamProvider().orElseThrow().newStream()),
        deleteObjectsRequest.httpRequest().firstMatchingHeader("Content-MD5").orElseThrow()
    );
    EasyMock.verify(clientConfig);
  }

  @Test
  public void testRetryWithIOExceptions()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              throw new IOException("hmm");
            },
            maxRetries
        ));
    Assertions.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithSslExceptionWrappingAeadBadTag()
  {
    // Transient TLS "Tag mismatch!" should be retried, not treated as terminal. See issue #19616.
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
        SSLException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              throw new SSLException("Tag mismatch!", new AEADBadTagException("Tag mismatch!"));
            },
            maxRetries
        ));
    Assertions.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith4XXErrors()
  {
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
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
    Assertions.assertEquals(1, count.get());
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
    Assertions.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsExceedingMaxRetries()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
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
    Assertions.assertEquals(maxRetries, count.get());
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
    Assertions.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithAsyncCredentialProviderChainException() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            throw new CompletionException(
                SdkClientException.builder()
                                  .message(
                                      "Unable to load credentials from any of the providers in the chain "
                                      + "AwsCredentialsProviderChain"
                                  )
                                  .build()
            );
          }
        },
        maxRetries
    );
    Assertions.assertEquals(maxRetries, count.get());
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
    Assertions.assertEquals(maxRetries, count.get());
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
    Assertions.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testNoRetryWithS3InternalErrorNon200Status()
  {
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
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
    Assertions.assertEquals(1, count.get());
  }

  @Test
  public void testNoRetryWithS3SlowDownNon200Status()
  {
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
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
    Assertions.assertEquals(1, count.get());
  }

  @Test
  public void testRetryWithS3Status200ButDifferentError()
  {
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(
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
    Assertions.assertEquals(1, count.get());
  }

  @Test
  public void testDeleteBucketKeysSuccess() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);
    DeleteObjectsResponse successResponse = DeleteObjectsResponse.builder().build();
    EasyMock.expect(s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class)))
            .andReturn(successResponse)
            .once();
    EasyMock.replay(s3Client);

    List<ObjectIdentifier> keys = List.of(
        ObjectIdentifier.builder().key("a").build(),
        ObjectIdentifier.builder().key("b").build()
    );
    S3Utils.deleteBucketKeys(s3Client, "bucket", keys, 3);
    EasyMock.verify(s3Client);
  }

  @Test
  public void testDeleteBucketKeysRetriesOnlyFailedKeys() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);

    // First call: key "b" fails
    DeleteObjectsResponse firstResponse = DeleteObjectsResponse.builder()
        .errors(S3Error.builder().key("b").code("InternalError").message("err").build())
        .build();
    // Second call (retry): only "b" is sent, succeeds
    DeleteObjectsResponse secondResponse = DeleteObjectsResponse.builder().build();

    Capture<DeleteObjectsRequest> capturedRequests = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(s3Client.deleteObjects(EasyMock.capture(capturedRequests)))
            .andReturn(firstResponse)
            .andReturn(secondResponse);
    EasyMock.replay(s3Client);

    List<ObjectIdentifier> keys = List.of(
        ObjectIdentifier.builder().key("a").build(),
        ObjectIdentifier.builder().key("b").build()
    );
    S3Utils.deleteBucketKeys(s3Client, "bucket", keys, 3);
    EasyMock.verify(s3Client);

    // First request should have both keys
    List<String> firstKeys = capturedRequests.getValues().get(0).delete().objects()
                                 .stream().map(ObjectIdentifier::key).collect(Collectors.toList());
    Assertions.assertEquals(List.of("a", "b"), firstKeys);

    // Second request should only have the failed key
    List<String> secondKeys = capturedRequests.getValues().get(1).delete().objects()
                                  .stream().map(ObjectIdentifier::key).collect(Collectors.toList());
    Assertions.assertEquals(List.of("b"), secondKeys);
  }

  @Test
  public void testDeleteBucketKeysThrowsAfterAllRetriesExhausted()
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);

    DeleteObjectsResponse errorResponse = DeleteObjectsResponse.builder()
        .errors(S3Error.builder().key("a").code("InternalError").message("err").build())
        .build();
    EasyMock.expect(s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class)))
            .andReturn(errorResponse)
            .anyTimes();
    EasyMock.replay(s3Client);

    List<ObjectIdentifier> keys = List.of(ObjectIdentifier.builder().key("a").build());
    S3MultiObjectDeleteException thrown = Assertions.assertThrows(
        S3MultiObjectDeleteException.class,
        () -> S3Utils.deleteBucketKeys(s3Client, "bucket", keys, 2)
    );
    Assertions.assertEquals(1, thrown.getErrors().size());
    Assertions.assertEquals("a", thrown.getErrors().get(0).key());
    EasyMock.verify(s3Client);
  }

  @Test
  public void testDeleteBucketKeysPartialFailureRetriesAlsoFail()
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);

    // First call: key "b" fails; second call (retry of "b"): still fails
    DeleteObjectsResponse firstResponse = DeleteObjectsResponse.builder()
        .errors(S3Error.builder().key("b").code("InternalError").message("err").build())
        .build();
    DeleteObjectsResponse retryResponse = DeleteObjectsResponse.builder()
        .errors(S3Error.builder().key("b").code("InternalError").message("err").build())
        .build();

    EasyMock.expect(s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class)))
            .andReturn(firstResponse)
            .andReturn(retryResponse);
    EasyMock.replay(s3Client);

    List<ObjectIdentifier> keys = List.of(
        ObjectIdentifier.builder().key("a").build(),
        ObjectIdentifier.builder().key("b").build()
    );
    S3MultiObjectDeleteException thrown = Assertions.assertThrows(
        S3MultiObjectDeleteException.class,
        () -> S3Utils.deleteBucketKeys(s3Client, "bucket", keys, 1)
    );
    Assertions.assertEquals(1, thrown.getErrors().size());
    Assertions.assertEquals("b", thrown.getErrors().get(0).key());
    EasyMock.verify(s3Client);
  }

  @Test
  public void testRetryWithS3MultiObjectDeleteException() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "success";
          } else {
            throw new S3MultiObjectDeleteException(
                List.of(S3Error.builder().key("x").code("InternalError").message("err").build())
            );
          }
        },
        maxRetries
    );
    Assertions.assertEquals(maxRetries, count.get());
  }

  private static final ObjectMapper JSON = new ObjectMapper();

  private static AWSEndpointConfig endpointWith(String json) throws IOException
  {
    return JSON.readValue(json, AWSEndpointConfig.class);
  }

  @Test
  public void testUseHttpsNullClientConfigSchemelessEndpointReturnsTrue() throws IOException
  {
    Assertions.assertTrue(S3Utils.useHttps(null, endpointWith("{\"url\":\"s3.example.com\"}")));
  }

  @Test
  public void testUseHttpsNullClientConfigHttpEndpointReturnsFalse() throws IOException
  {
    Assertions.assertFalse(S3Utils.useHttps(null, endpointWith("{\"url\":\"http://s3.example.com\"}")));
  }

  @Test
  public void testUseHttpsNullClientConfigHttpsEndpointReturnsTrue() throws IOException
  {
    Assertions.assertTrue(S3Utils.useHttps(null, endpointWith("{\"url\":\"https://s3.example.com\"}")));
  }

  @Test
  public void testUseHttpsNullClientConfigNullEndpointUrlReturnsTrue() throws IOException
  {
    Assertions.assertTrue(S3Utils.useHttps(null, new AWSEndpointConfig()));
  }

  @Test
  public void testUseHttpsDefaultClientConfigSchemelessEndpointReturnsTrue() throws IOException
  {
    // Sanity check: default AWSClientConfig protocol is "https"; schemeless URL inherits "https".
    Assertions.assertTrue(S3Utils.useHttps(new AWSClientConfig(), endpointWith("{\"url\":\"s3.example.com\"}")));
  }
}
