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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.IOException;

/**
 * Multipart uploads (used by MSQ durable storage and export) must carry the same encryption settings as single PUTs.
 */
public class ServerSideEncryptionTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final String KEY_ID = "arn:aws:kms:us-east-1:123456789012:key/test-key";
  private static final String CUSTOM_KEY = "dGVzdC1rZXktdGVzdC1rZXktdGVzdC1rZXktMTIzNA==";

  @Test
  public void testKmsDecoratesCreateMultipartUploadWithKeyId() throws IOException
  {
    final KmsServerSideEncryption sse = new KmsServerSideEncryption(
        MAPPER.readValue("{\"keyId\":\"" + KEY_ID + "\"}", S3SSEKmsConfig.class)
    );

    final CreateMultipartUploadRequest request = sse.decorate(multipartBuilder()).build();
    Assertions.assertEquals(software.amazon.awssdk.services.s3.model.ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
    Assertions.assertEquals(KEY_ID, request.ssekmsKeyId());

    // Must match what a single PUT gets.
    final PutObjectRequest putRequest = sse.decorate(PutObjectRequest.builder().bucket("b").key("k")).build();
    Assertions.assertEquals(putRequest.serverSideEncryption(), request.serverSideEncryption());
    Assertions.assertEquals(putRequest.ssekmsKeyId(), request.ssekmsKeyId());
  }

  @Test
  public void testKmsWithoutKeyIdDecoratesCreateMultipartUpload() throws IOException
  {
    final KmsServerSideEncryption sse = new KmsServerSideEncryption(MAPPER.readValue("{}", S3SSEKmsConfig.class));

    final CreateMultipartUploadRequest request = sse.decorate(multipartBuilder()).build();
    Assertions.assertEquals(software.amazon.awssdk.services.s3.model.ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
    Assertions.assertNull(request.ssekmsKeyId());
  }

  @Test
  public void testS3DecoratesCreateMultipartUpload()
  {
    final CreateMultipartUploadRequest request = new S3ServerSideEncryption().decorate(multipartBuilder()).build();
    Assertions.assertEquals(software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256, request.serverSideEncryption());
  }

  @Test
  public void testCustomDecoratesCreateMultipartUploadAndUploadPart() throws IOException
  {
    final CustomServerSideEncryption sse = new CustomServerSideEncryption(
        MAPPER.readValue("{\"base64EncodedKey\":\"" + CUSTOM_KEY + "\"}", S3SSECustomConfig.class)
    );

    final CreateMultipartUploadRequest createRequest = sse.decorate(multipartBuilder()).build();
    Assertions.assertEquals("AES256", createRequest.sseCustomerAlgorithm());
    Assertions.assertEquals(CUSTOM_KEY, createRequest.sseCustomerKey());

    final UploadPartRequest partRequest = sse.decorate(
        UploadPartRequest.builder().bucket("b").key("k").uploadId("u").partNumber(1)
    ).build();
    Assertions.assertEquals("AES256", partRequest.sseCustomerAlgorithm());
    Assertions.assertEquals(CUSTOM_KEY, partRequest.sseCustomerKey());
  }

  @Test
  public void testNoopLeavesCreateMultipartUploadUntouched()
  {
    final CreateMultipartUploadRequest request = new NoopServerSideEncryption().decorate(multipartBuilder()).build();
    Assertions.assertNull(request.serverSideEncryption());
    Assertions.assertNull(request.ssekmsKeyId());
  }

  private static CreateMultipartUploadRequest.Builder multipartBuilder()
  {
    return CreateMultipartUploadRequest.builder().bucket("b").key("k");
  }
}
