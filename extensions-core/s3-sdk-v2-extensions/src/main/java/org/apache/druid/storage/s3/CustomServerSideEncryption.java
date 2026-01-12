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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

class CustomServerSideEncryption implements ServerSideEncryption
{
  private static final String SSE_ALGORITHM = "AES256";
  private final String base64EncodedKey;
  private final String base64EncodedKeyMd5;

  @JsonCreator
  CustomServerSideEncryption(@JacksonInject S3SSECustomConfig config)
  {
    this.base64EncodedKey = config.getBase64EncodedKey();
    this.base64EncodedKeyMd5 = computeMd5(base64EncodedKey);
  }

  private static String computeMd5(String base64EncodedKey)
  {
    try {
      byte[] keyBytes = Base64.getDecoder().decode(base64EncodedKey);
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] md5Bytes = md.digest(keyBytes);
      return Base64.getEncoder().encodeToString(md5Bytes);
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm not available", e);
    }
  }

  @Override
  public PutObjectRequest decorate(PutObjectRequest request)
  {
    return request.toBuilder()
        .sseCustomerAlgorithm(SSE_ALGORITHM)
        .sseCustomerKey(base64EncodedKey)
        .sseCustomerKeyMD5(base64EncodedKeyMd5)
        .build();
  }

  @Override
  public GetObjectRequest decorate(GetObjectRequest request)
  {
    return request.toBuilder()
        .sseCustomerAlgorithm(SSE_ALGORITHM)
        .sseCustomerKey(base64EncodedKey)
        .sseCustomerKeyMD5(base64EncodedKeyMd5)
        .build();
  }

  @Override
  public HeadObjectRequest decorate(HeadObjectRequest request)
  {
    return request.toBuilder()
        .sseCustomerAlgorithm(SSE_ALGORITHM)
        .sseCustomerKey(base64EncodedKey)
        .sseCustomerKeyMD5(base64EncodedKeyMd5)
        .build();
  }

  @Override
  public CopyObjectRequest decorate(CopyObjectRequest request)
  {
    // Note: users might want to use a different key when they copy existing objects. This might additionally need to
    // manage key history or support updating keys at run time, either of which requires a huge refactoring. We simply
    // don't support changing keys for now.
    return request.toBuilder()
        .copySourceSSECustomerAlgorithm(SSE_ALGORITHM)
        .copySourceSSECustomerKey(base64EncodedKey)
        .copySourceSSECustomerKeyMD5(base64EncodedKeyMd5)
        .sseCustomerAlgorithm(SSE_ALGORITHM)
        .sseCustomerKey(base64EncodedKey)
        .sseCustomerKeyMD5(base64EncodedKeyMd5)
        .build();
  }
}
