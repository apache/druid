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

class CustomServerSideEncryption implements ServerSideEncryption
{
  private static final String ALGORITHM = "AES256";
  private final String base64EncodedKey;

  @JsonCreator
  CustomServerSideEncryption(@JacksonInject S3SSECustomConfig config)
  {
    this.base64EncodedKey = config.getBase64EncodedKey();
  }

  @Override
  public PutObjectRequest.Builder decorate(PutObjectRequest.Builder builder)
  {
    return builder
        .sseCustomerAlgorithm(ALGORITHM)
        .sseCustomerKey(base64EncodedKey);
  }

  @Override
  public GetObjectRequest.Builder decorate(GetObjectRequest.Builder builder)
  {
    return builder
        .sseCustomerAlgorithm(ALGORITHM)
        .sseCustomerKey(base64EncodedKey);
  }

  @Override
  public HeadObjectRequest.Builder decorate(HeadObjectRequest.Builder builder)
  {
    return builder
        .sseCustomerAlgorithm(ALGORITHM)
        .sseCustomerKey(base64EncodedKey);
  }

  @Override
  public CopyObjectRequest.Builder decorate(CopyObjectRequest.Builder builder)
  {
    // Note: users might want to use a different key when they copy existing objects. This might additionally need to
    // manage key history or support updating keys at run time, either of which requires a huge refactoring. We simply
    // don't support changing keys for now.
    return builder
        .sseCustomerAlgorithm(ALGORITHM)
        .sseCustomerKey(base64EncodedKey)
        .copySourceSSECustomerAlgorithm(ALGORITHM)
        .copySourceSSECustomerKey(base64EncodedKey);
  }
}
