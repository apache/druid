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

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;

import javax.annotation.Nullable;

class KmsServerSideEncryption implements ServerSideEncryption
{
  @Nullable
  private final String keyId;

  @JsonCreator
  KmsServerSideEncryption(@JacksonInject S3SSEKmsConfig config)
  {
    this.keyId = config.getKeyId();
  }

  @Override
  public PutObjectRequest decorate(PutObjectRequest request)
  {
    return request.withSSEAwsKeyManagementParams(
        keyId == null ? new SSEAwsKeyManagementParams() : new SSEAwsKeyManagementParams(keyId)
    );
  }

  @Override
  public CopyObjectRequest decorate(CopyObjectRequest request)
  {
    return request.withSSEAwsKeyManagementParams(
        keyId == null ? new SSEAwsKeyManagementParams() : new SSEAwsKeyManagementParams(keyId)
    );
  }
}
