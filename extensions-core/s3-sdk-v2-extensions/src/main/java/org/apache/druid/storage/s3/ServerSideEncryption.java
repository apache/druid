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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/**
 * Server-side encryption decorator for Amazon S3.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "noop", value = NoopServerSideEncryption.class),
    @Type(name = "s3", value = S3ServerSideEncryption.class),
    @Type(name = "kms", value = KmsServerSideEncryption.class),
    @Type(name = "custom", value = CustomServerSideEncryption.class)
})
public interface ServerSideEncryption
{
  default PutObjectRequest decorate(PutObjectRequest request)
  {
    return request;
  }

  default GetObjectRequest decorate(GetObjectRequest request)
  {
    return request;
  }

  default HeadObjectRequest decorate(HeadObjectRequest request)
  {
    return request;
  }

  default CopyObjectRequest decorate(CopyObjectRequest request)
  {
    return request;
  }

  default CreateMultipartUploadRequest decorate(CreateMultipartUploadRequest request)
  {
    return request;
  }

  default UploadPartRequest decorate(UploadPartRequest request)
  {
    return request;
  }

}
