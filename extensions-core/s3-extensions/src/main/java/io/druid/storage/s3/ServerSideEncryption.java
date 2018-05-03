/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.storage.s3;

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

/**
 * Server-side encryption decorator for Amazon S3.
 */
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

  default GetObjectMetadataRequest decorate(GetObjectMetadataRequest request)
  {
    return request;
  }

  default CopyObjectRequest decorate(CopyObjectRequest request)
  {
    return request;
  }
}
