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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

class S3ServerSideEncryption implements ServerSideEncryption
{
  @Override
  public PutObjectRequest decorate(PutObjectRequest request)
  {
    final ObjectMetadata objectMetadata = request.getMetadata() == null ?
                                          new ObjectMetadata() :
                                          request.getMetadata().clone();
    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    return request.withMetadata(objectMetadata);
  }

  @Override
  public CopyObjectRequest decorate(CopyObjectRequest request)
  {
    final ObjectMetadata objectMetadata = request.getNewObjectMetadata() == null ?
                                          new ObjectMetadata() :
                                          request.getNewObjectMetadata().clone();
    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    return request.withNewObjectMetadata(objectMetadata);
  }
}
