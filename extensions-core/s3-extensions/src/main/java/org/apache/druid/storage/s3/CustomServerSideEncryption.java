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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;

class CustomServerSideEncryption implements ServerSideEncryption
{
  private final SSECustomerKey key;

  @JsonCreator
  CustomServerSideEncryption(@JacksonInject S3SSECustomConfig config)
  {
    this.key = new SSECustomerKey(config.getBase64EncodedKey());
  }

  @Override
  public PutObjectRequest decorate(PutObjectRequest request)
  {
    return request.withSSECustomerKey(key);
  }

  @Override
  public GetObjectRequest decorate(GetObjectRequest request)
  {
    return request.withSSECustomerKey(key);
  }

  @Override
  public GetObjectMetadataRequest decorate(GetObjectMetadataRequest request)
  {
    return request.withSSECustomerKey(key);
  }

  @Override
  public CopyObjectRequest decorate(CopyObjectRequest request)
  {
    // Note: users might want to use a different key when they copy existing objects. This might additionally need to
    // manage key history or support updating keys at run time, either of which requires a huge refactoring. We simply
    // don't support changing keys for now.
    return request.withSourceSSECustomerKey(key)
                  .withDestinationSSECustomerKey(key);
  }
}
