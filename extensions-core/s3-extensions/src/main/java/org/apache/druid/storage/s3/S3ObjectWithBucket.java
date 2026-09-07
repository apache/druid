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

import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Pairs an {@link S3Object} with its bucket name.
 * In AWS SDK v2, {@link S3Object} does not carry the bucket name, so callers that need it must track it separately.
 */
public class S3ObjectWithBucket
{
  private final String bucket;
  private final S3Object s3Object;

  public S3ObjectWithBucket(String bucket, S3Object s3Object)
  {
    this.bucket = bucket;
    this.s3Object = s3Object;
  }

  public String getBucket()
  {
    return bucket;
  }

  public String getKey()
  {
    return s3Object.key();
  }

  public long getSize()
  {
    return s3Object.size();
  }

  public S3Object getS3Object()
  {
    return s3Object;
  }
}
