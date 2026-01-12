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

import java.time.Instant;

/**
 * Wrapper class that holds an S3Object along with its bucket name.
 * In AWS SDK v2, S3Object doesn't contain bucket information, so this class
 * provides a way to associate the bucket with the object.
 */
public class S3ObjectSummary
{
  private final String bucket;
  private final S3Object s3Object;

  public S3ObjectSummary(String bucket, S3Object s3Object)
  {
    this.bucket = bucket;
    this.s3Object = s3Object;
  }

  public String bucket()
  {
    return bucket;
  }

  public String key()
  {
    return s3Object.key();
  }

  public long size()
  {
    return s3Object.size();
  }

  public String eTag()
  {
    return s3Object.eTag();
  }

  public Instant lastModified()
  {
    return s3Object.lastModified();
  }

  public S3Object getS3Object()
  {
    return s3Object;
  }
}
