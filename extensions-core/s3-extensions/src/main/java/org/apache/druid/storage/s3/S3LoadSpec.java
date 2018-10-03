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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;

import java.io.File;

/**
 *
 */
@JsonTypeName(S3StorageDruidModule.SCHEME)
public class S3LoadSpec implements LoadSpec
{
  private final String bucket;
  private final String key;

  private final S3DataSegmentPuller puller;

  @JsonCreator
  public S3LoadSpec(
      @JacksonInject S3DataSegmentPuller puller,
      @JsonProperty(S3DataSegmentPuller.BUCKET) String bucket,
      @JsonProperty(S3DataSegmentPuller.KEY) String key
  )
  {
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(key);
    this.bucket = bucket;
    this.key = key;
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(new S3DataSegmentPuller.S3Coords(bucket, key), outDir).size());
  }

  @JsonProperty(S3DataSegmentPuller.BUCKET)
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty(S3DataSegmentPuller.KEY)
  public String getKey()
  {
    return key;
  }
}
