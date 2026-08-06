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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.File;

/**
 *
 */
@JsonTypeName(S3StorageDruidModule.SCHEME_S3_ZIP)
public class S3LoadSpec implements LoadSpec
{
  static final String RANGEABLE = "rangeable";

  private final String bucket;
  private final String key;

  /**
   * Stamped at push time when {@link S3DataSegmentPusher} writes a segment in a layout that supports byte-range reads
   * Only {@code Boolean.TRUE} enables {@link #openRangeReader()}; absence or {@code false} means full-download.
   */
  @Nullable
  private final Boolean rangeable;

  private final S3DataSegmentPuller puller;

  @JsonCreator
  public S3LoadSpec(
      @JacksonInject S3DataSegmentPuller puller,
      @JsonProperty(S3DataSegmentPuller.BUCKET) String bucket,
      @JsonProperty(S3DataSegmentPuller.KEY) String key,
      @JsonProperty(RANGEABLE) @Nullable Boolean rangeable
  )
  {
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(key);
    this.bucket = S3Utils.normalizeBucketName(bucket);
    this.key = key;
    this.rangeable = rangeable;
    this.puller = puller;
  }

  @Override
  public LoadSpecResult loadSegment(File outDir) throws SegmentLoadingException
  {
    return new LoadSpecResult(puller.getSegmentFiles(new CloudObjectLocation(bucket, key), outDir).size());
  }

  /**
   * Returns a {@link S3SegmentRangeReader} when the segment was stamped {@link #rangeable} {@code true} at push time
   * and isn't a zip; otherwise {@code null}.
   */
  @Nullable
  @Override
  public SegmentRangeReader openRangeReader()
  {
    if (CompressionUtils.isZip(key) || !Boolean.TRUE.equals(rangeable)) {
      return null;
    }
    return new S3SegmentRangeReader(puller.s3Client, bucket, key);
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

  /**
   * Returns the range-reads-supported flag stamped at push time, or {@code null} for legacy segments pushed before
   * this field existed (which will load via the full-download path).
   */
  @JsonProperty(RANGEABLE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Boolean getRangeable()
  {
    return rangeable;
  }
}
