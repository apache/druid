/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.elasticbeanstalk.model.S3LocationNotInServiceRegionException;
import com.amazonaws.services.s3.AmazonS3URI;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.metamx.common.CompressionUtils;
import com.metamx.common.FileUtils;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.RetryUtils;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.LoadSpec;
import io.druid.segment.loading.SegmentLoadingException;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;

import javax.swing.text.Segment;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@JsonTypeName(S3StorageDruidModule.SCHEME)
public class S3LoadSpec implements LoadSpec
{
  @JsonProperty(S3DataSegmentPuller.BUCKET)
  private final String bucket;
  @JsonProperty(S3DataSegmentPuller.KEY)
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
}
