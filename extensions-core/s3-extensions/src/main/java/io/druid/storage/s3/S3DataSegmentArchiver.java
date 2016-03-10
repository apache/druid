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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;


public class S3DataSegmentArchiver extends S3DataSegmentMover implements DataSegmentArchiver
{
  private final S3DataSegmentArchiverConfig archiveConfig;
  private final S3DataSegmentPusherConfig restoreConfig;

  @Inject
  public S3DataSegmentArchiver(
    RestS3Service s3Client,
    S3DataSegmentArchiverConfig archiveConfig,
    S3DataSegmentPusherConfig restoreConfig
  )
  {
    super(s3Client, restoreConfig);
    this.archiveConfig = archiveConfig;
    this.restoreConfig = restoreConfig;
  }

  @Override
  public DataSegment archive(DataSegment segment) throws SegmentLoadingException
  {
    String targetS3Bucket = archiveConfig.getArchiveBucket();
    String targetS3BaseKey = archiveConfig.getArchiveBaseKey();

    return move(
        segment,
        ImmutableMap.<String, Object>of(
            "bucket", targetS3Bucket,
            "baseKey", targetS3BaseKey
        )
    );
  }

  @Override
  public DataSegment restore(DataSegment segment) throws SegmentLoadingException
  {
    String targetS3Bucket = restoreConfig.getBucket();
    String targetS3BaseKey = restoreConfig.getBaseKey();

    return move(
        segment,
        ImmutableMap.<String, Object>of(
            "bucket", targetS3Bucket,
            "baseKey", targetS3BaseKey
        )
    );
  }
}
