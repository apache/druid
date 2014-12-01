/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
