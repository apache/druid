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
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.util.Map;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentMover.class);

  private final RestS3Service s3Client;

  @Inject
  public S3DataSegmentMover(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = S3Utils.descriptorPathForSegmentPath(s3Path);

      final String targetS3Bucket = MapUtils.getString(targetLoadSpec, "bucket");
      final String targetS3BaseKey = MapUtils.getString(targetLoadSpec, "baseKey");

      final String targetS3Path = S3Utils.constructSegmentPath(targetS3BaseKey, segment);
      String targetS3DescriptorPath = S3Utils.descriptorPathForSegmentPath(targetS3Path);

      if (targetS3Bucket.isEmpty()) {
        throw new SegmentLoadingException("Target S3 bucket is not specified");
      }
      if (targetS3Path.isEmpty()) {
        throw new SegmentLoadingException("Target S3 baseKey is not specified");
      }

      if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        log.info(
            "Moving index file[s3://%s/%s] to [s3://%s/%s]",
            s3Bucket,
            s3Path,
            targetS3Bucket,
            targetS3Path
        );
        s3Client.moveObject(s3Bucket, s3Path, targetS3Bucket, new S3Object(targetS3Path), false);
      }
      if (s3Client.isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        log.info(
            "Moving descriptor file[s3://%s/%s] to [s3://%s/%s]",
            s3Bucket,
            s3DescriptorPath,
            targetS3Bucket,
            targetS3DescriptorPath
        );
        s3Client.moveObject(s3Bucket, s3DescriptorPath, targetS3Bucket, new S3Object(targetS3DescriptorPath), false);
      }

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
                      .putAll(loadSpec)
                      .put("bucket", targetS3Bucket)
                      .put("key", targetS3Path)
                      .build()
      );
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]", segment.getIdentifier());
    }
  }
}
