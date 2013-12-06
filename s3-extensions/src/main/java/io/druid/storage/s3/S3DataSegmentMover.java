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
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.util.Map;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);

  private final RestS3Service s3Client;
  private final S3DataSegmentMoverConfig config;

  @Inject
  public S3DataSegmentMover(
      RestS3Service s3Client,
      S3DataSegmentMoverConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
  }

  @Override
  public DataSegment move(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";

      final String s3ArchiveBucket = config.getArchiveBucket();

      if (s3ArchiveBucket.isEmpty()) {
        log.warn("S3 archive bucket not specified, refusing to move segment [s3://%s/%s]", s3Bucket, s3Path);
        return segment;
      }

      if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        log.info(
            "Moving index file[s3://%s/%s] to [s3://%s/%s]",
            s3Bucket,
            s3Path,
            s3ArchiveBucket,
            s3Path
        );
        s3Client.moveObject(s3Bucket, s3Path, s3ArchiveBucket, new S3Object(s3Path), false);
      }
      if (s3Client.isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        log.info(
            "Moving descriptor file[s3://%s/%s] to [s3://%s/%s]",
            s3Bucket,
            s3DescriptorPath,
            s3ArchiveBucket,
            s3DescriptorPath
        );
        s3Client.moveObject(s3Bucket, s3DescriptorPath, s3ArchiveBucket, new S3Object(s3DescriptorPath), false);
      }

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
                      .putAll(loadSpec)
                      .put("bucket", s3ArchiveBucket).build()
      );
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]", segment.getIdentifier());
    }
  }
}
