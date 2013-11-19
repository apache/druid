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

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.util.Map;

/**
 */
public class S3DataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(S3DataSegmentKiller.class);

  private final RestS3Service s3Client;
  private final S3DataSegmentKillerConfig config;

  @Inject
  public S3DataSegmentKiller(
      RestS3Service s3Client,
      S3DataSegmentKillerConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";

      final String s3ArchiveBucket = config.getArchiveBucket();

      if(config.isArchive() && s3ArchiveBucket.isEmpty()) {
        log.warn("S3 archive bucket not specified, refusing to delete segment [s3://%s/%s]", s3Bucket, s3Path);
        return;
      }

      if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        if (config.isArchive()) {
          log.info("Archiving index file[s3://%s/%s] to [s3://%s/%s]",
                   s3Bucket,
                   s3Path,
                   s3ArchiveBucket,
                   s3Path
          );
          s3Client.moveObject(s3Bucket, s3Path, s3ArchiveBucket, new S3Object(s3Path), false);
        } else {
          log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
          s3Client.deleteObject(s3Bucket, s3Path);
        }
      }
      if (s3Client.isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        if (config.isArchive()) {
          log.info(
              "Archiving descriptor file[s3://%s/%s] to [s3://%s/%s]",
              s3Bucket,
              s3DescriptorPath,
              s3ArchiveBucket,
              s3DescriptorPath
          );
          s3Client.moveObject(s3Bucket, s3DescriptorPath, s3ArchiveBucket, new S3Object(s3DescriptorPath), false);
        } else {
          log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
          s3Client.deleteObject(s3Bucket, s3DescriptorPath);
        }
      }
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]", segment.getIdentifier());
    }
  }
}
