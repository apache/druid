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

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.util.Map;
import java.util.concurrent.Callable;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentMover.class);

  private final RestS3Service s3Client;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentMover(
      RestS3Service s3Client,
      S3DataSegmentPusherConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
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

      safeMove(s3Bucket, s3Path, targetS3Bucket, targetS3Path);
      safeMove(s3Bucket, s3DescriptorPath, targetS3Bucket, targetS3DescriptorPath);

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
                      .putAll(
                          Maps.filterKeys(
                              loadSpec, new Predicate<String>()
                          {
                            @Override
                            public boolean apply(String input)
                            {
                              return !(input.equals("bucket") || input.equals("key"));
                            }
                          }
                          )
                      )
                      .put("bucket", targetS3Bucket)
                      .put("key", targetS3Path)
                      .build()
      );
    }
    catch (ServiceException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]", segment.getIdentifier());
    }
  }

  private void safeMove(
      final String s3Bucket,
      final String s3Path,
      final String targetS3Bucket,
      final String targetS3Path
  ) throws ServiceException, SegmentLoadingException
  {
    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
                if (s3Bucket.equals(targetS3Bucket) && s3Path.equals(targetS3Path)) {
                  log.info("No need to move file[s3://%s/%s] onto itself", s3Bucket, s3Path);
                } else {
                  log.info(
                      "Moving file[s3://%s/%s] to [s3://%s/%s]",
                      s3Bucket,
                      s3Path,
                      targetS3Bucket,
                      targetS3Path
                  );
                  final S3Object target = new S3Object(targetS3Path);
                  if(!config.getDisableAcl()) {
                    target.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
                  }
                  s3Client.moveObject(s3Bucket, s3Path, targetS3Bucket, target, false);
                }
              } else {
                // ensure object exists in target location
                if (s3Client.isObjectInBucket(targetS3Bucket, targetS3Path)) {
                  log.info(
                      "Not moving file [s3://%s/%s], already present in target location [s3://%s/%s]",
                      s3Bucket, s3Path,
                      targetS3Bucket, targetS3Path
                  );
                } else {
                  throw new SegmentLoadingException(
                      "Unable to move file [s3://%s/%s] to [s3://%s/%s], not present in either source or target location",
                      s3Bucket, s3Path,
                      targetS3Bucket, targetS3Path
                  );
                }
              }
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, ServiceException.class);
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      throw Throwables.propagate(e);
    }
  }
}
