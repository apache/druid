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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.Map;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentMover.class);

  private final AmazonS3 s3Client;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentMover(
      AmazonS3 s3Client,
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

      final String targetS3Path = S3Utils.constructSegmentPath(
          targetS3BaseKey,
          DataSegmentPusher.getDefaultStorageDir(segment, false)
      );
      final String targetS3DescriptorPath = S3Utils.descriptorPathForSegmentPath(targetS3Path);

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
    catch (AmazonServiceException e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]: [%s]", segment.getIdentifier(), e);
    }
  }

  private void safeMove(
      final String s3Bucket,
      final String s3Path,
      final String targetS3Bucket,
      final String targetS3Path
  ) throws SegmentLoadingException
  {
    try {
      S3Utils.retryS3Operation(
          () -> {
            final String copyMsg = StringUtils.format(
                "[s3://%s/%s] to [s3://%s/%s]",
                s3Bucket,
                s3Path,
                targetS3Bucket,
                targetS3Path
            );
            try {
              selfCheckingMove(s3Bucket, targetS3Bucket, s3Path, targetS3Path, copyMsg);
              return null;
            }
            catch (AmazonServiceException | IOException | SegmentLoadingException e) {
              log.info(e, "Error while trying to move " + copyMsg);
              throw e;
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, AmazonServiceException.class);
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Copies an object and after that checks that the object is present at the target location, via a separate API call.
   * If it is not, an exception is thrown, and the object is not deleted at the old location. This "paranoic" check
   * is added after it was observed that S3 may report a successful move, and the object is not found at the target
   * location.
   */
  private void selfCheckingMove(
      String s3Bucket,
      String targetS3Bucket,
      String s3Path,
      String targetS3Path,
      String copyMsg
  ) throws IOException, SegmentLoadingException
  {
    if (s3Bucket.equals(targetS3Bucket) && s3Path.equals(targetS3Path)) {
      log.info("No need to move file[s3://%s/%s] onto itself", s3Bucket, s3Path);
      return;
    }
    if (s3Client.doesObjectExist(s3Bucket, s3Path)) {
      final ListObjectsV2Result listResult = s3Client.listObjectsV2(
          new ListObjectsV2Request()
              .withBucketName(s3Bucket)
              .withPrefix(s3Path)
              .withMaxKeys(1)
      );
      if (listResult.getKeyCount() == 0) {
        // should never happen
        throw new ISE("Unable to list object [s3://%s/%s]", s3Bucket, s3Path);
      }
      final S3ObjectSummary objectSummary = listResult.getObjectSummaries().get(0);
      if (objectSummary.getStorageClass() != null &&
          StorageClass.fromValue(StringUtils.toUpperCase(objectSummary.getStorageClass())).equals(StorageClass.Glacier)) {
        throw new AmazonServiceException(
            StringUtils.format(
                "Cannot move file[s3://%s/%s] of storage class glacier, skipping.",
                s3Bucket,
                s3Path
            )
        );
      } else {
        log.info("Moving file %s", copyMsg);
        final CopyObjectRequest copyRequest = new CopyObjectRequest(s3Bucket, s3Path, targetS3Bucket, targetS3Path);
        if (!config.getDisableAcl()) {
          copyRequest.setAccessControlList(S3Utils.grantFullControlToBucketOwner(s3Client, targetS3Bucket));
        }
        s3Client.copyObject(copyRequest);
        if (!s3Client.doesObjectExist(targetS3Bucket, targetS3Path)) {
          throw new IOE(
              "After copy was reported as successful the file doesn't exist in the target location [%s]",
              copyMsg
          );
        }
        deleteWithRetriesSilent(s3Bucket, s3Path);
        log.debug("Finished moving file %s", copyMsg);
      }
    } else {
      // ensure object exists in target location
      if (s3Client.doesObjectExist(targetS3Bucket, targetS3Path)) {
        log.info(
            "Not moving file [s3://%s/%s], already present in target location [s3://%s/%s]",
            s3Bucket, s3Path,
            targetS3Bucket, targetS3Path
        );
      } else {
        throw new SegmentLoadingException(
            "Unable to move file %s, not present in either source or target location",
            copyMsg
        );
      }
    }
  }

  private void deleteWithRetriesSilent(final String s3Bucket, final String s3Path)
  {
    try {
      deleteWithRetries(s3Bucket, s3Path);
    }
    catch (Exception e) {
      log.error(e, "Failed to delete file [s3://%s/%s], giving up", s3Bucket, s3Path);
    }
  }

  private void deleteWithRetries(final String s3Bucket, final String s3Path) throws Exception
  {
    RetryUtils.retry(
        () -> {
          try {
            s3Client.deleteObject(s3Bucket, s3Path);
            return null;
          }
          catch (Exception e) {
            log.info(e, "Error while trying to delete [s3://%s/%s]", s3Bucket, s3Path);
            throw e;
          }
        },
        S3Utils.S3RETRY,
        3
    );
  }
}
