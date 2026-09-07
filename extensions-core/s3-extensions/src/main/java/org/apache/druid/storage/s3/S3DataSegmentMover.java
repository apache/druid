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

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.nio.file.Paths;
import java.util.Map;

public class S3DataSegmentMover implements DataSegmentMover
{
  private static final Logger log = new Logger(S3DataSegmentMover.class);

  /**
   * Any implementation of DataSegmentMover is initialized when an ingestion job starts if the extension is loaded
   * even when the implementation of DataSegmentMover is not used. As a result, if we accept a s3 client instead
   * of a supplier of it, it can cause unnecessary config validation for s3 even when it's not used at all.
   * To perform the config validation only when it is actually used, we use a supplier.
   *
   * See OmniDataSegmentMover for how DataSegmentMovers are initialized.
   */
  private final Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentMover(
      Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier,
      S3DataSegmentPusherConfig config
  )
  {
    this.s3ClientSupplier = s3ClientSupplier;
    this.config = config;
  }

  @Override
  public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
  {
    try {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");

      final String targetS3Bucket = MapUtils.getString(targetLoadSpec, "bucket");
      final String targetS3BaseKey = MapUtils.getString(targetLoadSpec, "baseKey");
      final String targetS3Path;

      if (s3Path.endsWith("/")) {
        // segment is not compressed, list objects and move them all
        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(s3Bucket)
            .prefix(s3Path + "/")
            .build();
        final ListObjectsV2Response list = s3ClientSupplier.get().listObjectsV2(request);
        targetS3Path = S3Utils.constructSegmentBasePath(
            targetS3BaseKey,
            DataSegmentPusher.getDefaultStorageDir(segment, false)
        );
        for (S3Object objectSummary : list.contents()) {
          final String fileName = Paths.get(objectSummary.key()).getFileName().toString();
          if (targetS3Bucket.isEmpty()) {
            throw new SegmentLoadingException("Target S3 bucket is not specified");
          }
          if (targetS3Path.isEmpty()) {
            throw new SegmentLoadingException("Target S3 baseKey is not specified");
          }

          safeMove(s3Bucket, s3Path, targetS3Bucket, targetS3Path + "/" + fileName);
        }
      } else {
        targetS3Path = S3Utils.constructSegmentPath(
            targetS3BaseKey,
            DataSegmentPusher.getDefaultStorageDir(segment, false)
        );

        if (targetS3Bucket.isEmpty()) {
          throw new SegmentLoadingException("Target S3 bucket is not specified");
        }
        if (targetS3Path.isEmpty()) {
          throw new SegmentLoadingException("Target S3 baseKey is not specified");
        }

        safeMove(s3Bucket, s3Path, targetS3Bucket, targetS3Path);
      }

      return segment.withLoadSpec(
          ImmutableMap.<String, Object>builder()
                      .putAll(
                          Maps.filterKeys(
                              loadSpec,
                              new Predicate<>()
                              {
                                @Override
                                public boolean apply(String input)
                                {
                                  return !("bucket".equals(input) || "key".equals(input));
                                }
                              }
                          )
                      )
                      .put("bucket", targetS3Bucket)
                      .put("key", targetS3Path)
                      .build()
      );
    }
    catch (S3Exception e) {
      throw new SegmentLoadingException(e, "Unable to move segment[%s]: [%s]", segment.getId(), e);
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
              moveObject(s3Bucket, targetS3Bucket, s3Path, targetS3Path, copyMsg);
              return null;
            }
            catch (S3Exception | SegmentLoadingException e) {
              log.info(e, "Error while trying to move " + copyMsg);
              throw e;
            }
          }
      );
    }
    catch (Exception e) {
      if (e instanceof S3Exception) {
        throw (S3Exception) e;
      }
      if (e instanceof SegmentLoadingException) {
        throw (SegmentLoadingException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Copies an S3 object to a target location and deletes the source.
   * S3 has been strongly consistent since December 2020, so no post-copy existence check is needed.
   */
  private void moveObject(
      String s3Bucket,
      String targetS3Bucket,
      String s3Path,
      String targetS3Path,
      String copyMsg
  ) throws SegmentLoadingException
  {
    if (s3Bucket.equals(targetS3Bucket) && s3Path.equals(targetS3Path)) {
      log.info("No need to move file[s3://%s/%s] onto itself", s3Bucket, s3Path);
      return;
    }
    final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();
    final HeadObjectResponse sourceMetadata;
    try {
      sourceMetadata = s3Client.getObjectMetadata(s3Bucket, s3Path);
    }
    catch (S3Exception e) {
      if (e.statusCode() == 404) {
        // Source is gone; succeed silently if it already landed at the target.
        if (s3Client.doesObjectExist(targetS3Bucket, targetS3Path)) {
          log.info(
              "Not moving file [s3://%s/%s], already present in target location [s3://%s/%s]",
              s3Bucket,
              s3Path,
              targetS3Bucket,
              targetS3Path
          );
        } else {
          throw new SegmentLoadingException(
              "Unable to move file %s, not present in either source or target location",
              copyMsg
          );
        }
        return;
      }
      throw e;
    }
    final StorageClass sc = sourceMetadata.storageClass();
    if (StorageClass.GLACIER.equals(sc) || StorageClass.DEEP_ARCHIVE.equals(sc)) {
      throw S3Exception.builder()
          .message(StringUtils.format(
              "Cannot move file[s3://%s/%s] of storage class [%s], skipping.",
              s3Bucket,
              s3Path,
              sc
          ))
          .build();
    }
    log.info("Moving file %s", copyMsg);
    CopyObjectRequest.Builder copyRequestBuilder = CopyObjectRequest.builder()
        .sourceBucket(s3Bucket)
        .sourceKey(s3Path)
        .destinationBucket(targetS3Bucket)
        .destinationKey(targetS3Path);
    if (!config.getDisableAcl()) {
      final String headerValue = S3Utils.grantFullControlHeaderValue(
          s3Client.getBucketOwnerGrant(targetS3Bucket)
      );
      if (headerValue != null) {
        copyRequestBuilder.grantFullControl(headerValue);
      }
    }
    s3Client.copyObject(copyRequestBuilder);
    deleteWithRetriesSilent(s3Bucket, s3Path);
    log.debug("Finished moving file %s", copyMsg);
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
            s3ClientSupplier.get().deleteObject(s3Bucket, s3Path);
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
