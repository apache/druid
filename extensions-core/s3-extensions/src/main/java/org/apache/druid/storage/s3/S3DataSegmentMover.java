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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
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
              selfCheckingMove(s3Bucket, targetS3Bucket, s3Path, targetS3Path, copyMsg);
              return null;
            }
            catch (S3Exception | IOException | SegmentLoadingException e) {
              log.info(e, "Error while trying to move " + copyMsg);
              throw e;
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, S3Exception.class);
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      throw new RuntimeException(e);
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
    final ServerSideEncryptingAmazonS3 s3Client = this.s3ClientSupplier.get();
    if (s3Client.doesObjectExist(s3Bucket, s3Path)) {
      ListObjectsV2Request request = ListObjectsV2Request.builder()
          .bucket(s3Bucket)
          .prefix(s3Path)
          .maxKeys(1)
          .build();
      final ListObjectsV2Response listResult = s3Client.listObjectsV2(request);
      // Using contents().size() instead of keyCount as, in some cases
      // it is observed that even though the contents returns some data
      // keyCount is still zero.
      if (listResult.contents().size() == 0) {
        // should never happen
        throw new ISE("Unable to list object [s3://%s/%s]", s3Bucket, s3Path);
      }
      final S3Object objectSummary = listResult.contents().get(0);
      if (objectSummary.storageClass() != null &&
          objectSummary.storageClass().equals(ObjectStorageClass.GLACIER)) {
        throw S3Exception.builder()
            .message(StringUtils.format(
                "Cannot move file[s3://%s/%s] of storage class glacier, skipping.",
                s3Bucket,
                s3Path
            ))
            .build();
      } else {
        log.info("Moving file %s", copyMsg);
        CopyObjectRequest.Builder copyRequestBuilder = CopyObjectRequest.builder()
            .sourceBucket(s3Bucket)
            .sourceKey(s3Path)
            .destinationBucket(targetS3Bucket)
            .destinationKey(targetS3Path);
        if (!config.getDisableAcl()) {
          copyRequestBuilder.grantFullControl(S3Utils.grantFullControlToBucketOwner(s3Client, targetS3Bucket).grantee().id());
        }
        s3Client.copyObject(copyRequestBuilder);
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
